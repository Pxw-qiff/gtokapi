package api

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/aurora-develop/grok2api/internal/account"
	"github.com/aurora-develop/grok2api/internal/config"
	"github.com/aurora-develop/grok2api/internal/grok"
	"github.com/aurora-develop/grok2api/internal/logger"
	"github.com/aurora-develop/grok2api/internal/model"
	"github.com/aurora-develop/grok2api/internal/platform"
	"github.com/aurora-develop/grok2api/internal/storage"
)

// fileIDRE matches a valid local media file ID (UUID-style hex with dashes).
var fileIDRE = regexp.MustCompile(`^[0-9a-fA-F\-]{16,36}$`)

// handleFileImage serves a cached image by file ID (public).
func (s *Server) handleFileImage(c *gin.Context) {
	id := strings.TrimSpace(c.Query("id"))
	if !fileIDRE.MatchString(id) {
		writeAppError(c, platform.ValidationError("Invalid file id", "id"))
		return
	}
	dir, err := storage.ImageFilesDir()
	if err != nil {
		writeAppError(c, platform.UpstreamError("image dir: "+err.Error(), 500, ""))
		return
	}
	for _, ext := range []string{".jpg", ".png"} {
		path := filepath.Join(dir, id+ext)
		if _, err := os.Stat(path); err == nil {
			mime := "image/jpeg"
			if ext == ".png" {
				mime = "image/png"
			}
			c.Header("Content-Type", mime)
			c.File(path)
			return
		}
	}
	writeAppError(c, platform.ValidationErrorCode("Image not found", "id", "file_not_found"))
}

// handleFileVideo serves a cached video by file ID (public).
func (s *Server) handleFileVideo(c *gin.Context) {
	id := strings.TrimSpace(c.Query("id"))
	if !fileIDRE.MatchString(id) {
		writeAppError(c, platform.ValidationError("Invalid file id", "id"))
		return
	}
	dir, err := storage.VideoFilesDir()
	if err != nil {
		writeAppError(c, platform.UpstreamError("video dir: "+err.Error(), 500, ""))
		return
	}
	path := filepath.Join(dir, id+".mp4")
	if _, err := os.Stat(path); err != nil {
		writeAppError(c, platform.ValidationErrorCode("Video not found", "id", "file_not_found"))
		return
	}
	c.Header("Content-Type", "video/mp4")
	c.Header("Content-Disposition", `inline; filename="`+id+`.mp4"`)
	c.File(path)
}

// --- Image generation (standalone) ---

func (s *Server) handleImageGenerations(c *gin.Context) {
	var req struct {
		Model          string `json:"model"`
		Prompt         string `json:"prompt"`
		N              int    `json:"n,omitempty"`
		Size           string `json:"size,omitempty"`
		ResponseFormat string `json:"response_format,omitempty"`
	}
	if err := readJSON(c, &req); err != nil {
		writeAppError(c, err)
		return
	}
	spec, ok := model.Resolve(req.Model)
	if !ok {
		writeAppError(c, platform.ValidationErrorCode("Model '"+req.Model+"' not found", "model", "model_not_found"))
		return
	}
	if !spec.IsImage() {
		writeAppError(c, platform.ValidationErrorCode("Model '"+req.Model+"' is not an image model", "model", "invalid_model"))
		return
	}
	n := req.N
	if n <= 0 {
		n = 1
	}
	responseFormat := req.ResponseFormat
	if responseFormat == "" {
		responseFormat = "url"
	}

	// WS-based models (grok-imagine-image, grok-imagine-image-pro).
	if grok.IsWSImageModel(spec.ModelName) {
		s.handleWSImageGenerations(c, spec, req.Prompt, n, req.Size, responseFormat)
		return
	}

	// Lite model: chat-based generation with concurrent fan-out.
	maxN := 4
	if n > maxN {
		n = maxN
	}

	apiToken, _ := c.Get("api_token")
	ssoToken, _ := apiToken.(string)

	prompt := "Drawing: " + req.Prompt
	imageURLs, genErr := s.captureLiteImageBatch(c.Request, spec, prompt, n, ssoToken)

	if len(imageURLs) == 0 && genErr != nil {
		writeAppError(c, genErr)
		return
	}

	out := []map[string]any{}
	for _, url := range imageURLs {
		resolved, _ := resolveImageURL(s, url, ssoToken)
		if strings.HasPrefix(resolved, "data:") {
			b64 := resolved
			if idx := strings.Index(resolved, ","); idx >= 0 {
				b64 = resolved[idx+1:]
			}
			out = append(out, map[string]any{"b64_json": b64})
		} else {
			out = append(out, map[string]any{"url": resolved})
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"created": time.Now().Unix(),
		"data":    out,
	})
}

// handleWSImageGenerations handles image generation via the WS imagine endpoint.
func (s *Server) handleWSImageGenerations(c *gin.Context, spec *model.Spec, prompt string, n int, size, responseFormat string) {
	if n <= 0 {
		n = 1
	}
	maxN := 10
	if n > maxN {
		n = maxN
	}

	aspectRatio := grok.ResolveAspectRatio(size)
	enableNSFW := config.Global().GetBool("features.enable_nsfw", true)
	enablePro := grok.IsProImageModel(spec.ModelName)

	apiToken, _ := c.Get("api_token")
	ssoToken, _ := apiToken.(string)

	// Reserve an account.
	lease, _ := reserveAccount(c.Request.Context(), s.Directory, spec, nil)
	if lease == nil && ssoToken != "" {
		lease = &account.Lease{Token: ssoToken, ModeID: int(spec.ModeId)}
	}
	if lease == nil {
		writeAppError(c, platform.RateLimitError("No available accounts"))
		return
	}
	defer s.Directory.Release(lease)

	stream := grok.NewImagineStream(lease.Token)
	events := stream.StreamImages(prompt, aspectRatio, n, enableNSFW, enablePro)

	type collectedImage struct {
		url  string
		blob string
	}
	var images []collectedImage
	for ev := range events {
		switch ev.Type {
		case grok.ImagineEventImage:
			url := ""
			if ev.URL != "" {
				url = grok.ImageBaseURL + strings.TrimPrefix(ev.URL, "/")
			}
			images = append(images, collectedImage{url: url, blob: ev.Blob})
		case grok.ImagineEventError:
			writeAppError(c, platform.UpstreamError(ev.Error, 502, ""))
			return
		}
	}

	out := []map[string]any{}
	for _, img := range images {
		rawURL := img.url
		if responseFormat == "b64_json" && img.blob != "" {
			out = append(out, map[string]any{"b64_json": img.blob})
			continue
		}
		resolved, _ := resolveImageURL(s, rawURL, lease.Token)
		if strings.HasPrefix(resolved, "data:") {
			b64 := resolved
			if idx := strings.Index(resolved, ","); idx >= 0 {
				b64 = resolved[idx+1:]
			}
			out = append(out, map[string]any{"b64_json": b64})
		} else {
			out = append(out, map[string]any{"url": resolved})
		}
	}
	c.JSON(http.StatusOK, gin.H{"created": time.Now().Unix(), "data": out})
}

// handleImageEdits serves the multipart image-edit endpoint.
func (s *Server) handleImageEdits(c *gin.Context) {
	if err := c.Request.ParseMultipartForm(50 << 20); err != nil {
		writeAppError(c, platform.ValidationError("Invalid multipart form: "+err.Error(), "body"))
		return
	}
	modelName := strings.TrimSpace(c.Request.FormValue("model"))
	prompt := strings.TrimSpace(c.Request.FormValue("prompt"))
	if modelName == "" || prompt == "" {
		writeAppError(c, platform.ValidationError("Missing model or prompt", "body"))
		return
	}
	spec, ok := model.Resolve(modelName)
	if !ok {
		writeAppError(c, platform.ValidationErrorCode("Model '"+modelName+"' not found", "model", "model_not_found"))
		return
	}
	if !spec.IsImageEdit() {
		writeAppError(c, platform.ValidationErrorCode("Model '"+modelName+"' is not an image-edit model", "model", "invalid_model"))
		return
	}
	responseFormat := strings.TrimSpace(c.Request.FormValue("response_format"))
	if responseFormat == "" {
		responseFormat = "url"
	}
	files := c.Request.MultipartForm.File["image[]"]
	if len(files) == 0 {
		writeAppError(c, platform.ValidationError("No images provided", "image[]"))
		return
	}
	contentBlocks := []map[string]any{{"type": "text", "text": prompt}}
	for _, fh := range files {
		f, err := fh.Open()
		if err != nil {
			continue
		}
		raw, _ := io.ReadAll(io.LimitReader(f, 30<<20))
		f.Close()
		if len(raw) == 0 {
			continue
		}
		mime := fh.Header.Get("Content-Type")
		if mime == "" || !strings.HasPrefix(mime, "image/") {
			continue
		}
		b64 := base64.StdEncoding.EncodeToString(raw)
		dataURI := "data:" + mime + ";base64," + b64
		contentBlocks = append(contentBlocks, map[string]any{
			"type":      "image_url",
			"image_url": map[string]any{"url": dataURI},
		})
	}
	apiToken, _ := c.Get("api_token")
	ssoToken, _ := apiToken.(string)

	messages := []map[string]any{{"role": "user", "content": contentBlocks}}
	chatReq := &chatCompletionRequest{Model: modelName, Messages: messages}
	streamOff := false
	chatReq.Stream = &streamOff
	imageURLs, _ := s.captureImageURLs(c.Request, chatReq, spec, ssoToken)
	out := []map[string]any{}
	for _, url := range imageURLs {
		resolved, _ := resolveImageURL(s, url, ssoToken)
		if strings.HasPrefix(resolved, "data:") {
			b64 := resolved
			if idx := strings.Index(resolved, ","); idx >= 0 {
				b64 = resolved[idx+1:]
			}
			out = append(out, map[string]any{"b64_json": b64})
		} else {
			out = append(out, map[string]any{"url": resolved})
		}
	}
	c.JSON(http.StatusOK, gin.H{"created": time.Now().Unix(), "data": out})
}

// captureImageURLs runs the STREAMING chat path (same as /v1/chat/completions)
// and extracts any image URLs from the response. Using the streaming path is
// more reliable because grok's non-streaming responses sometimes omit the
// image URL even when progress reaches 100%.
// ssoToken is the Bearer token from the request, used as fallback when the pool is empty.
func (s *Server) captureImageURLs(r *http.Request, req *chatCompletionRequest, spec *model.Spec, ssoToken string) ([]string, error) {
	message, fileInputs, perr := extractMessages(req.Messages)
	if perr != nil {
		return nil, perr
	}

	maxRetries := selectionMaxRetries()
	exclude := []string{}
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		lease, _ := reserveAccount(r.Context(), s.Directory, spec, exclude)
		if lease == nil {
			if s.Refresh != nil {
				_ = s.Refresh.RefreshOnDemand(r.Context())
				lease, _ = reserveAccount(r.Context(), s.Directory, spec, exclude)
			}
		}
		if lease == nil && ssoToken != "" {
			lease = &account.Lease{Token: ssoToken, ModeID: int(spec.ModeId)}
		}
		if lease == nil {
			return nil, platform.RateLimitError("No available accounts")
		}

		urls, err := s.captureImageURLsOnce(r, lease, spec, message, fileInputs)
		s.Directory.Release(lease)

		if err != nil {
			lastErr = err
			if attempt < maxRetries {
				exclude = append(exclude, lease.Token)
			}
			continue
		}
		if len(urls) > 0 {
			return urls, nil
		}

		// Got a valid response but no image URLs — retry with a different account.
		if attempt < maxRetries {
			exclude = append(exclude, lease.Token)
		}
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, platform.UpstreamError("Image generation completed but no image URL was returned (may be rate-limited or moderated)", 502, "")
}

// captureImageURLsOnce executes one streaming chat attempt and collects image URLs.
func (s *Server) captureImageURLsOnce(r *http.Request, lease *account.Lease, spec *model.Spec, message string, fileInputs []string) ([]string, error) {
	payload := grok.BuildChatPayload(message, model.ModeId(lease.ModeID), fileInputs, nil, nil, nil)
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, platform.UpstreamError("encode payload: "+err.Error(), 500, "")
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()

	bodyReader, err := s.Transport.PostStream(ctx, grok.Chat, lease.Token, body)
	if err != nil {
		return nil, err
	}
	defer bodyReader.Close()

	adapter := grok.NewStreamAdapter()
	var imageURLs []string

	scanner := bufio.NewScanner(bodyReader)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		kind, data := grok.ClassifyLine(line)
		if kind == "done" {
			break
		}
		if kind != "data" {
			continue
		}
		events, _ := adapter.Feed([]byte(data))
		for _, ev := range events {
			if ev.Kind == grok.EventImage && ev.Content != "" {
				url := ev.Content
				if !strings.HasPrefix(url, "http") {
					url = grok.ImageBaseURL + strings.TrimPrefix(url, "/")
				}
				imageURLs = append(imageURLs, url)
			}
		}
	}

	// Also collect any URLs from the adapter's ImageURLs accumulator.
	for _, pair := range adapter.ImageURLs {
		url := pair[0]
		if url != "" {
			found := false
			for _, existing := range imageURLs {
				if existing == url {
					found = true
					break
				}
			}
			if !found {
				if !strings.HasPrefix(url, "http") {
					url = grok.ImageBaseURL + strings.TrimPrefix(url, "/")
				}
				imageURLs = append(imageURLs, url)
			}
		}
	}

	return imageURLs, nil
}

// captureLiteImageBatch runs N concurrent chat-based image generation
// requests and returns all collected image URLs and any error.
func (s *Server) captureLiteImageBatch(r *http.Request, spec *model.Spec, prompt string, n int, ssoToken string) ([]string, error) {
	if n <= 0 {
		n = 1
	}
	results := make([]string, n)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			msgs := []map[string]any{{"role": "user", "content": prompt}}
			chatReq := &chatCompletionRequest{
				Model:    spec.ModelName,
				Messages: msgs,
			}
			urls, err := s.captureImageURLs(r, chatReq, spec, ssoToken)
			if len(urls) > 0 {
				results[idx] = urls[0]
			}
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	// Collect non-empty results preserving order.
	out := make([]string, 0, n)
	for _, u := range results {
		if u != "" {
			out = append(out, u)
		}
	}
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

// extractImageURLsFromMarkdown returns URLs found in markdown image syntax.
var imageMDRE = regexp.MustCompile(`!\[[^\]]*\]\(([^)]+)\)`)

func extractImageURLsFromMarkdown(text string) []string {
	matches := imageMDRE.FindAllStringSubmatch(text, -1)
	out := []string{}
	for _, m := range matches {
		if len(m) > 1 {
			out = append(out, m[1])
		}
	}
	return out
}

// fetchImageBase64 downloads the image bytes and returns the base64 encoding.
func fetchImageBase64(url string) (string, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 50<<20))
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(body), nil
}

// fetchImageBase64ViaTransport downloads the image bytes via the authenticated
// Transport (carries cf_clearance and grok session cookies), then returns the
// base64 encoding. This is needed for assets.grok.com URLs that require auth.
func (s *Server) fetchImageBase64ViaTransport(url string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bodyReader, err := s.Transport.GetBytes(ctx, url, "")
	if err != nil {
		return "", err
	}
	defer bodyReader.Close()
	body, err := io.ReadAll(io.LimitReader(bodyReader, 50<<20))
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(body), nil
}

// --- Video jobs (async) ---

type videoJob struct {
	ID          string `json:"id"`
	Object      string `json:"object"`
	CreatedAt   int64  `json:"created_at"`
	Status      string `json:"status"`
	Model       string `json:"model"`
	Progress    int    `json:"progress"`
	Prompt      string `json:"prompt"`
	Seconds     int    `json:"seconds"`
	Size        string `json:"size"`
	Quality     string `json:"quality"`
	ImageURL    string `json:"image_url,omitempty"`
	ImageURLs   []string `json:"image_urls,omitempty"`
	Upscale     bool   `json:"upscale,omitempty"`
	VideoURL    string `json:"video_url,omitempty"`
	CompletedAt *int64 `json:"completed_at,omitempty"`
	Error       *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
	contentPath string
}

var (
	videoJobsMap   = map[string]*videoJob{}
	videoJobsMutex sync.Mutex
)

// handleVideoCreate queues an async video job.
// 同时支持 JSON body 和 multipart 表单两种请求格式。
//
// 【修改说明】
// 修改背景：用户要求请求体字段与 OpenAI 风格对齐
// 解决问题：seconds->duration, size->aspect_ratio, image_url/image_urls->images（统一数组）
// 设计考虑：resolutionName 固定 720p 不暴露，preset 默认 custom 不暴露
func (s *Server) handleVideoCreate(c *gin.Context) {
	var modelName, prompt, aspectRatio string
	var images []string
	var durationInt int

	contentType := c.GetHeader("Content-Type")
	var upscaleRequested bool
	if strings.HasPrefix(contentType, "application/json") {
		var body struct {
			Model       string   `json:"model"`
			Prompt      string   `json:"prompt"`
			Duration    int      `json:"duration"`
			AspectRatio string   `json:"aspect_ratio"`
			Images      []string `json:"images"`
			Upscale     bool     `json:"upscale"`
		}
		if err := readJSON(c, &body); err != nil {
			writeAppError(c, err)
			return
		}
		modelName = strings.TrimSpace(body.Model)
		prompt = strings.TrimSpace(body.Prompt)
		durationInt = body.Duration
		aspectRatio = strings.TrimSpace(body.AspectRatio)
		images = body.Images
		upscaleRequested = body.Upscale
	} else {
		if err := c.Request.ParseMultipartForm(50 << 20); err != nil {
			writeAppError(c, platform.ValidationError("Invalid multipart form: "+err.Error(), "body"))
			return
		}
		modelName = strings.TrimSpace(c.Request.FormValue("model"))
		prompt = strings.TrimSpace(c.Request.FormValue("prompt"))
		if v := c.Request.FormValue("duration"); v != "" {
			if n, err := parseIntStr(v); err == nil {
				durationInt = n
			}
		}
		aspectRatio = strings.TrimSpace(c.Request.FormValue("aspect_ratio"))
		// multipart 表单的 images 通过重复字段名传参
		images = c.Request.PostForm["images"]
		upscaleRequested = c.Request.FormValue("upscale") == "true" || c.Request.FormValue("upscale") == "1"
	}

	// 参考图最多 7 张
	allImages := []string{}
	for _, u := range images {
		u = strings.TrimSpace(u)
		if u != "" {
			allImages = append(allImages, u)
		}
	}
	if len(allImages) > 7 {
		allImages = allImages[:7]
	}

	if modelName == "" || prompt == "" {
		writeAppError(c, platform.ValidationError("Missing model or prompt", "body"))
		return
	}
	spec, ok := model.Resolve(modelName)
	if !ok {
		writeAppError(c, platform.ValidationErrorCode("Model '"+modelName+"' not found", "model", "model_not_found"))
		return
	}
	if !spec.IsVideo() {
		writeAppError(c, platform.ValidationErrorCode("Model '"+modelName+"' is not a video model", "model", "invalid_model"))
		return
	}
	duration := 6
	if durationInt > 0 && isValidVideoLength(durationInt) {
		duration = durationInt
	}
	if aspectRatio == "" {
		aspectRatio = "9:16"
	}
	if !grok.IsValidAspectRatio(aspectRatio) {
		writeAppError(c, platform.ValidationError("aspect_ratio must be one of [9:16, 16:9, 1:1]", "aspect_ratio"))
		return
	}
	job := &videoJob{
		ID:          "video_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:24],
		Object:      "video",
		CreatedAt:   time.Now().Unix(),
		Status:      "queued",
		Model:       modelName,
		Progress:    0,
		Prompt:      prompt,
		Seconds:     duration,
		Size:        aspectRatio,
		Quality:     "standard",
		ImageURLs:   allImages,
		Upscale:     upscaleRequested,
	}
	registerVideoJob(job)

	logger.Infof("视频任务已创建: job=%s model=%s duration=%d aspect_ratio=%s refs=%d upscale=%t", job.ID, modelName, duration, aspectRatio, len(allImages), upscaleRequested)

	go s.runVideoJob(job, prompt, allImages, spec)
	c.JSON(http.StatusOK, job.toDict())
}

// handleVideoGet serves GET /v1/videos/:id and /v1/videos/:id/content.
func (s *Server) handleVideoGet(c *gin.Context) {
	id := c.Param("id")
	job := lookupVideoJob(id)
	if job == nil {
		writeAppError(c, platform.ValidationErrorCode("Video '"+id+"' not found", "video_id", "video_not_found"))
		return
	}
	// Check if requesting content
	if strings.HasSuffix(c.Request.URL.Path, "/content") {
		if job.Status != "completed" || job.contentPath == "" {
			writeAppError(c, platform.NewAppError("Video content is not ready yet", platform.ErrUpstream, "video_not_ready", http.StatusConflict))
			return
		}
		c.Header("Content-Type", "video/mp4")
		c.Header("Content-Disposition", `inline; filename="`+id+`.mp4"`)
		c.File(job.contentPath)
		return
	}
	c.JSON(http.StatusOK, job.toDict())
}

// runVideoJob 执行视频生成任务（异步）。
//
// 【修改说明】
// 修改背景：新增多张参考图功能，最多 7 张
// 解决问题：有参考图时逐张上传 -> 每张创建 image media post -> 第一张 post id 作为 parentPostId，所有 content URL 作为 imageReferences
// 设计考虑：参考图上传使用 UploadFromInput，支持 URL 和 data URI 两种格式
// 注意事项：modelName 不再传入，已存储在 job.Model 中；preset 默认 custom
func (s *Server) runVideoJob(job *videoJob, prompt string, imageURLs []string, spec *model.Spec) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	job.Status = "in_progress"
	job.Progress = 1
	logger.Infof("视频任务开始: job=%s prompt=%q refs=%d", job.ID, truncate(prompt, 80), len(imageURLs))

	// 【修改说明】账号获取 + media post 创建合并为重试循环
	// 401 时自动标记当前账号 expired 并换号重试，避免因单个 token 失效导致整个视频任务失败
	var lease *account.Lease
	var token string
	var parentPostID string
	var imageReferences []string
	excludedTokens := []string{}

	maxAccountRetries := 3
	for attempt := 0; attempt < maxAccountRetries; attempt++ {
		lease, _ = reserveAccount(ctx, s.Directory, spec, excludedTokens)
		if lease == nil {
			logger.Warnf("视频任务无可用账号: job=%s", job.ID)
			s.failVideoJob(job, "no available accounts")
			return
		}
		token = lease.Token
		logger.Infof("视频任务已分配账号: job=%s token=%s attempt=%d", job.ID, platform.SanitizeToken(token), attempt+1)

		// 创建 media post
		if len(imageURLs) > 0 {
			logger.Infof("视频任务上传参考图: job=%s count=%d", job.ID, len(imageURLs))
			retry401 := false
			for i, imgURL := range imageURLs {
				uploadResult, err := grok.UploadFromInput(ctx, s.Transport, token, imgURL)
				if err != nil {
					if isUnauthorizedError(err) && attempt < maxAccountRetries-1 {
						logger.Warnf("视频任务参考图上传 401，换号重试: job=%s index=%d", job.ID, i+1)
						retry401 = true
						break
					}
					logger.Warnf("视频任务参考图上传失败: job=%s index=%d error=%v body=%s", job.ID, i+1, err, errBody(err))
					s.failVideoJob(job, fmt.Sprintf("reference image %d upload: %s", i+1, err.Error()))
					s.Directory.Release(lease)
					return
				}
				contentURL, err := grok.ResolveUploadedAssetReference(token, uploadResult.FileID, uploadResult.FileURI)
				if err != nil {
					logger.Warnf("视频任务参考图URL解析失败: job=%s index=%d error=%v", job.ID, i+1, err)
					s.failVideoJob(job, fmt.Sprintf("reference image %d resolve: %s", i+1, err.Error()))
					s.Directory.Release(lease)
					return
				}
				imgPostPayload := grok.BuildImagePostPayload(contentURL)
				imgPostBody, _ := json.Marshal(imgPostPayload)
				imgPostResp, err := s.Transport.PostJSON(ctx, grok.MediaPost, token, imgPostBody,
					grok.WithReferer("https://grok.com/imagine"))
				if err != nil {
					if isUnauthorizedError(err) && attempt < maxAccountRetries-1 {
						logger.Warnf("视频任务参考图media post 401，换号重试: job=%s index=%d", job.ID, i+1)
						retry401 = true
						break
					}
					logger.Warnf("视频任务参考图media post失败: job=%s index=%d error=%v body=%s", job.ID, i+1, err, errBody(err))
					s.failVideoJob(job, fmt.Sprintf("image media post %d create: %s", i+1, err.Error()))
					s.Directory.Release(lease)
					return
				}
				postID := ""
				if post, ok := imgPostResp["post"].(map[string]any); ok {
					postID, _ = post["id"].(string)
				}
				if postID == "" {
					logger.Warnf("视频任务参考图media post无post id: job=%s index=%d", job.ID, i+1)
					s.failVideoJob(job, fmt.Sprintf("image media post %d returned no post id", i+1))
					s.Directory.Release(lease)
					return
				}
				if i == 0 {
					parentPostID = postID
				}
				imageReferences = append(imageReferences, contentURL)
			}
			if retry401 {
				s.feedbackError(token, platform.UpstreamError("invalid credentials", 401, ""), lease.ModeID)
				s.Directory.Release(lease)
				excludedTokens = append(excludedTokens, token)
				parentPostID = ""
				imageReferences = nil
				continue
			}
			if parentPostID == "" {
				logger.Warnf("视频任务参考图无parent post id: job=%s", job.ID)
				s.failVideoJob(job, "no parent post id from reference images")
				s.Directory.Release(lease)
				return
			}
			logger.Infof("视频任务参考图上传完成: job=%s parentPostId=%s refs=%d", job.ID, parentPostID, len(imageReferences))
		} else {
			postPayload := grok.BuildVideoPostPayload(prompt)
			postBody, _ := json.Marshal(postPayload)
			postResp, err := s.Transport.PostJSON(ctx, grok.MediaPost, token, postBody,
				grok.WithReferer("https://grok.com/imagine"))
			if err != nil {
				if isUnauthorizedError(err) && attempt < maxAccountRetries-1 {
					logger.Warnf("视频任务media post 401，换号重试: job=%s attempt=%d", job.ID, attempt+1)
					s.feedbackError(token, platform.UpstreamError("invalid credentials", 401, ""), lease.ModeID)
					s.Directory.Release(lease)
					excludedTokens = append(excludedTokens, token)
					continue
				}
				logger.Warnf("视频任务media post创建失败: job=%s error=%v body=%s", job.ID, err, errBody(err))
				s.failVideoJob(job, "media post create: "+err.Error())
				s.Directory.Release(lease)
				return
			}
			if post, ok := postResp["post"].(map[string]any); ok {
				parentPostID, _ = post["id"].(string)
			}
			if parentPostID == "" {
				logger.Warnf("视频任务media post无post id: job=%s", job.ID)
				s.failVideoJob(job, "media post returned no post id")
				s.Directory.Release(lease)
				return
			}
			logger.Infof("视频任务media post已创建: job=%s parentPostId=%s", job.ID, parentPostID)
		}
		break
	}
	defer s.Directory.Release(lease)
	defer func() {
		if job.Status == "failed" {
			s.feedbackError(token, platform.UpstreamError(job.Error.Message, 502, ""), lease.ModeID)
		}
	}()

	// 3. 分段生成视频
	// 【修改说明】aspectRatio 直接从 job.Size 获取（用户直传），resolutionName 固定 720p
	aspectRatio := job.Size
	resolutionName := grok.VideoResolution
	segments := grok.BuildSegmentLengths(job.Seconds)
	totalSegments := len(segments)
	logger.Infof("视频任务开始分段生成: job=%s segments=%v aspect=%s resolution=%s", job.ID, segments, aspectRatio, resolutionName)
	extendPostID := parentPostID
	elapsedSeconds := 0
	var lastArtifact *grok.VideoArtifact

	for index, segmentLength := range segments {
		logger.Infof("视频任务分段 %d/%d 开始: job=%s length=%ds", index+1, totalSegments, job.ID, segmentLength)
		var payload map[string]any
		referer := "https://grok.com/imagine"
		if index == 0 {
			payload = grok.BuildVideoGenPayload(
				prompt, parentPostID, aspectRatio, resolutionName, "custom", segmentLength, imageReferences,
			)
		} else {
			payload = grok.BuildVideoExtendPayload(
				prompt, parentPostID, extendPostID, aspectRatio, resolutionName, "custom",
				segmentLength, grok.VideoExtendStartTime(elapsedSeconds),
			)
			referer = fmt.Sprintf("https://grok.com/imagine/post/%s", parentPostID)
		}

		body, _ := json.Marshal(payload)
		bodyReader, err := s.Transport.PostStream(ctx, grok.Chat, token, body,
			grok.WithReferer(referer))
		if err != nil {
			// 【修改说明】403 anti-bot 时清除远程签名缓存并重试一次，因为缓存的签名可能已过期
			if isAntiBotError(err) {
				logger.Warnf("视频任务分段 %d 触发 anti-bot 403，清除签名缓存并重试: job=%s", index+1, job.ID)
				grok.InvalidateRemoteStatsigCache()
				bodyReader, err = s.Transport.PostStream(ctx, grok.Chat, token, body,
					grok.WithReferer(referer))
			}
			if err != nil {
				logger.Warnf("视频任务分段 %d 上游请求失败: job=%s error=%v body=%s", index+1, job.ID, err, errBody(err))
				s.failVideoJob(job, fmt.Sprintf("video segment %d upstream: %s", index, err.Error()))
				return
			}
		}

		artifact, err := s.collectVideoSegment(bodyReader, index, totalSegments, job)
		bodyReader.Close()
		if err != nil {
			logger.Warnf("视频任务分段 %d 解析失败: job=%s error=%v", index+1, job.ID, err)
			s.failVideoJob(job, fmt.Sprintf("video segment %d: %s", index, err.Error()))
			return
		}
		if artifact == nil {
			logger.Warnf("视频任务分段 %d 无视频URL: job=%s", index+1, job.ID)
			s.failVideoJob(job, fmt.Sprintf("video segment %d: no video URL", index))
			return
		}

		lastArtifact = artifact
		logger.Infof("视频任务分段 %d/%d 完成: job=%s videoUrl=%s postId=%q assetId=%q", index+1, totalSegments, job.ID, truncate(artifact.VideoURL, 80), artifact.VideoPostID, artifact.AssetID)
		// 【修改说明】优先用 assetId 作为 extendPostId，因为 videoPostId 在参考图场景下可能不是可扩展的资产 ID
		extendPostID = artifact.AssetID
		if extendPostID == "" {
			extendPostID = artifact.VideoPostID
		}
		if extendPostID == "" {
			extendPostID = parentPostID
		}
		logger.Infof("视频任务分段 %d extendPostId=%q", index+1, extendPostID)
		elapsedSeconds += segmentLength
	}

	// 4. 完成
	if lastArtifact != nil && lastArtifact.VideoURL != "" {
		videoURL := lastArtifact.VideoURL

		// 【修改说明】upscale 1080p：生成完成后调 /rest/media/video/upscale 提升画质
		// Grok 的 videoId 需要传 assetId（视频资产 ID），不是 videoPostId（对话 post ID）
		upscaleVideoID := lastArtifact.AssetID
		if upscaleVideoID == "" {
			upscaleVideoID = lastArtifact.VideoPostID
		}
		if job.Upscale && upscaleVideoID != "" {
			logger.Infof("视频任务开始 upscale: job=%s videoId=%s", job.ID, upscaleVideoID)
			upscalePayload := grok.BuildVideoUpscalePayload(upscaleVideoID)
			upscaleBody, _ := json.Marshal(upscalePayload)
			upscaleResp, err := s.Transport.PostJSON(ctx, grok.VideoUpscale, token, upscaleBody,
				grok.WithReferer("https://grok.com/imagine"))
			if err != nil {
				logger.Warnf("视频任务 upscale 失败: job=%s error=%v body=%s，使用原始视频", job.ID, err, errBody(err))
			} else {
				// 解析 upscale 响应，尝试从多种可能的字段中提取 1080p 视频 URL
				if upscaledURL := extractUpscaledVideoURL(upscaleResp); upscaledURL != "" {
					logger.Infof("视频任务 upscale 完成: job=%s upscaledUrl=%s", job.ID, truncate(upscaledURL, 100))
					videoURL = upscaledURL
				} else {
					logger.Warnf("视频任务 upscale 响应无视频URL: job=%s resp=%v", job.ID, upscaleResp)
				}
			}
		}

		now := time.Now().Unix()
		job.Status = "completed"
		job.Progress = 100
		job.CompletedAt = &now
		finalURL, finalPath, _ := resolveVideoURL(s, videoURL, token)
		job.VideoURL = finalURL
		job.contentPath = finalPath
		s.feedback(token, account.FbSuccess, lease.ModeID, nil, nil)
		logger.Infof("视频任务完成: job=%s videoUrl=%s upscale=%t", job.ID, truncate(finalURL, 100), job.Upscale)
		return
	}
	logger.Warnf("视频任务无最终URL: job=%s", job.ID)
	s.feedbackError(token, platform.UpstreamError("no video URL in upstream response", 502, ""), lease.ModeID)
	s.failVideoJob(job, "no video URL in upstream response")
}

// collectVideoSegment 解析一个视频段的 SSE 流，提取视频 URL 和 post ID。
// 通过 StreamAdapter.Feed 解析每帧，监听 EventVideo 和 EventVideoProgress。
func (s *Server) collectVideoSegment(bodyReader io.ReadCloser, segmentIndex, totalSegments int, job *videoJob) (*grok.VideoArtifact, error) {
	adapter := grok.NewStreamAdapter()
	scanner := bufio.NewScanner(bodyReader)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)

	frameCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		kind, data := grok.ClassifyLine(line)
		if kind == "done" {
			break
		}
		if kind != "data" {
			continue
		}
		frameCount++
		// 【修改说明】记录每帧的顶层 key 用于诊断上游返回了什么（如错误、空响应、非视频帧）
		logger.Infof("视频段 %d SSE frame %d keys: %s", segmentIndex+1, frameCount, truncate(data, 300))
		events, appErr := adapter.Feed([]byte(data))
		if appErr != nil {
			logger.Warnf("视频段 %d SSE frame %d 解析错误: %v", segmentIndex+1, frameCount, appErr)
			return nil, appErr
		}
		for _, ev := range events {
			if ev.Kind == grok.EventVideoProgress {
				if n, err := parseIntStr(ev.Content); err == nil && n > 0 {
					scaled := int((float64(segmentIndex) + float64(n)/100.0) / float64(totalSegments) * 100)
					if scaled > job.Progress {
						job.Progress = scaled
					}
				}
			}
			if ev.Kind == grok.EventVideo {
				logger.Infof("视频段 %d EventVideo: url=%s postId=%q", segmentIndex+1, truncate(ev.Content, 80), ev.ImageID)
				return &grok.VideoArtifact{
					VideoURL:    ev.Content,
					VideoPostID: ev.ImageID,
					AssetID:     adapter.LastAssetID(),
				}, nil
			}
		}
	}

	// 【修改说明】检查 scanner 是否因错误退出（如超时、连接中断）
	if err := scanner.Err(); err != nil {
		logger.Warnf("视频段 %d SSE 读取错误: job=%s error=%v", segmentIndex+1, job.ID, err)
		return nil, fmt.Errorf("video segment %d: stream read error: %w", segmentIndex, err)
	}

	// Fallback：检查 adapter 累积的 VideoURLs
	if len(adapter.VideoURLs) > 0 {
		pair := adapter.VideoURLs[len(adapter.VideoURLs)-1]
		return &grok.VideoArtifact{
			VideoURL:    pair[0],
			VideoPostID: pair[1],
		}, nil
	}

	// 【修改说明】流结束但无视频URL时记录帧数和会话信息，便于判断是空响应还是非视频帧
	logger.Warnf("视频段 %d SSE 流结束无视频URL: job=%s frames=%d convId=%s respId=%s",
		segmentIndex+1, job.ID, frameCount, adapter.ConversationID, adapter.LastResponseID)

	return nil, nil
}

func (s *Server) failVideoJob(job *videoJob, message string) {
	now := time.Now().Unix()
	job.Status = "failed"
	job.CompletedAt = &now
	job.Error = &struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}{Code: "video_generation_failed", Message: message}
	logger.Warnf("视频任务失败: job=%s reason=%s", job.ID, message)
}

func (j *videoJob) toDict() map[string]any {
	m := map[string]any{
		"id": j.ID, "object": j.Object, "created_at": j.CreatedAt,
		"status": j.Status, "model": j.Model, "progress": j.Progress,
		"prompt": j.Prompt, "seconds": fmt.Sprintf("%d", j.Seconds),
		"size": j.Size, "quality": j.Quality,
	}
	if j.VideoURL != "" {
		m["video_url"] = j.VideoURL
	}
	if j.CompletedAt != nil {
		m["completed_at"] = *j.CompletedAt
	}
	if j.Error != nil {
		m["error"] = j.Error
	}
	return m
}

func registerVideoJob(job *videoJob) {
	videoJobsMutex.Lock()
	defer videoJobsMutex.Unlock()
	videoJobsMap[job.ID] = job
}

func lookupVideoJob(id string) *videoJob {
	videoJobsMutex.Lock()
	defer videoJobsMutex.Unlock()
	return videoJobsMap[id]
}

func isValidVideoLength(n int) bool {
	switch n {
	case 6, 10, 12, 16, 20:
		return true
	}
	return false
}

// extractUpscaledVideoURL 从 upscale 响应中提取 1080p 视频 URL。
// 尝试多种可能的字段名，因为 Grok 的响应格式可能变化。
func extractUpscaledVideoURL(resp map[string]any) string {
	// 直接字段
	for _, key := range []string{"videoUrl", "video_url", "url", "upscaledVideoUrl"} {
		if v, ok := resp[key].(string); ok && v != "" {
			return grok.AbsolutizeVideoURL(v)
		}
	}
	// 嵌套在 post 或 video 对象里
	for _, nestKey := range []string{"post", "video", "result"} {
		if nested, ok := resp[nestKey].(map[string]any); ok {
			for _, key := range []string{"videoUrl", "video_url", "url", "upscaledVideoUrl"} {
				if v, ok := nested[key].(string); ok && v != "" {
					return grok.AbsolutizeVideoURL(v)
				}
			}
		}
	}
	return ""
}

// errBody 从 AppError 中提取 Body 字段，用于日志打印上游错误响应体。
func errBody(err error) string {
	var appErr *platform.AppError
	if errors.As(err, &appErr) && appErr.Body != "" {
		return appErr.Body
	}
	return ""
}

// isAntiBotError 检查错误是否为 403 anti-bot 拒绝
func isAntiBotError(err error) bool {
	var appErr *platform.AppError
	if !errors.As(err, &appErr) {
		return false
	}
	if appErr.Status != 403 {
		return false
	}
	return strings.Contains(appErr.Body, "anti-bot") || strings.Contains(appErr.Body, "code\":7")
}

// isUnauthorizedError 检查错误是否为 401 凭据失效
func isUnauthorizedError(err error) bool {
	var appErr *platform.AppError
	if !errors.As(err, &appErr) {
		return false
	}
	if appErr.Status == 401 {
		return true
	}
	return platform.IsInvalidCredentialsBody(appErr.Body)
}

// truncate 截断字符串到指定长度，超长则加省略号。
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func parseIntStr(s string) (int, error) {
	n := 0
	neg := false
	i := 0
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		neg = s[i] == '-'
		i++
	}
	if i == len(s) {
		return 0, fmt.Errorf("empty")
	}
	for ; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid digit")
		}
		n = n*10 + int(c-'0')
	}
	if neg {
		n = -n
	}
	return n, nil
}
