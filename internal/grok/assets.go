package grok

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/aurora-develop/grok2api/internal/config"
	"github.com/aurora-develop/grok2api/internal/platform"
)

// AssetUploadResult is the response from POST /rest/app-chat/upload-file.
type AssetUploadResult struct {
	FileID  string `json:"fileMetadataId"`
	FileURI string `json:"fileUri"`
}

// extensionMIME maps file extensions to MIME types (best-effort inference).
var extensionMIME = map[string]string{
	".jpg":  "image/jpeg",
	".jpeg": "image/jpeg",
	".png":  "image/png",
	".webp": "image/webp",
	".mp4":  "video/mp4",
	".webm": "video/webm",
}

// InferContentType returns the MIME type for *url* based on its extension.
func InferContentType(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	if mime, ok := extensionMIME[strings.ToLower(path.Ext(u.Path))]; ok {
		return mime
	}
	return ""
}

// ResolveDownloadURL splits a *file_path* (full URL, absolute path, or
// relative path) into (url, origin, referer).
func ResolveDownloadURL(filePath string) (rawURL, origin, referer string) {
	parsed, err := url.Parse(filePath)
	if err == nil && parsed.Scheme != "" && parsed.Host != "" {
		rawURL = filePath
		origin = fmt.Sprintf("%s://%s", parsed.Scheme, parsed.Host)
		return rawURL, origin, origin + "/"
	}
	p := filePath
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return AssetsDownload + p, AssetsDownload, AssetsDownload + "/"
}

// ResolveAssetReference returns the absolute content URL for an uploaded asset.
// Falls back to {cdn}/users/{user_id}/{file_id}/content when only file_id is
// available.
func ResolveAssetReference(fileID, fileURI string, userID string) string {
	if fileURI != "" {
		url, _, _ := ResolveDownloadURL(fileURI)
		return url
	}
	if fileID != "" && userID != "" {
		return fmt.Sprintf("%s/users/%s/%s/content", AssetsDownload, userID, fileID)
	}
	return ""
}

// ParseDataURI splits a data URI into (filename, base64Content, mimeType).
// Returns an error when the URI is malformed.
func ParseDataURI(dataURI string) (filename, b64, mime string, err error) {
	if !strings.HasPrefix(dataURI, "data:") {
		return "", "", "", platform.ValidationError("File input must be a URL or data URI", "content")
	}
	comma := strings.IndexByte(dataURI, ',')
	if comma < 0 {
		return "", "", "", platform.ValidationError("Malformed data URI: missing comma separator", "content")
	}
	header := dataURI[:comma]
	b64 = dataURI[comma+1:]
	if !strings.Contains(header, ";base64") {
		return "", "", "", platform.ValidationError("Data URI must be base64-encoded", "content")
	}
	mime = strings.TrimSpace(header[len("data:"):strings.Index(header, ";")])
	if mime == "" {
		mime = "application/octet-stream"
	}
	b64 = stripWhitespace(b64)
	if b64 == "" {
		return "", "", "", platform.ValidationError("Data URI has empty payload", "content")
	}
	ext := "bin"
	if i := strings.IndexByte(mime, '/'); i >= 0 {
		ext = mime[i+1:]
	}
	return "file." + ext, b64, mime, nil
}

func stripWhitespace(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			continue
		}
		out = append(out, c)
	}
	return string(out)
}

// UploadFile uploads base64-encoded content to Grok and returns the file
// metadata ID used as a chat attachment reference.
func UploadFile(ctx context.Context, t *Transport, token, filename, mime, b64 string) (AssetUploadResult, error) {
	cfg := config.Global()
	timeoutS := cfg.GetInt("asset.upload_timeout", 60)
	payload := map[string]any{
		"fileName":     filename,
		"fileMimeType": mime,
		"content":      b64,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return AssetUploadResult{}, platform.UpstreamError("asset upload encode failed: "+err.Error(), 500, "")
	}
	resp, err := t.PostJSON(ctx, AssetsUpload, token, body,
		WithTimeout(time.Duration(timeoutS)*time.Second),
	)
	if err != nil {
		return AssetUploadResult{}, err
	}
	result := AssetUploadResult{}
	if v, ok := resp["fileMetadataId"].(string); ok {
		result.FileID = v
	} else if v, ok := resp["fileId"].(string); ok {
		result.FileID = v
	}
	if v, ok := resp["fileUri"].(string); ok {
		result.FileURI = v
	}
	return result, nil
}

// UploadFromInput parses *fileInput* (URL or data URI), fetches remote bytes
// when needed, then uploads to Grok.
func UploadFromInput(ctx context.Context, t *Transport, token, fileInput string) (AssetUploadResult, error) {
	if isURL(fileInput) {
		// Fetch the remote URL through the transport and re-upload as base64.
		cfg := config.Global()
		timeoutS := cfg.GetInt("asset.upload_timeout", 60)
		reader, err := t.GetBytes(ctx, fileInput, token,
			WithOrigin(""), WithReferer(""),
			WithTimeout(time.Duration(timeoutS)*time.Second),
		)
		if err != nil {
			return AssetUploadResult{}, err
		}
		defer reader.Close()
		raw, err := io.ReadAll(reader)
		if err != nil {
			return AssetUploadResult{}, platform.UpstreamError("asset fetch read failed: "+err.Error(), 502, "")
		}
		mime := InferContentType(fileInput)
		if mime == "" {
			mime = "application/octet-stream"
		}
		filename := path.Base(fileInput)
		if filename == "" || filename == "/" || filename == "." {
			filename = "download"
		}
		b64 := base64.StdEncoding.EncodeToString(raw)
		return UploadFile(ctx, t, token, filename, mime, b64)
	}
	filename, b64, mime, err := ParseDataURI(fileInput)
	if err != nil {
		return AssetUploadResult{}, err
	}
	return UploadFile(ctx, t, token, filename, mime, b64)
}

// ListAssets returns the asset list for *token*.
func ListAssets(ctx context.Context, t *Transport, token string) (map[string]any, error) {
	cfg := config.Global()
	timeoutS := cfg.GetInt("asset.list_timeout", 60)
	return t.GetJSON(ctx, AssetsListURL, token,
		WithTimeout(time.Duration(timeoutS)*time.Second),
	)
}

// DeleteAsset deletes a single asset by ID.
func DeleteAsset(ctx context.Context, t *Transport, token, assetID string) (map[string]any, error) {
	cfg := config.Global()
	timeoutS := cfg.GetInt("asset.delete_timeout", 60)
	return t.DeleteJSON(ctx, AssetsDeleteURL+"/"+assetID, token,
		WithTimeout(time.Duration(timeoutS)*time.Second),
	)
}

// ResolveUploadedAssetReference resolves an uploaded asset to the content URL
// required by image-edit requests. The userID is extracted from the SSO token's
// cookie (the upstream sometimes embeds x-userid when set via cf_cookies).
func ResolveUploadedAssetReference(token, fileID, fileURI string) (string, error) {
	userID := extractUserID(token)
	url := ResolveAssetReference(fileID, fileURI, userID)
	if url == "" {
		return "", platform.UpstreamError("Could not resolve uploaded asset reference URL", 502, "")
	}
	return url, nil
}

// extractUserID returns the x-userid value from the cookie header built for
// *token*. Returns "" when no x-userid cookie is present.
func extractUserID(token string) string {
	cookie := BuildSSOCookie(token, resolveProxyProfile())
	for _, part := range strings.Split(cookie, ";") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "x-userid=") {
			return strings.TrimPrefix(part, "x-userid=")
		}
	}
	return ""
}

func isURL(value string) bool {
	u, err := url.Parse(value)
	if err != nil {
		return false
	}
	return (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}
