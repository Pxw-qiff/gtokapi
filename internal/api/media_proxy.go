package api

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/aurora-develop/grok2api/internal/config"
)

// mediaProxy 提供将 Grok 远程资源下载到本地并返回本地代理 URL 的能力。
//
// 【修改说明】
// 修改背景：用户希望像 Python 版 grok2api 一样，把图片/视频下载到本地，通过配置的 app_url 访问
// 解决问题：统一封装资源下载、本地保存、URL 转换逻辑，供图片/视频接口复用
// 设计考虑：复用现有 LocalMediaCacheStore 和 /v1/files/image、/v1/files/video 路由
// 注意事项：
//   - app_url 未配置时返回相对路径 /v1/files/...
//   - 下载失败时回退到原始 Grok URL，避免阻塞主流程
//   - 图片支持 grok_url / local_url / base64；视频支持 grok_url / local_url

// appURL 返回配置的应用访问地址，去掉末尾斜杠。
func appURL() string {
	return strings.TrimRight(config.Global().GetStr("app.app_url", ""), "/")
}

// imageFormat 返回当前配置的图片返回格式，默认 grok_url。
func imageFormat() string {
	fmtStr := strings.ToLower(strings.TrimSpace(config.Global().GetStr("features.image_format", "grok_url")))
	switch fmtStr {
	case "local_url", "base64":
		return fmtStr
	default:
		return "grok_url"
	}
}

// videoFormat 返回当前配置的视频返回格式，默认 grok_url。
func videoFormat() string {
	fmtStr := strings.ToLower(strings.TrimSpace(config.Global().GetStr("features.video_format", "grok_url")))
	if fmtStr == "local_url" {
		return fmtStr
	}
	return "grok_url"
}

// localImageURL 根据 fileID 生成本地图片代理 URL。
func localImageURL(fileID string) string {
	base := appURL()
	if base == "" {
		return fmt.Sprintf("/v1/files/image?id=%s", fileID)
	}
	return fmt.Sprintf("%s/v1/files/image?id=%s", base, fileID)
}

// localVideoURL 根据 fileID 生成本地视频代理 URL。
func localVideoURL(fileID string) string {
	base := appURL()
	if base == "" {
		return fmt.Sprintf("/v1/files/video?id=%s", fileID)
	}
	return fmt.Sprintf("%s/v1/files/video?id=%s", base, fileID)
}

// extractFileIDFromURL 从 Grok 资源 URL 中提取可用作本地文件 ID 的字符串。
// 优先使用 URL 路径最后一段的主文件名，无法提取时用 SHA1 前 32 位。
func extractFileIDFromURL(rawURL string) string {
	if rawURL == "" {
		return ""
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		h := sha1.Sum([]byte(rawURL))
		return hex.EncodeToString(h[:])[:32]
	}
	base := path.Base(u.Path)
	if base == "" || base == "." || base == "/" {
		h := sha1.Sum([]byte(rawURL))
		return hex.EncodeToString(h[:])[:32]
	}
	stem := strings.SplitN(base, ".", 2)[0]
	if stem == "" {
		h := sha1.Sum([]byte(rawURL))
		return hex.EncodeToString(h[:])[:32]
	}
	return stem
}

// downloadMediaViaTransport 通过 Server.Transport 下载远程媒体资源。
// 返回原始字节和 content-type（当前 Transport 未返回 content-type，固定返回空字符串）。
func downloadMediaViaTransport(s *Server, rawURL string) ([]byte, string, error) {
	if rawURL == "" {
		return nil, "", fmt.Errorf("empty url")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	bodyReader, err := s.Transport.GetBytes(ctx, rawURL, "")
	if err != nil {
		return nil, "", err
	}
	defer bodyReader.Close()

	const maxSize = 200 << 20 // 200MB
	body, err := io.ReadAll(io.LimitReader(bodyReader, maxSize))
	if err != nil {
		return nil, "", err
	}
	return body, "", nil
}

// resolveImageURL 根据 image_format 把 Grok 图片 URL 转换成目标形式。
// 返回转换后的 URL（或 base64 字符串）以及可能的错误；出错时回退到原始 URL。
func resolveImageURL(s *Server, rawURL string) (string, error) {
	if rawURL == "" {
		return "", nil
	}
	fmtStr := imageFormat()
	if fmtStr == "grok_url" {
		return rawURL, nil
	}

	body, contentType, err := downloadMediaViaTransport(s, rawURL)
	if err != nil {
		return rawURL, err
	}

	if fmtStr == "base64" {
		mime := contentType
		if mime == "" {
			mime = inferImageMIME(rawURL)
		}
		return fmt.Sprintf("data:%s;base64,%s", mime, base64.StdEncoding.EncodeToString(body)), nil
	}

	// local_url
	fileID := extractFileIDFromURL(rawURL)
	if fileID == "" {
		fileID = randomFileID()
	}
	_, err = s.Media.SaveImage(body, contentType, fileID)
	if err != nil {
		return rawURL, err
	}
	return localImageURL(fileID), nil
}

// resolveVideoURL 根据 video_format 把 Grok 视频 URL 转换成目标形式。
// 返回要对外暴露的 URL 以及本地文件路径（local_url 模式时用于 /v1/videos/:id/content）。
// 出错时回退到原始 URL。
func resolveVideoURL(s *Server, rawURL string) (string, string, error) {
	if rawURL == "" {
		return "", "", nil
	}
	if videoFormat() == "grok_url" {
		return rawURL, "", nil
	}

	body, _, err := downloadMediaViaTransport(s, rawURL)
	if err != nil {
		return rawURL, "", err
	}

	fileID := extractFileIDFromURL(rawURL)
	if fileID == "" {
		fileID = randomFileID()
	}
	localPath, err := s.Media.SaveVideo(body, fileID)
	if err != nil {
		return rawURL, "", err
	}
	return localVideoURL(fileID), localPath, nil
}

// inferImageMIME 根据 URL/路径后缀推断图片 MIME 类型。
func inferImageMIME(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "image/jpeg"
	}
	ext := strings.ToLower(filepath.Ext(u.Path))
	switch ext {
	case ".png":
		return "image/png"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	case ".bmp":
		return "image/bmp"
	default:
		return "image/jpeg"
	}
}

// randomFileID 生成一个随机的本地文件 ID。
func randomFileID() string {
	h := sha1.Sum([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))
	return hex.EncodeToString(h[:])[:32]
}