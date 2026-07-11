// Package grok video.go - 视频生成协议：payload 构建、SSE 流解析、尺寸/分段映射。
//
// 【修改说明】
// 修改背景：aurora 原视频生成用 BuildChatPayload（聊天接口），生成的是图片不是视频
// 解决问题：移植 jiujiu532 的正确视频流程：media post -> imagine-video-gen -> SSE 解析
// 设计考虑：独立文件，不侵入现有 chat.go 的 BuildChatPayload 逻辑
// 注意事项：视频秒数 > 10 时需要多段拼接（12=[6,6], 16=[10,6], 20=[10,10]）
package grok

import (
	"fmt"
	"strings"
)

// -----------------------------------------------------------------------
// 常量
// -----------------------------------------------------------------------

const (
	VideoMediaType        = "MEDIA_POST_TYPE_VIDEO"
	ImageMediaType        = "MEDIA_POST_TYPE_IMAGE"
	VideoModelName        = "imagine-video-gen"
	VideoExtensionRefType = "ORIGINAL_REF_TYPE_VIDEO_EXTENSION"
)

// 尺寸 -> (aspectRatio, resolutionName)
var videoSizeMap = map[string][2]string{
	"720x1280":  {"9:16", "720p"},
	"1280x720":  {"16:9", "720p"},
	"1024x1024": {"1:1", "720p"},
	"1024x1792": {"9:16", "720p"},
	"1792x1024": {"16:9", "720p"},
}

// preset -> mode flag
var videoPresetFlags = map[string]string{
	"fun":    "--mode=extremely-crazy",
	"normal": "--mode=normal",
	"spicy":  "--mode=extremely-spicy-or-crazy",
	"custom": "--mode=custom",
}

// -----------------------------------------------------------------------
// 数据结构
// -----------------------------------------------------------------------

// VideoStreamResult 是从 SSE 帧 result.response.streamingVideoGenerationResponse 中解析出的视频状态。
type VideoStreamResult struct {
	Progress     int
	VideoPostID  string
	VideoURL     string
	AssetID      string
	ThumbnailURL string
	Moderated    bool
}

// VideoArtifact 是一段视频生成完成后的产物。
type VideoArtifact struct {
	VideoURL    string
	VideoPostID string
	AssetID     string
	Thumbnail   string
}

// -----------------------------------------------------------------------
// 尺寸 / 分段 / preset 解析
// -----------------------------------------------------------------------

// ResolveVideoSize 将尺寸字符串映射为 (aspectRatio, resolutionName)。
func ResolveVideoSize(size string) (string, string) {
	if v, ok := videoSizeMap[size]; ok {
		return v[0], v[1]
	}
	return "9:16", "720p"
}

// ResolveVideoPresetFlag 返回 preset 对应的 --mode=xxx flag。
func ResolveVideoPresetFlag(preset string) string {
	if v, ok := videoPresetFlags[preset]; ok {
		return v
	}
	return videoPresetFlags["custom"]
}

// IsValidVideoLength 检查秒数是否合法。
func IsValidVideoLength(n int) bool {
	switch n {
	case 6, 10, 12, 16, 20:
		return true
	}
	return false
}

// BuildSegmentLengths 将总秒数拆分为分段列表。
// 6->[6], 10->[10], 12->[6,6], 16->[10,6], 20->[10,10]
func BuildSegmentLengths(seconds int) []int {
	switch seconds {
	case 6:
		return []int{6}
	case 10:
		return []int{10}
	case 12:
		return []int{6, 6}
	case 16:
		return []int{10, 6}
	case 20:
		return []int{10, 10}
	default:
		return []int{6}
	}
}

// VideoExtendStartTime 计算续写段的起始时间（秒）。
func VideoExtendStartTime(seconds int) float64 {
	return float64(seconds) + (1.0 / 24.0)
}

// -----------------------------------------------------------------------
// Payload 构建
// -----------------------------------------------------------------------

// BuildVideoPostPayload 构建 POST /rest/media/post/create 的 payload。
// 返回 {"mediaType": "MEDIA_POST_TYPE_VIDEO", "prompt": "..."}
func BuildVideoPostPayload(prompt string) map[string]any {
	return map[string]any{
		"mediaType": VideoMediaType,
		"prompt":    prompt,
	}
}

// BuildVideoGenPayload 构建第一段视频生成请求的 payload。
// 发送到 POST /rest/app-chat/conversations/new，modelName 为 imagine-video-gen。
func BuildVideoGenPayload(prompt, parentPostID, aspectRatio, resolutionName, preset string, videoLength int) map[string]any {
	return map[string]any{
		"temporary":        true,
		"modelName":        VideoModelName,
		"message":          fmt.Sprintf("%s %s", prompt, ResolveVideoPresetFlag(preset)),
		"enableSideBySide": true,
		"responseMetadata": map[string]any{
			"experiments": []any{},
			"modelConfigOverride": map[string]any{
				"modelMap": map[string]any{
					"videoGenModelConfig": map[string]any{
						"parentPostId":   parentPostID,
						"aspectRatio":    aspectRatio,
						"videoLength":    videoLength,
						"resolutionName": resolutionName,
					},
				},
			},
		},
	}
}

// BuildVideoExtendPayload 构建视频续写段（第二段及以后）的 payload。
// 与 BuildVideoGenPayload 的区别：videoGenModelConfig 中包含 isVideoExtension 等续写参数。
func BuildVideoExtendPayload(prompt, parentPostID, extendPostID, aspectRatio, resolutionName, preset string, videoLength int, startTime float64) map[string]any {
	return map[string]any{
		"temporary":        true,
		"modelName":        VideoModelName,
		"message":          fmt.Sprintf("%s %s", prompt, ResolveVideoPresetFlag(preset)),
		"enableSideBySide": true,
		"responseMetadata": map[string]any{
			"experiments": []any{},
			"modelConfigOverride": map[string]any{
				"modelMap": map[string]any{
					"videoGenModelConfig": map[string]any{
						"isVideoExtension":        true,
						"videoExtensionStartTime": startTime,
						"extendPostId":            extendPostID,
						"stitchWithExtendPostId":  true,
						"originalPrompt":          prompt,
						"originalPostId":          parentPostID,
						"originalRefType":         VideoExtensionRefType,
						"mode":                    preset,
						"aspectRatio":             aspectRatio,
						"videoLength":             videoLength,
						"resolutionName":          resolutionName,
						"parentPostId":            parentPostID,
						"isVideoEdit":             false,
					},
				},
			},
		},
	}
}

// -----------------------------------------------------------------------
// SSE 流解析
// -----------------------------------------------------------------------

// ParseVideoStreamResponse 从 SSE 帧的顶层 JSON 对象中提取 streamingVideoGenerationResponse。
// 路径：obj -> result -> response -> streamingVideoGenerationResponse
// 返回 nil 表示该帧不包含视频流数据。
func ParseVideoStreamResponse(obj map[string]any) *VideoStreamResult {
	result, _ := obj["result"].(map[string]any)
	if result == nil {
		return nil
	}
	resp, _ := result["response"].(map[string]any)
	if resp == nil {
		return nil
	}
	stream, _ := resp["streamingVideoGenerationResponse"].(map[string]any)
	if stream == nil {
		return nil
	}

	r := &VideoStreamResult{}
	if v, ok := stream["progress"].(float64); ok {
		r.Progress = int(v)
	}
	if v, ok := stream["videoPostId"].(string); ok {
		r.VideoPostID = v
	}
	if r.VideoPostID == "" {
		if v, ok := stream["videoId"].(string); ok {
			r.VideoPostID = v
		}
	}
	if v, ok := stream["videoUrl"].(string); ok {
		r.VideoURL = v
	}
	if v, ok := stream["assetId"].(string); ok {
		r.AssetID = v
	}
	if v, ok := stream["thumbnailImageUrl"].(string); ok {
		r.ThumbnailURL = v
	}
	if v, ok := stream["moderated"].(bool); ok {
		r.Moderated = v
	}
	return r
}

// AbsolutizeVideoURL 确保视频 URL 是绝对路径。
// 相对路径会补上 https://assets.grok.com/ 前缀。
func AbsolutizeVideoURL(url string) string {
	if url == "" {
		return ""
	}
	if strings.HasPrefix(url, "http") {
		return url
	}
	return imageBaseURL + strings.TrimPrefix(url, "/")
}