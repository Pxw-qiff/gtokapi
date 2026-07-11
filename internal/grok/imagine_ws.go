// Package grok — WebSocket image generation protocol for
// wss://grok.com/ws/imagine/listen.
package grok

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"
)

const (
	// WSImagineURL is the WebSocket endpoint for image generation.
	WSImagineURL = "wss://grok.com/ws/imagine/listen"

	// ImageBaseURL is the CDN prefix for generated images (with trailing slash).
	ImageBaseURL = "https://assets.grok.com/"
)

var imageURLPattern = regexp.MustCompile(`/images/([a-f0-9\-]+)\.(png|jpg|jpeg)`)

// AspectRatioMap maps OpenAI size strings to Grok aspect ratios.
var AspectRatioMap = map[string]string{
	"1280x720":  "16:9",
	"16:9":      "16:9",
	"720x1280":  "9:16",
	"9:16":      "9:16",
	"1792x1024": "3:2",
	"3:2":       "3:2",
	"1024x1792": "2:3",
	"2:3":       "2:3",
	"1024x1024": "1:1",
	"1:1":       "1:1",
}

// ResolveAspectRatio converts an OpenAI size to a Grok aspect ratio.
func ResolveAspectRatio(size string) string {
	if ar, ok := AspectRatioMap[size]; ok {
		return ar
	}
	return "2:3"
}

// --- Client message builders ---

// BuildImagineResetMessage builds the reset message sent before each prompt.
func BuildImagineResetMessage() map[string]any {
	return map[string]any{
		"type":      "conversation.item.create",
		"timestamp": time.Now().UnixMilli(),
		"item": map[string]any{
			"type": "message",
			"content": []any{
				map[string]any{"type": "reset"},
			},
		},
	}
}

// BuildImagineRequestMessage builds the image generation request message.
func BuildImagineRequestMessage(requestID, prompt, aspectRatio string, enableNSFW, enablePro bool) map[string]any {
	return map[string]any{
		"type":      "conversation.item.create",
		"timestamp": time.Now().UnixMilli(),
		"item": map[string]any{
			"type": "message",
			"content": []any{
				map[string]any{
					"requestId": requestID,
					"text":      prompt,
					"type":      "input_text",
					"properties": map[string]any{
						"section_count":       0,
						"is_kids_mode":        false,
						"enable_nsfw":         enableNSFW,
						"skip_upsampler":      false,
						"enable_side_by_side": true,
						"is_initial":          false,
						"aspect_ratio":        aspectRatio,
						"enable_pro":          enablePro,
					},
				},
			},
		},
	}
}

// --- Server frame parsers ---

// ParseImageURL extracts (imageID, ext) from a /images/{id}.{ext} URL.
func ParseImageURL(rawURL string) (string, string) {
	m := imageURLPattern.FindStringSubmatch(rawURL)
	if len(m) >= 3 {
		return m[1], m[2]
	}
	return genUUID(), "jpg"
}

// ImagineSlotStatus represents the status of an image generation slot.
type ImagineSlotStatus struct {
	Status    string
	ImageID   string
	Order     int
	Width     int
	Height    int
	Moderated bool
	RRated    bool
}

// ParseImagineJSONFrame parses a {type:"json"} server frame.
func ParseImagineJSONFrame(msg map[string]any) *ImagineSlotStatus {
	status, _ := msg["current_status"].(string)
	if status != "start_stage" && status != "completed" {
		return nil
	}
	imageID := ""
	if v, ok := msg["image_id"].(string); ok {
		imageID = v
	} else if v, ok := msg["job_id"].(string); ok {
		imageID = v
	}
	if imageID == "" {
		return nil
	}
	order := 0
	if v, ok := msg["order"].(float64); ok {
		order = int(v)
	}
	width := 0
	if v, ok := msg["width"].(float64); ok {
		width = int(v)
	}
	height := 0
	if v, ok := msg["height"].(float64); ok {
		height = int(v)
	}
	return &ImagineSlotStatus{
		Status:    status,
		ImageID:   imageID,
		Order:     order,
		Width:     width,
		Height:    height,
		Moderated: msg["moderated"] == true,
		RRated:    msg["r_rated"] == true,
	}
}

// ImagineEventKind enumerates the event types from the WS imagine stream.
type ImagineEventKind string

const (
	ImagineEventImage     ImagineEventKind = "image"
	ImagineEventProgress  ImagineEventKind = "progress"
	ImagineEventModerated ImagineEventKind = "moderated"
	ImagineEventError     ImagineEventKind = "error"
)

// ImagineEvent is one event from the WS image generation stream.
type ImagineEvent struct {
	Type      ImagineEventKind
	ImageID   string
	Order     int
	Progress  int
	Blob      string
	URL       string
	Width     int
	Height    int
	IsFinal   bool
	Moderated bool
	Error     string
}

// IsWSImageModel returns true if the model should use WS-based generation.
func IsWSImageModel(modelName string) bool {
	return modelName == "grok-imagine-image" || modelName == "grok-imagine-image-pro"
}

// IsProImageModel returns true if the model uses quality (pro) mode.
func IsProImageModel(modelName string) bool {
	return modelName == "grok-imagine-image-pro"
}

// FormatImageProgress formats a progress string for reasoning_content.
func FormatImageProgress(label string, progress int, completed, total int) string {
	if completed >= 0 && total > 0 {
		return fmt.Sprintf("%s正在生成 %d%% (%d/%d)", label, progress, completed, total)
	}
	return fmt.Sprintf("%s正在生成 %d%%", label, progress)
}

// MarshalJSONBytes marshals v to JSON bytes.
func MarshalJSONBytes(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
