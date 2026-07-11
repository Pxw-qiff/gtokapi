package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/aurora-develop/grok2api/internal/config"
	"github.com/aurora-develop/grok2api/internal/model"
	"github.com/aurora-develop/grok2api/internal/platform"
)

// handleMessages serves the Anthropic-compatible /v1/messages endpoint.
func (s *Server) handleMessages(c *gin.Context) {
	var req struct {
		Model       string           `json:"model"`
		Messages    []map[string]any `json:"messages"`
		System      any              `json:"system,omitempty"`
		MaxTokens   *int             `json:"max_tokens,omitempty"`
		Stream      *bool            `json:"stream,omitempty"`
		Temperature *float64         `json:"temperature,omitempty"`
		TopP        *float64         `json:"top_p,omitempty"`
		Tools       []map[string]any `json:"tools,omitempty"`
		ToolChoice  any              `json:"tool_choice,omitempty"`
		Thinking    map[string]any   `json:"thinking,omitempty"`
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

	stream := config.Global().GetBool("features.stream", true)
	if req.Stream != nil {
		stream = *req.Stream
	}
	emitThink := false
	if req.Thinking != nil {
		if t, _ := req.Thinking["type"].(string); t == "enabled" {
			emitThink = true
		}
	} else {
		emitThink = config.Global().GetBool("features.thinking", true)
	}

	messages := anthropicToOpenAIMessages(req.System, req.Messages)
	chatReq := &chatCompletionRequest{
		Model:       req.Model,
		Messages:    messages,
		Stream:      &stream,
		Temperature: req.Temperature,
		TopP:        req.TopP,
		Tools:       req.Tools,
		ToolChoice:  req.ToolChoice,
	}

	if stream {
		s.handleAnthropicStream(c, chatReq, spec, emitThink)
	} else {
		s.handleAnthropicNonStream(c, chatReq, spec, emitThink)
	}
}

// anthropicToOpenAIMessages converts Anthropic-style messages to OpenAI-style.
func anthropicToOpenAIMessages(system any, messages []map[string]any) []map[string]any {
	out := []map[string]any{}
	if system != nil {
		switch s := system.(type) {
		case string:
			if strings.TrimSpace(s) != "" {
				out = append(out, map[string]any{"role": "system", "content": s})
			}
		case []any:
			var b strings.Builder
			for _, item := range s {
				m, _ := item.(map[string]any)
				if m == nil {
					continue
				}
				if t, _ := m["type"].(string); t == "text" {
					if text, _ := m["text"].(string); text != "" {
						b.WriteString(text)
					}
				}
			}
			if b.Len() > 0 {
				out = append(out, map[string]any{"role": "system", "content": b.String()})
			}
		}
	}
	for _, msg := range messages {
		role, _ := msg["role"].(string)
		if role == "" {
			role = "user"
		}
		content := msg["content"]
		switch c := content.(type) {
		case string:
			out = append(out, map[string]any{"role": role, "content": c})
		case []any:
			blocks := []any{}
			toolCalls := []any{}
			toolResults := []map[string]any{}
			for _, item := range c {
				bm, ok := item.(map[string]any)
				if !ok {
					continue
				}
				t, _ := bm["type"].(string)
				switch t {
				case "text":
					text, _ := bm["text"].(string)
					blocks = append(blocks, map[string]any{"type": "text", "text": text})
				case "image":
					src, _ := bm["source"].(map[string]any)
					if src != nil {
						srcType, _ := src["type"].(string)
						mediaType, _ := src["media_type"].(string)
						data, _ := src["data"].(string)
						if srcType == "base64" && data != "" {
							url := "data:" + mediaType + ";base64," + data
							blocks = append(blocks, map[string]any{
								"type":      "image_url",
								"image_url": map[string]any{"url": url},
							})
						}
						if srcType == "url" {
							if u, _ := src["url"].(string); u != "" {
								blocks = append(blocks, map[string]any{
									"type":      "image_url",
									"image_url": map[string]any{"url": u},
								})
							}
						}
					}
				case "tool_use":
					name, _ := bm["name"].(string)
					input := bm["input"]
					args, _ := json.Marshal(input)
					toolCalls = append(toolCalls, map[string]any{
						"id": bm["id"], "type": "function",
						"function": map[string]any{"name": name, "arguments": string(args)},
					})
				case "tool_result":
					toolResults = append(toolResults, map[string]any{
						"role":         "tool",
						"tool_call_id": bm["tool_use_id"],
						"content":      anthropicContentToString(bm["content"]),
					})
				}
			}
			if len(toolCalls) > 0 {
				out = append(out, map[string]any{"role": "assistant", "tool_calls": toolCalls})
			}
			out = append(out, toolResults...)
			if len(blocks) > 0 {
				out = append(out, map[string]any{"role": role, "content": blocks})
			}
		default:
			out = append(out, map[string]any{"role": role, "content": content})
		}
	}
	return out
}

func anthropicContentToString(content any) string {
	switch v := content.(type) {
	case string:
		return v
	case []any:
		var b strings.Builder
		for _, item := range v {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if t, _ := m["type"].(string); t == "text" {
				if text, _ := m["text"].(string); text != "" {
					b.WriteString(text)
				}
			}
		}
		return b.String()
	}
	return ""
}

// handleAnthropicNonStream emits the Anthropic message envelope.
func (s *Server) handleAnthropicNonStream(c *gin.Context, req *chatCompletionRequest, spec *model.Spec, emitThink bool) {
	text := s.captureChatText(c.Request, req, spec)
	messageID := "msg_" + uuid.NewString()
	blocks := []any{}
	if emitThink && text != "" {
		// No thinking buffer captured here for simplicity.
	}
	blocks = append(blocks, map[string]any{"type": "text", "text": text})
	resp := map[string]any{
		"id":            messageID,
		"type":          "message",
		"role":          "assistant",
		"model":         req.Model,
		"content":       blocks,
		"stop_reason":   "end_turn",
		"stop_sequence": nil,
		"usage":         map[string]any{"input_tokens": 0, "output_tokens": 0},
	}
	c.JSON(http.StatusOK, resp)
}

// handleAnthropicStream emits the Anthropic event stream.
func (s *Server) handleAnthropicStream(c *gin.Context, req *chatCompletionRequest, spec *model.Spec, emitThink bool) {
	sw := newSSEWriter(c.Writer)
	messageID := "msg_" + uuid.NewString()

	// 1. message_start
	sw.writeEventJSON("message_start", map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id": messageID, "type": "message", "role": "assistant",
			"model": req.Model, "content": []any{}, "stop_reason": nil,
			"usage": map[string]any{"input_tokens": 0, "output_tokens": 0},
		},
	})
	sw.writeEvent("ping", "")
	sw.writeJSONData(map[string]any{"type": "ping"})
	sw.writeComment("heartbeat")

	// Capture text via non-stream chat path.
	nonStreamReq := *req
	f := false
	nonStreamReq.Stream = &f
	text := s.captureChatText(c.Request, &nonStreamReq, spec)

	// 2. content_block_start
	sw.writeEventJSON("content_block_start", map[string]any{
		"type": "content_block_start", "index": 0,
		"content_block": map[string]any{"type": "text", "text": ""},
	})
	// 3. content_block_delta
	if text != "" {
		sw.writeEventJSON("content_block_delta", map[string]any{
			"type": "content_block_delta", "index": 0,
			"delta": map[string]any{"type": "text_delta", "text": text},
		})
	}
	// 4. content_block_stop.
	sw.writeEventJSON("content_block_stop", map[string]any{
		"type": "content_block_stop", "index": 0,
	})
	// 5. message_delta.
	sw.writeEventJSON("message_delta", map[string]any{
		"type":  "message_delta",
		"delta": map[string]any{"stop_reason": "end_turn", "stop_sequence": nil},
		"usage": map[string]any{"output_tokens": 0},
	})
	// 6. message_stop.
	sw.writeEventJSON("message_stop", map[string]any{"type": "message_stop"})
	sw.writeDone()
	_ = emitThink
}

// suppress unused import warnings.
var _ = config.Global
