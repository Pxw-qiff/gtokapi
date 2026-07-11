package api

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/aurora-develop/grok2api/internal/account"
	"github.com/aurora-develop/grok2api/internal/config"
	"github.com/aurora-develop/grok2api/internal/grok"
	"github.com/aurora-develop/grok2api/internal/model"
	"github.com/aurora-develop/grok2api/internal/platform"
)

// runConsoleChatWithRetry handles retry + account selection for console chat.
// Falls back to the Bearer SSO token from the request when the pool is empty.
func (s *Server) runConsoleChatWithRetry(c *gin.Context, req *chatCompletionRequest, spec *model.Spec, stream bool) {
	temp := 0.8
	if req.Temperature != nil {
		temp = *req.Temperature
	}
	topP := 0.95
	if req.TopP != nil {
		topP = *req.TopP
	}
	effort := ""
	if req.ReasoningEffort != nil {
		effort = *req.ReasoningEffort
	}

	apiToken, _ := c.Get("api_token")
	ssoToken, _ := apiToken.(string)

	maxRetries := selectionMaxRetries()
	exclude := []string{}
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		lease, _ := reserveAccount(c.Request.Context(), s.Directory, spec, exclude)
		if lease == nil {
			if s.Refresh != nil && attempt == 0 {
				_ = s.Refresh.RefreshOnDemand(c.Request.Context())
				lease, _ = reserveAccount(c.Request.Context(), s.Directory, spec, exclude)
			}
		}
		// Pool exhausted — fall back to the SSO token from Authorization header.
		if lease == nil && ssoToken != "" {
			lease = &account.Lease{Token: ssoToken, ModeID: int(spec.ModeId)}
		}
		if lease == nil {
			writeAppError(c, platform.RateLimitError("No available accounts"))
			return
		}
		exclude = append(exclude, lease.Token)
		err := s.runConsoleChatOnce(c.Writer, c.Request, lease, req, temp, topP, effort, stream)
		s.Directory.Release(lease)
		if err == nil {
			s.feedback(lease.Token, account.FbSuccess, lease.ModeID, nil, nil)
			return
		}
		s.feedbackError(lease.Token, err, lease.ModeID)
		lastErr = err
		if !shouldRetryUpstream(err) || attempt == maxRetries {
			writeAppError(c, err)
			return
		}
	}
	if lastErr != nil {
		writeAppError(c, lastErr)
	}
}

// runConsoleChatOnce executes one console.x.ai chat attempt.
func (s *Server) runConsoleChatOnce(w http.ResponseWriter, r *http.Request, lease *account.Lease, req *chatCompletionRequest, temp, topP float64, effort string, stream bool) error {
	messages := make([]map[string]any, 0, len(req.Messages))
	for _, m := range req.Messages {
		messages = append(messages, m)
	}
	payload := grok.BuildConsolePayload(messages, req.Model, temp, topP, effort, stream)
	body, err := json.Marshal(payload)
	if err != nil {
		return platform.UpstreamError("encode console payload: "+err.Error(), 500, "")
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()

	bodyReader, err := s.Transport.PostStream(ctx, grok.ConsoleResponses, lease.Token, body,
		grok.WithConsoleMode())
	if err != nil {
		return err
	}
	defer bodyReader.Close()

	completionID := "chatcmpl-" + uuid.NewString()
	created := time.Now().Unix()
	adapter := grok.NewConsoleStreamAdapter()

	if stream {
		sw := newSSEWriter(w)
		sw.writeComment("heartbeat")
		scanner := bufio.NewScanner(bodyReader)
		scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
		var currentEvent string
		for scanner.Scan() {
			line := scanner.Text()
			kind, value := grok.ClassifyConsoleLine(line)
			if kind == "done" {
				break
			}
			if kind == "event" {
				currentEvent = value
				continue
			}
			if kind != "data" {
				continue
			}
			tokens, errObj := adapter.Feed(currentEvent, value)
			if errObj != nil {
				sw.writeOpenAIError(errObj.Message, string(errObj.Kind), errObj.Code, errObj.Param)
				return nil
			}
			for _, tok := range tokens {
				chunk := makeStreamChunk(completionID, created, req.Model, tok, "", false)
				sw.writeJSONData(chunk)
			}
		}
		finalChunk := makeStreamChunk(completionID, created, req.Model, "", "", true)
		sw.writeJSONData(finalChunk)
		sw.writeDone()
		return nil
	}

	// Non-streaming: aggregate tokens.
	scanner := bufio.NewScanner(bodyReader)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var currentEvent string
	for scanner.Scan() {
		line := scanner.Text()
		kind, value := grok.ClassifyConsoleLine(line)
		if kind == "done" {
			break
		}
		if kind == "event" {
			currentEvent = value
			continue
		}
		if kind != "data" {
			continue
		}
		_, errObj := adapter.Feed(currentEvent, value)
		if errObj != nil {
			return errObj
		}
	}
	text := adapter.FullText()
	resp := makeChatResponse(completionID, created, req.Model, text, "", false)
	if adapter.Usage != nil {
		resp["usage"] = adapter.Usage
	}
	b, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)
	return nil
}

// handleResponses serves the OpenAI Responses API (/v1/responses).
func (s *Server) handleResponses(c *gin.Context) {
	var req struct {
		Model        string           `json:"model"`
		Input        any              `json:"input"`
		Instructions string           `json:"instructions,omitempty"`
		Stream       *bool            `json:"stream,omitempty"`
		Reasoning    map[string]any   `json:"reasoning,omitempty"`
		Temperature  *float64         `json:"temperature,omitempty"`
		TopP         *float64         `json:"top_p,omitempty"`
		Tools        []map[string]any `json:"tools,omitempty"`
		ToolChoice   any              `json:"tool_choice,omitempty"`
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

	messages := responsesInputToMessages(req.Input, req.Instructions)
	chatReq := &chatCompletionRequest{
		Model:       req.Model,
		Messages:    messages,
		Stream:      &stream,
		Temperature: req.Temperature,
		TopP:        req.TopP,
		Tools:       req.Tools,
		ToolChoice:  req.ToolChoice,
	}
	if req.Reasoning != nil {
		if e, ok := req.Reasoning["effort"].(string); ok {
			chatReq.ReasoningEffort = &e
		}
	}

	if stream {
		s.handleResponsesStream(c, chatReq, spec)
	} else {
		s.handleResponsesNonStream(c, chatReq, spec)
	}
}

// responsesInputToMessages converts Responses API `input` to chat messages.
func responsesInputToMessages(input any, instructions string) []map[string]any {
	out := []map[string]any{}
	if strings.TrimSpace(instructions) != "" {
		out = append(out, map[string]any{"role": "system", "content": instructions})
	}
	switch v := input.(type) {
	case string:
		out = append(out, map[string]any{"role": "user", "content": v})
	case []any:
		for _, item := range v {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			t, _ := m["type"].(string)
			switch t {
			case "message":
				role, _ := m["role"].(string)
				if role == "" {
					role = "user"
				}
				out = append(out, map[string]any{"role": role, "content": responsesContentToText(m["content"])})
			case "function_call":
				name, _ := m["name"].(string)
				args, _ := m["arguments"].(string)
				out = append(out, map[string]any{
					"role": "assistant",
					"tool_calls": []any{map[string]any{
						"id": m["call_id"], "type": "function",
						"function": map[string]any{"name": name, "arguments": args},
					}},
				})
			case "function_call_output":
				out = append(out, map[string]any{
					"role":         "tool",
					"tool_call_id": m["call_id"],
					"content":      m["output"],
				})
			}
		}
	}
	return out
}

func responsesContentToText(content any) any {
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
			t, _ := m["type"].(string)
			if t == "input_text" || t == "output_text" || t == "text" {
				if text, ok := m["text"].(string); ok {
					b.WriteString(text)
				}
			}
			if t == "input_image" || t == "image" {
				if u, ok := m["image_url"].(string); ok {
					return []any{map[string]any{"type": "text", "text": ""}, map[string]any{"type": "image_url", "image_url": map[string]any{"url": u}}}
				}
			}
		}
		return b.String()
	}
	return content
}

// handleResponsesStream emits the Responses-API event stream.
func (s *Server) handleResponsesStream(c *gin.Context, req *chatCompletionRequest, spec *model.Spec) {
	sw := newSSEWriter(c.Writer)
	responseID := "resp_" + uuid.NewString()
	created := time.Now().Unix()

	createdEvent := map[string]any{
		"type": "response.created",
		"response": map[string]any{
			"id": responseID, "object": "response", "created_at": created,
			"status": "in_progress", "model": req.Model,
		},
	}
	sw.writeEventJSON("response.created", createdEvent)
	sw.writeComment("heartbeat")

	// Run non-streaming chat to capture text.
	nonStreamReq := *req
	f := false
	nonStreamReq.Stream = &f
	text := s.captureChatText(c.Request, &nonStreamReq, spec)

	itemID := "msg_" + uuid.NewString()
	outItem := map[string]any{
		"type":         "response.output_item.added",
		"output_index": 0,
		"item": map[string]any{
			"id": itemID, "type": "message", "role": "assistant",
			"status": "in_progress", "content": []any{},
		},
	}
	sw.writeEventJSON("response.output_item.added", outItem)
	sw.writeEventJSON("response.content_part.added", map[string]any{
		"type": "response.content_part.added", "item_id": itemID, "output_index": 0,
		"content_index": 0,
		"part":          map[string]any{"type": "output_text", "text": "", "annotations": []any{}},
	})
	if text != "" {
		sw.writeEventJSON("response.output_text.delta", map[string]any{
			"type": "response.output_text.delta", "item_id": itemID,
			"output_index": 0, "content_index": 0, "delta": text,
		})
	}
	sw.writeEventJSON("response.output_text.done", map[string]any{
		"type": "response.output_text.done", "item_id": itemID,
		"output_index": 0, "content_index": 0, "text": text,
	})
	sw.writeEventJSON("response.content_part.done", map[string]any{
		"type": "response.content_part.done", "item_id": itemID, "output_index": 0,
		"content_index": 0,
		"part":          map[string]any{"type": "output_text", "text": text, "annotations": []any{}},
	})
	sw.writeEventJSON("response.output_item.done", map[string]any{
		"type": "response.output_item.done", "output_index": 0,
		"item": map[string]any{
			"id": itemID, "type": "message", "role": "assistant",
			"content": []any{map[string]any{"type": "output_text", "text": text, "annotations": []any{}}},
			"status":  "completed",
		},
	})
	sw.writeEventJSON("response.completed", map[string]any{
		"type": "response.completed",
		"response": map[string]any{
			"id": responseID, "object": "response", "created_at": created,
			"status": "completed", "model": req.Model,
			"output": []any{map[string]any{
				"id": itemID, "type": "message", "role": "assistant",
				"content": []any{map[string]any{"type": "output_text", "text": text, "annotations": []any{}}},
				"status":  "completed",
			}},
			"usage": map[string]any{"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
		},
	})
	sw.writeDone()
}

// captureWriter is a fake http.ResponseWriter that captures the body.
type captureWriter struct {
	header http.Header
	status int
	body   []byte
	chunks []string
}

func (c *captureWriter) Header() http.Header {
	if c.header == nil {
		c.header = http.Header{}
	}
	return c.header
}
func (c *captureWriter) Write(p []byte) (int, error) {
	c.body = append(c.body, p...)
	return len(p), nil
}
func (c *captureWriter) WriteHeader(code int) { c.status = code }

// captureChatText runs the non-streaming chat path and extracts the text.
// Uses the internal runChatOnce methods directly (no retry loop — callers
// that need retry should handle it themselves).
func (s *Server) captureChatText(r *http.Request, req *chatCompletionRequest, spec *model.Spec) string {
	cw := &captureWriter{}

	temp := 0.8
	if req.Temperature != nil {
		temp = *req.Temperature
	}
	topP := 0.95
	if req.TopP != nil {
		topP = *req.TopP
	}

	// Select an account and run one attempt.
	lease, _ := reserveAccount(r.Context(), s.Directory, spec, nil)
	if lease == nil {
		return ""
	}
	defer s.Directory.Release(lease)

	var err error
	if spec.IsConsoleChat() {
		effort := ""
		if req.ReasoningEffort != nil {
			effort = *req.ReasoningEffort
		}
		err = s.runConsoleChatOnce(cw, r, lease, req, temp, topP, effort, false)
	} else {
		emitThink := resolveEmitThink(req.ReasoningEffort)
		message, fileInputs, perr := extractMessages(req.Messages)
		if perr != nil {
			return ""
		}
		err = s.runGrokChatOnce(cw, r, lease, spec, message, fileInputs, temp, topP, emitThink, false, req.Model)
	}
	if err != nil {
		return ""
	}

	var obj map[string]any
	if err := json.Unmarshal(cw.body, &obj); err != nil {
		return ""
	}
	choices, _ := obj["choices"].([]any)
	if len(choices) == 0 {
		return ""
	}
	choice, _ := choices[0].(map[string]any)
	msg, _ := choice["message"].(map[string]any)
	if msg == nil {
		return ""
	}
	text, _ := msg["content"].(string)
	return text
}

// handleResponsesNonStream returns a single response.completed object.
func (s *Server) handleResponsesNonStream(c *gin.Context, req *chatCompletionRequest, spec *model.Spec) {
	text := s.captureChatText(c.Request, req, spec)
	responseID := "resp_" + uuid.NewString()
	itemID := "msg_" + uuid.NewString()
	resp := map[string]any{
		"id":         responseID,
		"object":     "response",
		"created_at": time.Now().Unix(),
		"status":     "completed",
		"model":      req.Model,
		"output": []any{map[string]any{
			"id": itemID, "type": "message", "role": "assistant",
			"content": []any{map[string]any{"type": "output_text", "text": text, "annotations": []any{}}},
			"status":  "completed",
		}},
		"usage": map[string]any{"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
	}
	c.JSON(http.StatusOK, resp)
}
