package api

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// sseWriter writes Server-Sent Events frames to an HTTP response.
type sseWriter struct {
	w       http.ResponseWriter
	flusher http.Flusher
}

func newSSEWriter(w http.ResponseWriter) *sseWriter {
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	return &sseWriter{w: w, flusher: flusher}
}

// writeData emits "data: <payload>\n\n".
func (s *sseWriter) writeData(payload string) {
	fmt.Fprintf(s.w, "data: %s\n\n", payload)
	if s.flusher != nil {
		s.flusher.Flush()
	}
}

// writeJSONData emits "data: <json>\n\n" with the JSON-encoded payload.
func (s *sseWriter) writeJSONData(v any) {
	b, _ := json.Marshal(v)
	s.writeData(string(b))
}

// writeEvent emits "event: <type>\ndata: <payload>\n\n".
func (s *sseWriter) writeEvent(eventType, payload string) {
	fmt.Fprintf(s.w, "event: %s\ndata: %s\n\n", eventType, payload)
	if s.flusher != nil {
		s.flusher.Flush()
	}
}

// writeEventJSON emits "event: <type>\ndata: <json>\n\n".
func (s *sseWriter) writeEventJSON(eventType string, v any) {
	b, _ := json.Marshal(v)
	s.writeEvent(eventType, string(b))
}

// writeComment emits ": <comment>\n\n" (a comment/heartbeat line).
func (s *sseWriter) writeComment(comment string) {
	fmt.Fprintf(s.w, ": %s\n\n", comment)
	if s.flusher != nil {
		s.flusher.Flush()
	}
}

// writeDone emits the terminal "data: [DONE]\n\n".
func (s *sseWriter) writeDone() {
	s.writeData("[DONE]")
}

// writeOpenAIError emits the OpenAI-style SSE error frame, then [DONE].
func (s *sseWriter) writeOpenAIError(message string, kind string, code string, param string) {
	errObj := map[string]any{"message": message, "type": kind, "code": code}
	if param != "" {
		errObj["param"] = param
	}
	s.writeEvent("error", "")
	// SSE "event:" line is followed by a data: line with the actual payload.
	// Some clients ignore the event line for OpenAI-style errors, so we
	// emit the error object under "data:" as well.
	s.writeJSONData(map[string]any{"error": errObj})
	s.writeDone()
}

// writeAnthropicError emits the Anthropic SSE error frame.
func (s *sseWriter) writeAnthropicError(message string, kind string, code string) {
	errObj := map[string]any{"type": "error", "error": map[string]any{
		"type": kind, "message": message, "code": code,
	}}
	s.writeEvent("error", "")
	s.writeJSONData(errObj)
	s.writeDone()
}
