package grok

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strings"
	"unicode"

	"net/http"
)

// EncodeGRPCWebPayload wraps *data* in a gRPC-Web data frame (flag=0x00).
// Format: 0x00 + uint32 BE length + payload.
func EncodeGRPCWebPayload(data []byte) []byte {
	out := make([]byte, 5+len(data))
	out[0] = 0x00
	binary.BigEndian.PutUint32(out[1:5], uint32(len(data)))
	copy(out[5:], data)
	return out
}

// GRPCStatus carries the gRPC code and message extracted from trailers.
type GRPCStatus struct {
	Code    int
	Message string
}

// OK returns true when the gRPC code is 0 (OK).
func (s GRPCStatus) OK() bool { return s.Code == 0 }

// HTTPEquiv returns the best-effort HTTP-equivalent status code for the grpc code.
func (s GRPCStatus) HTTPEquiv() int {
	switch s.Code {
	case 0:
		return 200
	case 4:
		return 504
	case 7:
		return 403
	case 8:
		return 429
	case 14:
		return 503
	case 16:
		return 401
	}
	return 502
}

// grpcHTTP maps gRPC status codes to HTTP equivalents.
var grpcHTTP = map[int]int{0: 200, 4: 504, 7: 403, 8: 429, 14: 503, 16: 401}

// ParseGRPCWebResponse parses a gRPC-Web response body.
// Returns (messages, trailers). Trailers contains "grpc-status" / "grpc-message".
func ParseGRPCWebResponse(body []byte, contentType string, headers http.Header) ([][]byte, map[string]string) {
	decoded := maybeDecodeBase64(body, contentType)
	messages := [][]byte{}
	trailers := map[string]string{}

	i, n := 0, len(decoded)
	for i < n {
		if n-i < 5 {
			break
		}
		flag := decoded[i]
		length := binary.BigEndian.Uint32(decoded[i+1 : i+5])
		i += 5
		if uint32(n-i) < length {
			break
		}
		payload := decoded[i : i+int(length)]
		i += int(length)

		if flag&0x80 != 0 {
			for k, v := range parseGRPCTrailers(payload) {
				trailers[k] = v
			}
		} else if flag&0x01 != 0 {
			// compressed frame not supported; ignore
			continue
		} else {
			messages = append(messages, payload)
		}
	}

	// Supplement from HTTP headers (some servers send trailers as headers).
	for _, key := range []string{"grpc-status", "grpc-message"} {
		if _, ok := trailers[key]; ok {
			continue
		}
		val := headerGetCI(headers, key)
		if val == "" {
			continue
		}
		val = strings.TrimSpace(val)
		if key == "grpc-message" {
			val = unquoteGRPCMessage(val)
		}
		trailers[key] = val
	}
	if raw, ok := trailers["grpc-message"]; ok {
		trailers["grpc-message"] = unquoteGRPCMessage(raw)
	}
	return messages, trailers
}

// GetGRPCStatus extracts a GRPCStatus from parsed trailers.
func GetGRPCStatus(trailers map[string]string) GRPCStatus {
	raw := strings.TrimSpace(trailers["grpc-status"])
	msg := strings.TrimSpace(trailers["grpc-message"])
	code := -1
	if raw != "" {
		var n int
		_, err := fmt.Sscanf(raw, "%d", &n)
		if err == nil {
			code = n
		}
	}
	return GRPCStatus{Code: code, Message: msg}
}

func parseGRPCTrailers(payload []byte) map[string]string {
	out := map[string]string{}
	text := string(payload)
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimRight(line, "\r")
		idx := strings.IndexByte(line, ':')
		if idx < 0 {
			continue
		}
		k := strings.TrimSpace(line[:idx])
		v := strings.TrimSpace(line[idx+1:])
		out[strings.ToLower(k)] = v
	}
	return out
}

// maybeDecodeBase64 unwraps grpc-web-text payloads (base64-encoded bodies).
func maybeDecodeBase64(body []byte, contentType string) []byte {
	ct := strings.ToLower(contentType)
	if strings.Contains(ct, "grpc-web-text") {
		joined := bytes.Map(func(r rune) rune {
			if unicode.IsSpace(r) {
				return -1
			}
			return r
		}, body)
		if decoded, err := base64.StdEncoding.DecodeString(string(joined)); err == nil {
			return decoded
		}
		return body
	}
	// Heuristic: if the leading bytes look like base64 and not a gRPC frame,
	// attempt to decode.
	if len(body) >= 1 && body[0] != 0x00 && body[0] != 0x80 {
		head := body
		if len(head) > 2048 {
			head = head[:2048]
		}
		if isProbablyBase64(head) {
			joined := stripSpace(body)
			if decoded, err := base64.StdEncoding.DecodeString(string(joined)); err == nil {
				return decoded
			}
		}
	}
	return body
}

func isProbablyBase64(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	for _, c := range b {
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') || c == '+' || c == '/' || c == '=' ||
			c == '\r' || c == '\n' || c == ' ' || c == '\t') {
			return false
		}
	}
	return true
}

func stripSpace(b []byte) []byte {
	out := make([]byte, 0, len(b))
	for _, c := range b {
		if c == ' ' || c == '\t' || c == '\r' || c == '\n' {
			continue
		}
		out = append(out, c)
	}
	return out
}

func unquoteGRPCMessage(s string) string {
	// gRPC-Web percent-encodes the message; do a best-effort unquote.
	if s == "" {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '%' && i+2 < len(s) {
			hi := fromHex(s[i+1])
			lo := fromHex(s[i+2])
			if hi >= 0 && lo >= 0 {
				b.WriteByte(byte(hi<<4 | lo))
				i += 2
				continue
			}
		}
		b.WriteByte(c)
	}
	return b.String()
}

func fromHex(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	}
	return -1
}

// headerGetCI does a case-insensitive header lookup.
func headerGetCI(h http.Header, key string) string {
	if h == nil {
		return ""
	}
	lk := strings.ToLower(key)
	for k, vs := range h {
		if strings.ToLower(k) == lk && len(vs) > 0 {
			return vs[0]
		}
	}
	return ""
}
