package platform

import "fmt"

// ErrorKind is the machine-readable error type used in API responses.
type ErrorKind string

const (
	ErrValidation     ErrorKind = "invalid_request_error"
	ErrAuthentication ErrorKind = "authentication_error"
	ErrRateLimit       ErrorKind = "rate_limit_exceeded"
	ErrUpstream        ErrorKind = "upstream_error"
	ErrServer          ErrorKind = "server_error"
)

// AppError is the base error type carried through the request pipeline and
// converted to a JSON response by the global handler.
type AppError struct {
	Message string
	Kind    ErrorKind
	Code    string
	Status  int
	Param   string
	Body    string
}

func (e *AppError) Error() string {
	if e.Message == "" {
		return string(e.Kind)
	}
	return e.Message
}

// NewAppError builds an AppError with sane defaults.
func NewAppError(message string, kind ErrorKind, code string, status int) *AppError {
	return &AppError{Message: message, Kind: kind, Code: code, Status: status}
}

// ToDict renders the error in the OpenAI-style error envelope.
func (e *AppError) ToDict() map[string]any {
	err := map[string]any{
		"message": e.Message,
		"type":    string(e.Kind),
		"code":    e.Code,
	}
	if e.Param != "" {
		err["param"] = e.Param
	}
	return map[string]any{"error": err}
}

// ValidationError is a 400 invalid-request error.
func ValidationError(message, param string) *AppError {
	if param == "" {
		param = ""
	}
	return &AppError{Message: message, Kind: ErrValidation, Code: "invalid_value", Status: 400, Param: param}
}

// ValidationErrorCode is a 400 error with a specific code (e.g. model_not_found).
func ValidationErrorCode(message, param, code string) *AppError {
	return &AppError{Message: message, Kind: ErrValidation, Code: code, Status: 400, Param: param}
}

// AuthError is a 401 authentication error.
func AuthError(message string) *AppError {
	if message == "" {
		message = "Invalid or missing API key"
	}
	return &AppError{Message: message, Kind: ErrAuthentication, Code: "invalid_api_key", Status: 401}
}

// RateLimitError is a 429 rate-limit error.
func RateLimitError(message string) *AppError {
	if message == "" {
		message = "No available accounts"
	}
	return &AppError{Message: message, Kind: ErrRateLimit, Code: "rate_limit_exceeded", Status: 429}
}

// UpstreamError is an error from the upstream Grok service.
func UpstreamError(message string, status int, body string) *AppError {
	if status == 0 {
		status = 502
	}
	return &AppError{Message: message, Kind: ErrUpstream, Code: "upstream_error", Status: status, Body: body}
}

// StreamIdleTimeout is a 504 idle-timeout error on an SSE stream.
func StreamIdleTimeout(timeoutS int) *AppError {
	return &AppError{
		Message: fmt.Sprintf("stream idle timeout after %ds", timeoutS),
		Kind:    ErrUpstream, Code: "stream_idle_timeout", Status: 504,
	}
}

// IsInvalidCredentialsBody returns true when an upstream response body
// indicates the SSO token is no longer valid.
func IsInvalidCredentialsBody(body string) bool {
	if body == "" {
		return false
	}
	for _, needle := range invalidCredentialsMarkers {
		if containsCI(body, needle) {
			return true
		}
	}
	return false
}

var invalidCredentialsMarkers = []string{
	"invalid-credentials",
	"bad-credentials",
	"failed to look up session id",
	"blocked-user",
	"email-domain-rejected",
	"session not found",
	"account suspended",
	"token revoked",
	"token expired",
}

func containsCI(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	if len(s) < len(substr) {
		return false
	}
	ls := toLowerASCII(s)
	lsub := toLowerASCII(substr)
	for i := 0; i+len(lsub) <= len(ls); i++ {
		if ls[i:i+len(lsub)] == lsub {
			return true
		}
	}
	return false
}

func toLowerASCII(s string) string {
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}
