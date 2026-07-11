package api

import (
	"crypto/subtle"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/aurora-develop/grok2api/internal/config"
	"github.com/aurora-develop/grok2api/internal/platform"
)

// extractBearer returns the token from an "Authorization: Bearer <x>" header.
func extractBearer(header string) string {
	header = strings.TrimSpace(header)
	if header == "" {
		return ""
	}
	const prefix = "Bearer "
	if len(header) < len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}

// verifyAPIKey middleware. When api_key is configured, requires a matching key
// or a valid SSO token. When api_key is empty, open mode.
// The extracted token is stored in gin.Context as "api_token" for downstream use.
func verifyAPIKey() gin.HandlerFunc {
	return func(c *gin.Context) {
		token := extractBearer(c.GetHeader("Authorization"))
		if token == "" {
			token = strings.TrimSpace(c.GetHeader("x-api-key"))
		}
		// Store the raw token so handlers can use it as a fallback SSO token.
		if token != "" {
			c.Set("api_token", token)
		}

		cfg := config.Global()
		keys := cfg.GetList("app.api_key", nil)

		// Open mode: no api_key configured → allow everything.
		if len(keys) == 0 {
			c.Next()
			return
		}

		// api_key configured → must provide a token.
		if token == "" {
			writeAppError(c, platform.AuthError("Missing or invalid Authorization header."))
			c.Abort()
			return
		}
		// Match against configured api_key list.
		for _, k := range keys {
			if token == k {
				c.Next()
				return
			}
		}
		// No api_key match — still allow, the token will be used as SSO directly.
		c.Next()
	}
}

// verifyAdminKey is the admin-key middleware.
// Accepts the key via Authorization: Bearer <key> OR ?app_key=<key> query.
func verifyAdminKey() gin.HandlerFunc {
	return func(c *gin.Context) {
		cfg := config.Global()
		key := strings.TrimSpace(cfg.GetStr("app.app_key", ""))
		if key == "" {
			writeAppError(c, platform.AuthError("Admin key is not configured."))
			c.Abort()
			return
		}
		token := extractBearer(c.GetHeader("Authorization"))
		if token == "" {
			token = strings.TrimSpace(c.Query("app_key"))
		}
		if token == "" {
			writeAppError(c, platform.AuthError("Missing authentication token."))
			c.Abort()
			return
		}
		if subtle.ConstantTimeCompare([]byte(token), []byte(key)) != 1 {
			writeAppError(c, platform.AuthError("Invalid authentication token."))
			c.Abort()
			return
		}
		c.Next()
	}
}
