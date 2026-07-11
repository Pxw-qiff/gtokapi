// Package platform collects cross-cutting platform helpers: paths, errors,
// meta, and tokens.
package platform

import (
	"os"
	"path/filepath"
	"strings"
)

// DataDir returns the configured data root (env DATA_DIR, default ./data).
func DataDir() string {
	d := os.Getenv("DATA_DIR")
	if d == "" {
		d = "./data"
	}
	return d
}

// DataPath joins a relative path under the data directory.
func DataPath(parts ...string) string {
	return filepath.Join(append([]string{DataDir()}, parts...)...)
}

// LogDir returns the configured log directory (env LOG_DIR, default ./logs).
func LogDir() string {
	d := os.Getenv("LOG_DIR")
	if d == "" {
		d = "./logs"
	}
	return d
}

// ProjectRoot returns the directory of the running binary's working dir.
func ProjectRoot() string {
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}
	return wd
}

// SanitizeToken normalizes an SSO token the same way the Python project does:
// translate Unicode dash/space/zero-width variants to ASCII, strip all
// whitespace, strip a leading "sso=" prefix, keep ASCII-only.
func SanitizeToken(raw string) string {
	if raw == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range raw {
		switch r {
		case ' ', '\t', '\n', '\r', '\u00a0', '\u200b', '\u200c', '\u200d',
			'\u2060', '\ufeff', '\u3000':
			continue
		case '‐', '‑', '‒', '–', '—', '―', '－':
			b.WriteByte('-')
		case '“', '”', '‘', '’':
			b.WriteByte('"')
		default:
			if r < 128 {
				b.WriteRune(r)
			}
		}
	}
	s := b.String()
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "sso=")
	return s
}

// SanitizeProxyValue strips Unicode dash/space/zero-width variants from a
// proxy header value (user_agent / cf_cookies). For cf_cookies, all spaces
// are stripped too.
func SanitizeProxyValue(v string, stripAllSpaces bool) string {
	if v == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range v {
		switch r {
		case '‐', '‑', '‒', '–', '—', '―', '－':
			b.WriteByte('-')
		case '“', '”', '‘', '’':
			b.WriteByte('"')
		case '\u00a0', '\u200b', '\u200c', '\u200d', '\u2060', '\ufeff', '\u3000':
			continue
		case ' ', '\t':
			if stripAllSpaces {
				continue
			}
			b.WriteByte(' ')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// NowMs returns the current time in milliseconds since epoch.
func NowMs() int64 {
	return timeNow().UnixMilli()
}
