// Package config implements TOML configuration loading with environment
// variable overrides, mirroring the Python project's loader.
//
// Precedence: defaults TOML -> user config.toml -> GROK_ env overrides.
//
// Env overrides use the form GROK_SECTION_KEY=value which maps to the
// dotted key section.key (split on the FIRST underscore). Env can only set
// two-level (section.key) entries, matching the upstream behaviour.
package config

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/pelletier/go-toml/v2"
)

// Snapshot is the process-global configuration store. It lazily reloads when
// the underlying user config file changes (detected by mtime).
type Snapshot struct {
	mu             sync.RWMutex
	data           map[string]any
	defaultsPath   string
	userPath       string
	defaultsMtime  float64
	userMtime      float64
}

var global = &Snapshot{}

// Global returns the process-global snapshot.
func Global() *Snapshot { return global }

// SetPaths configures the defaults and user config file paths.
func SetPaths(defaultsPath, userPath string) {
	global.mu.Lock()
	defer global.mu.Unlock()
	global.defaultsPath = defaultsPath
	global.userPath = userPath
}

// DefaultsPath returns the resolved defaults TOML path.
func DefaultsPath() string {
	return global.defaultsPath
}

// UserPath returns the resolved user config.toml path.
func UserPath() string { return global.userPath }

// Load (re)loads the configuration if the underlying files changed.
func Load() error {
	return global.Load()
}

// Load reloads the configuration if the underlying files changed.
func (s *Snapshot) Load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dMtime := fileMtime(s.defaultsPath)
	uMtime := fileMtime(s.userPath)
	if s.data != nil && dMtime == s.defaultsMtime && uMtime == s.userMtime {
		return nil
	}

	data, err := loadTOML(s.defaultsPath)
	if err != nil {
		return err
	}
	user, err := loadTOML(s.userPath)
	if err != nil {
		return err
	}
	data = deepMerge(data, user)
	applyEnv(data, "GROK_")

	s.data = data
	s.defaultsMtime = dMtime
	s.userMtime = uMtime
	return nil
}

// Raw returns a deep copy of the current nested config data.
func (s *Snapshot) Raw() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return deepCopy(s.data)
}

// Get walks the nested config using a dotted key and returns the value or def.
func (s *Snapshot) Get(key string, def any) any {
	s.mu.RLock()
	v := getNestedLocked(s.data, key)
	s.mu.RUnlock()
	if v == nil {
		return def
	}
	return v
}

// GetStr returns a string value (stringifying if needed).
func (s *Snapshot) GetStr(key, def string) string {
	v := s.Get(key, nil)
	if v == nil {
		return def
	}
	switch t := v.(type) {
	case string:
		return t
	case int64:
		return strconv.FormatInt(t, 10)
	case int:
		return strconv.Itoa(t)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	default:
		return def
	}
}

// GetInt returns an integer value.
func (s *Snapshot) GetInt(key string, def int) int {
	v := s.Get(key, nil)
	switch t := v.(type) {
	case int64:
		return int(t)
	case int:
		return t
	case float64:
		return int(t)
	case string:
		if n, err := strconv.Atoi(strings.TrimSpace(t)); err == nil {
			return n
		}
		return def
	default:
		return def
	}
}

// GetBool returns a boolean value. Accepts 1/true/yes/on (case-insensitive).
func (s *Snapshot) GetBool(key string, def bool) bool {
	v := s.Get(key, nil)
	switch t := v.(type) {
	case bool:
		return t
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
		return def
	case int64:
		return t != 0
	case int:
		return t != 0
	default:
		return def
	}
}

// GetList returns a list. Strings are CSV-split; TOML arrays pass through.
func (s *Snapshot) GetList(key string, def []string) []string {
	v := s.Get(key, nil)
	switch t := v.(type) {
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			out = append(out, toStr(item))
		}
		return out
	case string:
		parts := strings.Split(t, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				out = append(out, p)
			}
		}
		return out
	default:
		return def
	}
}

// Update deep-merges a patch into the user config file and persists it.
func (s *Snapshot) Update(patch map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.userPath == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.userPath), 0o755); err != nil {
		return err
	}
	existing, err := loadTOML(s.userPath)
	if err != nil {
		return err
	}
	merged := deepMerge(existing, patch)
	f, err := os.Create(s.userPath)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := toml.NewEncoder(f)
	if err := enc.Encode(merged); err != nil {
		return err
	}
	// Force reload on next access.
	s.defaultsMtime = -1
	return nil
}

// --- helpers ---

func loadTOML(path string) (map[string]any, error) {
	if path == "" {
		return map[string]any{}, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]any{}, nil
		}
		return nil, err
	}
	var out map[string]any
	if err := toml.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	if out == nil {
		out = map[string]any{}
	}
	return out, nil
}

func deepMerge(base, override map[string]any) map[string]any {
	result := make(map[string]any, len(base))
	for k, v := range base {
		result[k] = v
	}
	for k, v := range override {
		if existing, ok := result[k]; ok {
			if em, ok1 := existing.(map[string]any); ok1 {
				if vm, ok2 := v.(map[string]any); ok2 {
					result[k] = deepMerge(em, vm)
					continue
				}
			}
		}
		result[k] = v
	}
	return result
}

func deepCopy(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		if vm, ok := v.(map[string]any); ok {
			out[k] = deepCopy(vm)
		} else {
			out[k] = v
		}
	}
	return out
}

func applyEnv(data map[string]any, prefix string) {
	const prefixLen = len("GROK_")
	for _, env := range os.Environ() {
		eq := strings.IndexByte(env, '=')
		if eq < 0 || !strings.HasPrefix(env[:eq], prefix) {
			continue
		}
		rest := env[:eq][prefixLen:]
		key := strings.ToLower(rest)
		idx := strings.IndexByte(key, '_')
		if idx < 0 {
			continue
		}
		section := key[:idx]
		sub := key[idx+1:]
		sec, _ := data[section].(map[string]any)
		if sec == nil {
			sec = map[string]any{}
		}
		sec[sub] = env[eq+1:]
		data[section] = sec
	}
}

func getNestedLocked(data map[string]any, dotted string) any {
	if dotted == "" {
		return data
	}
	parts := strings.Split(dotted, ".")
	var node any = data
	for _, p := range parts {
		m, ok := node.(map[string]any)
		if !ok {
			return nil
		}
		node = m[p]
		if node == nil {
			return nil
		}
	}
	return node
}

func fileMtime(path string) float64 {
	if path == "" {
		return 0
	}
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return float64(fi.ModTime().UnixNano())
}

func toStr(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	default:
		return ""
	}
}

// IsStartupOnlyConfigKey returns true when a dotted key path is reserved for
// startup-time configuration (storage backends) and cannot be changed at runtime.
func IsStartupOnlyConfigKey(dotted string) bool {
	for _, p := range startupOnlyPrefixes {
		if strings.HasPrefix(dotted, p) {
			return true
		}
	}
	return false
}

var startupOnlyPrefixes = []string{
	"account.storage",
	"account.local",
	"account.redis",
	"account.mysql",
	"account.postgresql",
}
