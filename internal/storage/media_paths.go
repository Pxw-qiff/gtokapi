// Package storage implements local media file persistence with optional
// per-type cache capacity enforcement.
package storage

import (
	"os"
	"path/filepath"

	"github.com/aurora-develop/grok2api/internal/platform"
)

// MediaType is "image" or "video".
type MediaType string

const (
	MediaImage MediaType = "image"
	MediaVideo MediaType = "video"
)

// ImageFilesDir returns the local image storage directory, creating it if
// missing.
func ImageFilesDir() (string, error) {
	p := platform.DataPath("files", "images")
	if err := os.MkdirAll(p, 0o755); err != nil {
		return "", err
	}
	return p, nil
}

// VideoFilesDir returns the local video storage directory, creating it if
// missing.
func VideoFilesDir() (string, error) {
	p := platform.DataPath("files", "videos")
	if err := os.MkdirAll(p, 0o755); err != nil {
		return "", err
	}
	return p, nil
}

// LocalMediaCacheDBPath returns the SQLite index path for the media cache.
func LocalMediaCacheDBPath() (string, error) {
	p := platform.DataPath("cache")
	if err := os.MkdirAll(p, 0o755); err != nil {
		return "", err
	}
	return filepath.Join(p, "local_media_cache.db"), nil
}

// LocalMediaLockPath returns the advisory lock file used by one media-type
// cache operation.
func LocalMediaLockPath(mediaType MediaType) (string, error) {
	p := platform.DataPath("cache", "locks")
	if err := os.MkdirAll(p, 0o755); err != nil {
		return "", err
	}
	return filepath.Join(p, "local_media_"+string(mediaType)+".lock"), nil
}
