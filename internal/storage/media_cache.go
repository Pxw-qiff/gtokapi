package storage

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/google/uuid"

	"github.com/aurora-develop/grok2api/internal/config"
	"github.com/aurora-develop/grok2api/internal/logger"
)

// lowWatermarkRatio is the cache target after an eviction sweep (60% of max).
const lowWatermarkRatio = 0.60

const table = "local_media_files"

var (
	imageExts = map[string]struct{}{".jpg": {}, ".jpeg": {}, ".png": {}, ".gif": {}, ".webp": {}, ".bmp": {}}
	videoExts = map[string]struct{}{".mp4": {}, ".mov": {}, ".m4v": {}, ".webm": {}, ".avi": {}, ".mkv": {}}
)

// LocalMediaCacheStore manages local media files and enforces per-type
// cache limits. The SQLite index tracks file sizes and creation timestamps
// so eviction can target the low-watermark target.
type LocalMediaCacheStore struct {
	mu             sync.Mutex
	videoMu        sync.Mutex
	initMu         sync.Mutex
	initializedDBs map[string]struct{}
}

// NewLocalMediaCacheStore returns an empty store.
func NewLocalMediaCacheStore() *LocalMediaCacheStore {
	return &LocalMediaCacheStore{initializedDBs: map[string]struct{}{}}
}

// SaveImage persists an image and returns the stable local file ID.
// The extension is derived from the MIME type (.png for PNG, .jpg otherwise).
func (s *LocalMediaCacheStore) SaveImage(raw []byte, mime, fileID string) (string, error) {
	ext := ".jpg"
	if strings.Contains(strings.ToLower(mime), "png") {
		ext = ".png"
	}
	if _, err := s.save(MediaImage, fileID, raw, ext); err != nil {
		return "", err
	}
	return fileID, nil
}

// SaveVideo persists a video file and returns the final file path.
func (s *LocalMediaCacheStore) SaveVideo(raw []byte, fileID string) (string, error) {
	return s.save(MediaVideo, fileID, raw, ".mp4")
}

// Rebuild rebuilds the on-disk index for one media type and enforces limits.
func (s *LocalMediaCacheStore) Rebuild(mediaType MediaType) error {
	maxBytes := s.limitBytes(mediaType)
	if maxBytes <= 0 {
		return nil
	}
	release, err := s.guard(mediaType)
	if err != nil {
		return err
	}
	defer release()
	db, err := s.connect()
	if err != nil {
		return err
	}
	defer db.Close()
	dir, err := s.mediaDir(mediaType)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	allowed := allowedExts(mediaType)
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec("DELETE FROM "+table+" WHERE media_type = ?", string(mediaType)); err != nil {
		return err
	}
	nowNs := time.Now().UnixNano()
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(entry.Name()))
		if _, ok := allowed[ext]; !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if _, err := tx.Exec(
			"INSERT INTO "+table+" (media_type, name, size_bytes, created_at_ns, updated_at_ns) VALUES (?, ?, ?, ?, ?)",
			string(mediaType), entry.Name(), info.Size(), nowNs, info.ModTime().UnixNano(),
		); err != nil {
			return err
		}
	}
	if err := s.enforceLimitLocked(tx, mediaType, nil); err != nil {
		return err
	}
	return tx.Commit()
}

// Delete removes one local media file by name and updates the index.
func (s *LocalMediaCacheStore) Delete(mediaType MediaType, name string) (bool, error) {
	safe, err := s.validateName(mediaType, name)
	if err != nil {
		return false, err
	}
	dir, err := s.mediaDir(mediaType)
	if err != nil {
		return false, err
	}
	path := filepath.Join(dir, safe)
	release, err := s.guard(mediaType)
	if err != nil {
		return false, err
	}
	defer release()
	existed := fileExists(path)
	if existed {
		_ = os.Remove(path)
	}
	if err := s.deleteIndexRowIfPresent(mediaType, safe); err != nil {
		return existed, err
	}
	return existed, nil
}

// Clear removes all tracked files for one media type.
func (s *LocalMediaCacheStore) Clear(mediaType MediaType) (int, error) {
	dir, err := s.mediaDir(mediaType)
	if err != nil {
		return 0, err
	}
	allowed := allowedExts(mediaType)
	release, err := s.guard(mediaType)
	if err != nil {
		return 0, err
	}
	defer release()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	removed := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(entry.Name()))
		if _, ok := allowed[ext]; !ok {
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err == nil {
			removed++
		}
	}
	if err := s.deleteIndexRowsIfPresent(mediaType); err != nil {
		return removed, err
	}
	return removed, nil
}

// --- internal helpers ---

func (s *LocalMediaCacheStore) save(mediaType MediaType, fileID string, raw []byte, ext string) (string, error) {
	dir, err := s.mediaDir(mediaType)
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, fileID+ext)
	if s.limitBytes(mediaType) <= 0 {
		if err := writeIfMissing(path, raw); err != nil {
			return "", err
		}
		return path, nil
	}
	release, err := s.guard(mediaType)
	if err != nil {
		return "", err
	}
	defer release()
	db, err := s.connect()
	if err != nil {
		return "", err
	}
	defer db.Close()
	if fileExists(path) {
		if err := s.upsertExistingRow(db, mediaType, path); err != nil {
			return "", err
		}
	} else {
		if err := atomicWrite(path, raw); err != nil {
			return "", err
		}
		if err := s.upsertNewRow(db, mediaType, path); err != nil {
			return "", err
		}
	}
	protected := map[string]struct{}{filepath.Base(path): {}}
	if err := s.enforceLimitLockedDB(db, mediaType, protected); err != nil {
		return "", err
	}
	return path, nil
}

func (s *LocalMediaCacheStore) limitBytes(mediaType MediaType) int64 {
	cfg := config.Global()
	limitMB := int64(cfg.GetInt("cache.local."+string(mediaType)+"_max_mb", 0))
	if limitMB < 0 {
		limitMB = 0
	}
	return limitMB * 1024 * 1024
}

func (s *LocalMediaCacheStore) targetBytes(maxBytes int64) int64 {
	if maxBytes <= 0 {
		return 0
	}
	return int64(float64(maxBytes) * lowWatermarkRatio)
}

func (s *LocalMediaCacheStore) mediaDir(mediaType MediaType) (string, error) {
	if mediaType == MediaImage {
		return ImageFilesDir()
	}
	return VideoFilesDir()
}

func allowedExts(mediaType MediaType) map[string]struct{} {
	if mediaType == MediaImage {
		return imageExts
	}
	return videoExts
}

func (s *LocalMediaCacheStore) validateName(mediaType MediaType, name string) (string, error) {
	value := strings.TrimSpace(name)
	if value == "" {
		return "", fmt.Errorf("missing file name")
	}
	if filepath.Base(value) != value {
		return "", fmt.Errorf("invalid file name")
	}
	ext := strings.ToLower(filepath.Ext(value))
	allowed := allowedExts(mediaType)
	if _, ok := allowed[ext]; !ok {
		return "", fmt.Errorf("unsupported file type")
	}
	return value, nil
}

func writeIfMissing(path string, raw []byte) error {
	if fileExists(path) {
		return nil
	}
	return atomicWrite(path, raw)
}

func atomicWrite(path string, raw []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if fileExists(path) {
		return nil
	}
	tmp := filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+"."+uuid.NewString()+".part")
	defer os.Remove(tmp)
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	if fileExists(path) {
		return nil
	}
	return os.Rename(tmp, path)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// guard acquires the per-media-type thread mutex (no flock — this is a
// single-process Go binary; cross-process locking is unnecessary).
func (s *LocalMediaCacheStore) guard(mediaType MediaType) (func(), error) {
	mu := &s.mu
	if mediaType == MediaVideo {
		mu = &s.videoMu
	}
	mu.Lock()
	return func() { mu.Unlock() }, nil
}

func (s *LocalMediaCacheStore) connect() (*sql.DB, error) {
	dbPath, err := LocalMediaCacheDBPath()
	if err != nil {
		return nil, err
	}
	dsn := "file:" + dbPath + "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := s.ensureSchema(db, dbPath); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func (s *LocalMediaCacheStore) ensureSchema(db *sql.DB, dbPath string) error {
	s.initMu.Lock()
	defer s.initMu.Unlock()
	if _, ok := s.initializedDBs[dbPath]; ok {
		return nil
	}
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS ` + table + ` (
    media_type    TEXT    NOT NULL,
    name          TEXT    NOT NULL,
    size_bytes    INTEGER NOT NULL,
    created_at_ns INTEGER NOT NULL,
    updated_at_ns INTEGER NOT NULL,
    PRIMARY KEY (media_type, name)
);
CREATE INDEX IF NOT EXISTS idx_local_media_order
    ON ` + table + ` (media_type, created_at_ns, name);
`)
	if err != nil {
		return err
	}
	s.initializedDBs[dbPath] = struct{}{}
	return nil
}

func (s *LocalMediaCacheStore) upsertExistingRow(db *sql.DB, mediaType MediaType, path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return nil
	}
	createdAt, err := s.lookupCreatedAtNS(db, mediaType, filepath.Base(path), info.ModTime().UnixNano())
	if err != nil {
		return err
	}
	_, err = db.Exec(`
INSERT INTO `+table+` (media_type, name, size_bytes, created_at_ns, updated_at_ns)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(media_type, name) DO UPDATE SET
    size_bytes = excluded.size_bytes,
    updated_at_ns = excluded.updated_at_ns`,
		string(mediaType), filepath.Base(path), info.Size(), createdAt, info.ModTime().UnixNano(),
	)
	return err
}

func (s *LocalMediaCacheStore) upsertNewRow(db *sql.DB, mediaType MediaType, path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	nowNs := time.Now().UnixNano()
	_, err = db.Exec(`
INSERT INTO `+table+` (media_type, name, size_bytes, created_at_ns, updated_at_ns)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(media_type, name) DO UPDATE SET
    size_bytes = excluded.size_bytes,
    created_at_ns = excluded.created_at_ns,
    updated_at_ns = excluded.updated_at_ns`,
		string(mediaType), filepath.Base(path), info.Size(), nowNs, nowNs,
	)
	return err
}

func (s *LocalMediaCacheStore) lookupCreatedAtNS(db *sql.DB, mediaType MediaType, name string, fallback int64) (int64, error) {
	row := db.QueryRow(`SELECT created_at_ns FROM `+table+` WHERE media_type = ? AND name = ?`,
		string(mediaType), name)
	var v int64
	if err := row.Scan(&v); err != nil {
		if err == sql.ErrNoRows {
			return fallback, nil
		}
		return 0, err
	}
	return v, nil
}

func (s *LocalMediaCacheStore) enforceLimitLocked(tx *sql.Tx, mediaType MediaType, protected map[string]struct{}) error {
	db := tx
	return s.enforceLimitLockedDB(db, mediaType, protected)
}

// enforceLimitLockedDB trims the cache down to the low-watermark target by
// evicting the oldest files. The newest file is always protected.
func (s *LocalMediaCacheStore) enforceLimitLockedDB(db dbExec, mediaType MediaType, protected map[string]struct{}) error {
	maxBytes := s.limitBytes(mediaType)
	if maxBytes <= 0 {
		return nil
	}
	usage, err := s.usageBytes(db, mediaType)
	if err != nil {
		return err
	}
	if usage <= maxBytes {
		return nil
	}
	if protected == nil {
		protected = map[string]struct{}{}
	}
	newest, err := s.newestName(db, mediaType)
	if err != nil {
		return err
	}
	if newest != "" {
		protected[newest] = struct{}{}
	}
	target := s.targetBytes(maxBytes)
	dir, err := s.mediaDir(mediaType)
	if err != nil {
		return err
	}
	rows, err := db.Query(`SELECT name, size_bytes FROM `+table+` WHERE media_type = ? ORDER BY created_at_ns ASC, name ASC`,
		string(mediaType))
	if err != nil {
		return err
	}
	defer rows.Close()
	removed := 0
	for rows.Next() {
		if usage <= target {
			break
		}
		var name string
		var sizeBytes int64
		if err := rows.Scan(&name, &sizeBytes); err != nil {
			return err
		}
		if _, ok := protected[name]; ok {
			continue
		}
		path := filepath.Join(dir, name)
		if err := os.Remove(path); err != nil {
			if !os.IsNotExist(err) {
				logger.Warnf("local media cache delete failed: media_type=%s name=%s error=%v", mediaType, name, err)
			}
			continue
		}
		if _, err := db.Exec(`DELETE FROM `+table+` WHERE media_type = ? AND name = ?`,
			string(mediaType), name); err != nil {
			return err
		}
		usage -= sizeBytes
		if usage < 0 {
			usage = 0
		}
		removed++
	}
	if removed > 0 {
		logger.Infof("local media cache trimmed: media_type=%s removed=%d usage_bytes=%d limit_bytes=%d target_bytes=%d",
			mediaType, removed, usage, maxBytes, target)
	}
	return nil
}

func (s *LocalMediaCacheStore) usageBytes(db dbExec, mediaType MediaType) (int64, error) {
	row := db.QueryRow(`SELECT COALESCE(SUM(size_bytes), 0) AS total FROM `+table+` WHERE media_type = ?`,
		string(mediaType))
	var v int64
	if err := row.Scan(&v); err != nil {
		return 0, err
	}
	return v, nil
}

func (s *LocalMediaCacheStore) newestName(db dbExec, mediaType MediaType) (string, error) {
	row := db.QueryRow(`SELECT name FROM `+table+` WHERE media_type = ? ORDER BY created_at_ns DESC, name DESC LIMIT 1`,
		string(mediaType))
	var name string
	if err := row.Scan(&name); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}
	return name, nil
}

func (s *LocalMediaCacheStore) deleteIndexRowIfPresent(mediaType MediaType, name string) error {
	dbPath, err := LocalMediaCacheDBPath()
	if err != nil {
		return err
	}
	if !fileExists(dbPath) {
		return nil
	}
	db, err := s.connect()
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec(`DELETE FROM `+table+` WHERE media_type = ? AND name = ?`,
		string(mediaType), name)
	return err
}

func (s *LocalMediaCacheStore) deleteIndexRowsIfPresent(mediaType MediaType) error {
	dbPath, err := LocalMediaCacheDBPath()
	if err != nil {
		return err
	}
	if !fileExists(dbPath) {
		return nil
	}
	db, err := s.connect()
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec(`DELETE FROM `+table+` WHERE media_type = ?`, string(mediaType))
	return err
}

// dbExec is the minimal subset of *sql.DB / *sql.Tx used by the eviction
// logic. Both types satisfy this interface.
type dbExec interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}
