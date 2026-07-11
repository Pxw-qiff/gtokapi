package account

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/aurora-develop/grok2api/internal/platform"
)

// TxtRepository stores accounts in a local JSONL file (one JSON object per line).
// All data is kept in memory; writes flush the full file atomically.
type TxtRepository struct {
	mu       sync.Mutex
	path     string
	accounts map[string]*Record // token → record
	revision int
}

// NewTxtRepository opens (or creates) a JSONL file at path.
func NewTxtRepository(path string) *TxtRepository {
	return &TxtRepository{
		path:     path,
		accounts: make(map[string]*Record),
	}
}

func (r *TxtRepository) Initialize(ctx context.Context) error {
	dir := filepath.Dir(r.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	// Load existing file if present.
	if _, err := os.Stat(r.path); os.IsNotExist(err) {
		return r.flush()
	}
	return r.load()
}

func (r *TxtRepository) Close(ctx context.Context) error {
	return nil
}

func (r *TxtRepository) GetRevision(ctx context.Context) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.revision, nil
}

func (r *TxtRepository) RuntimeSnapshot(ctx context.Context) (*Snapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	items := make([]*Record, 0, len(r.accounts))
	for _, rec := range r.accounts {
		if !rec.IsDeleted() {
			items = append(items, rec)
		}
	}
	return &Snapshot{Revision: r.revision, Items: items}, nil
}

func (r *TxtRepository) ScanChanges(ctx context.Context, since int, limit int) (*ChangeSet, error) {
	if limit <= 0 {
		limit = 5000
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	var items []*Record
	batchMax := since
	for _, rec := range r.accounts {
		if rec.Revision > since {
			items = append(items, rec)
			if rec.Revision > batchMax {
				batchMax = rec.Revision
			}
		}
	}
	// Sort by revision for deterministic order.
	sort.Slice(items, func(i, j int) bool { return items[i].Revision < items[j].Revision })

	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}
	var deleted []string
	for _, it := range items {
		if it.IsDeleted() {
			deleted = append(deleted, it.Token)
		}
	}
	cs := &ChangeSet{Items: items, DeletedTokens: deleted, HasMore: hasMore, BatchMaxRev: batchMax}
	if hasMore {
		cs.Revision = batchMax
	} else {
		cs.Revision = r.revision
	}
	return cs, nil
}

func (r *TxtRepository) UpsertAccounts(ctx context.Context, items []Upsert) (*MutationResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.revision++
	rev := r.revision
	now := platform.NowMs()
	upserted := 0
	for _, it := range items {
		tok := platform.SanitizeToken(it.Token)
		if tok == "" {
			continue
		}
		pool := it.Pool
		if _, ok := PoolFromName(pool); !ok {
			pool = "basic"
		}
		tags := SortTags(it.Tags)
		ext := it.Ext
		if ext == nil {
			ext = map[string]any{}
		}
		qs := DefaultQuotaSet(pool)
		rec := &Record{
			Token:     tok,
			Pool:      pool,
			Status:    StatusActive,
			CreatedAt: now,
			UpdatedAt: now,
			Tags:      tags,
			Quota:     qs.ToMap(),
			Ext:       ext,
			Revision:  rev,
		}
		// Preserve CreatedAt if record already exists.
		if old, ok := r.accounts[tok]; ok && old != nil {
			rec.CreatedAt = old.CreatedAt
		}
		r.accounts[tok] = rec
		upserted++
	}
	if err := r.flush(); err != nil {
		return nil, err
	}
	return &MutationResult{Upserted: upserted, Revision: rev}, nil
}

func (r *TxtRepository) PatchAccounts(ctx context.Context, patches []Patch) (*MutationResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.revision++
	rev := r.revision
	now := platform.NowMs()
	patched := 0
	for _, p := range patches {
		tok := platform.SanitizeToken(p.Token)
		if tok == "" {
			continue
		}
		rec, ok := r.accounts[tok]
		if !ok || rec.IsDeleted() {
			continue
		}
		if p.Pool != nil {
			rec.Pool = *p.Pool
		}
		if p.Status != nil {
			rec.Status = *p.Status
		}
		if p.Tags != nil {
			rec.Tags = SortTags(p.Tags)
		}
		if len(p.AddTags) > 0 || len(p.RemoveTags) > 0 {
			rec.Tags = MergeTags(rec.Tags, p.AddTags, p.RemoveTags)
		}
		if p.QuotaAuto != nil {
			rec.Quota["auto"] = *p.QuotaAuto
		}
		if p.QuotaFast != nil {
			rec.Quota["fast"] = *p.QuotaFast
		}
		if p.QuotaExpert != nil {
			rec.Quota["expert"] = *p.QuotaExpert
		}
		if p.QuotaHeavy != nil {
			rec.Quota["heavy"] = *p.QuotaHeavy
		}
		if p.QuotaGrok43 != nil {
			rec.Quota["grok_4_3"] = *p.QuotaGrok43
		}
		if p.QuotaConsole != nil {
			rec.Quota["console"] = *p.QuotaConsole
		}
		if p.UsageUseDelta != nil {
			rec.UsageUseCount += *p.UsageUseDelta
			if rec.UsageUseCount < 0 {
				rec.UsageUseCount = 0
			}
		}
		if p.UsageFailDelta != nil {
			rec.UsageFailCount += *p.UsageFailDelta
			if rec.UsageFailCount < 0 {
				rec.UsageFailCount = 0
			}
		}
		if p.UsageSyncDelta != nil {
			rec.UsageSyncCount += *p.UsageSyncDelta
			if rec.UsageSyncCount < 0 {
				rec.UsageSyncCount = 0
			}
		}
		if p.LastUseAt != nil {
			rec.LastUseAt = p.LastUseAt
		}
		if p.LastFailAt != nil {
			rec.LastFailAt = p.LastFailAt
		}
		if p.LastFailReason != nil {
			rec.LastFailReason = p.LastFailReason
		}
		if p.LastSyncAt != nil {
			rec.LastSyncAt = p.LastSyncAt
		}
		if p.LastClearAt != nil {
			rec.LastClearAt = p.LastClearAt
		}
		if p.StateReason != nil {
			rec.StateReason = p.StateReason
		}
		if p.ClearFailures {
			rec.UsageFailCount = 0
			rec.LastFailAt = nil
			rec.LastFailReason = nil
			rec.StateReason = nil
			rec.Status = StatusActive
			deleteExtKeys(rec.Ext, "cooldown_until", "cooldown_reason",
				"disabled_at", "disabled_reason", "expired_at",
				"expired_reason", "forbidden_strikes")
		} else if len(p.ExtMerge) > 0 {
			for k, v := range p.ExtMerge {
				rec.Ext[k] = v
			}
		}
		rec.UpdatedAt = now
		rec.Revision = rev
		patched++
	}
	if err := r.flush(); err != nil {
		return nil, err
	}
	return &MutationResult{Patched: patched, Revision: rev}, nil
}

func (r *TxtRepository) DeleteAccounts(ctx context.Context, tokens []string) (*MutationResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.revision++
	rev := r.revision
	now := platform.NowMs()
	deleted := 0
	for _, raw := range tokens {
		tok := platform.SanitizeToken(raw)
		if tok == "" {
			continue
		}
		rec, ok := r.accounts[tok]
		if !ok || rec.IsDeleted() {
			continue
		}
		rec.DeletedAt = &now
		rec.UpdatedAt = now
		rec.Revision = rev
		deleted++
	}
	if err := r.flush(); err != nil {
		return nil, err
	}
	return &MutationResult{Deleted: deleted, Revision: rev}, nil
}

func (r *TxtRepository) GetAccounts(ctx context.Context, tokens []string) ([]*Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*Record, 0, len(tokens))
	for _, raw := range tokens {
		tok := platform.SanitizeToken(raw)
		if rec, ok := r.accounts[tok]; ok {
			out = append(out, rec)
		}
	}
	return out, nil
}

func (r *TxtRepository) ListAccounts(ctx context.Context, q ListQuery) (*Page, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if q.Page < 1 {
		q.Page = 1
	}
	if q.PageSize < 1 {
		q.PageSize = 50
	}
	if q.PageSize > 2000 {
		q.PageSize = 2000
	}

	// Collect and filter.
	items := make([]*Record, 0, len(r.accounts))
	for _, rec := range r.accounts {
		if !q.IncludeDeleted && rec.IsDeleted() {
			continue
		}
		if q.Pool != "" && rec.Pool != q.Pool {
			continue
		}
		if q.Status != nil && rec.Status != *q.Status {
			continue
		}
		if len(q.Tags) > 0 {
			hasAll := true
			for _, want := range q.Tags {
				found := false
				for _, have := range rec.Tags {
					if have == want {
						found = true
						break
					}
				}
				if !found {
					hasAll = false
					break
				}
			}
			if !hasAll {
				continue
			}
		}
		items = append(items, rec)
	}

	// Sort.
	sort.Slice(items, func(i, j int) bool {
		var less bool
		switch q.SortBy {
		case "created_at":
			less = items[i].CreatedAt < items[j].CreatedAt
		case "last_use_at":
			li, lj := int64(0), int64(0)
			if items[i].LastUseAt != nil {
				li = *items[i].LastUseAt
			}
			if items[j].LastUseAt != nil {
				lj = *items[j].LastUseAt
			}
			less = li < lj
		case "token":
			less = items[i].Token < items[j].Token
		case "usage_use_count":
			less = items[i].UsageUseCount < items[j].UsageUseCount
		case "usage_fail_count":
			less = items[i].UsageFailCount < items[j].UsageFailCount
		default:
			less = items[i].UpdatedAt < items[j].UpdatedAt
		}
		if q.SortDesc {
			return !less
		}
		return less
	})

	total := len(items)
	totalPages := 1
	if total > 0 {
		totalPages = (total + q.PageSize - 1) / q.PageSize
	}
	offset := (q.Page - 1) * q.PageSize
	if offset > total {
		offset = total
	}
	end := offset + q.PageSize
	if end > total {
		end = total
	}
	pageItems := items[offset:end]

	// Shallow copy records so callers can't mutate our state.
	out := make([]*Record, len(pageItems))
	for i, rec := range pageItems {
		cp := *rec
		out[i] = &cp
	}
	return &Page{Items: out, Total: total, Page: q.Page, PageSize: q.PageSize, TotalPages: totalPages, Revision: r.revision}, nil
}

func (r *TxtRepository) ReplacePool(ctx context.Context, pool string, upserts []Upsert) (*MutationResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.revision++
	rev := r.revision
	now := platform.NowMs()
	// Soft-delete all existing records in this pool.
	for _, rec := range r.accounts {
		if rec.Pool == pool && !rec.IsDeleted() {
			rec.DeletedAt = &now
			rec.UpdatedAt = now
			rec.Revision = rev
		}
	}
	// Upsert new records.
	for _, it := range upserts {
		tok := platform.SanitizeToken(it.Token)
		if tok == "" {
			continue
		}
		tags := SortTags(it.Tags)
		ext := it.Ext
		if ext == nil {
			ext = map[string]any{}
		}
		qs := DefaultQuotaSet(pool)
		rec := &Record{
			Token:     tok,
			Pool:      pool,
			Status:    StatusActive,
			CreatedAt: now,
			UpdatedAt: now,
			Tags:      tags,
			Quota:     qs.ToMap(),
			Ext:       ext,
			Revision:  rev,
		}
		if old, ok := r.accounts[tok]; ok && old != nil {
			rec.CreatedAt = old.CreatedAt
		}
		r.accounts[tok] = rec
	}
	if err := r.flush(); err != nil {
		return nil, err
	}
	return &MutationResult{Upserted: len(upserts), Revision: rev}, nil
}

func (r *TxtRepository) ResetExpiredConsoleWindows(ctx context.Context) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.revision++
	rev := r.revision
	now := platform.NowMs()
	def := DefaultQuotaWindow("basic", 5)
	defMap := def.ToMap()
	reset := 0
	for _, rec := range r.accounts {
		if rec.IsDeleted() || rec.Status != StatusActive {
			continue
		}
		qs := rec.QuotaSet()
		w := qs.Console
		if w == nil {
			continue
		}
		if w.Remaining <= 0 || w.ResetAt == nil || *w.ResetAt <= now {
			rec.Quota["console"] = defMap
			rec.LastSyncAt = &now
			rec.UpdatedAt = now
			rec.Revision = rev
			reset++
		}
	}
	if reset > 0 {
		if err := r.flush(); err != nil {
			return 0, err
		}
	}
	return reset, nil
}

func (r *TxtRepository) RecoverConsoleExpiredAccounts(ctx context.Context) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.revision++
	rev := r.revision
	now := platform.NowMs()
	threshold := now - 3600000
	recovered := 0
	for _, rec := range r.accounts {
		if rec.IsDeleted() || rec.Status != StatusExpired {
			continue
		}
		if rec.StateReason == nil || *rec.StateReason != "console_429_threshold_exceeded" {
			continue
		}
		if rec.UsageUseCount <= 5 {
			continue
		}
		expiredAt := int64(0)
		if v, ok := rec.Ext["expired_at"].(float64); ok {
			expiredAt = int64(v)
		}
		if expiredAt > threshold {
			continue
		}
		rec.Status = StatusActive
		rec.StateReason = nil
		deleteExtKeys(rec.Ext, "expired_at", "expired_reason",
			"console_429_count", "console_429_last_at")
		rec.UpdatedAt = now
		rec.Revision = rev
		recovered++
	}
	if recovered > 0 {
		if err := r.flush(); err != nil {
			return 0, err
		}
	}
	return recovered, nil
}

// --- internal helpers ---

// load reads the JSONL file into the in-memory map.
func (r *TxtRepository) load() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	f, err := os.Open(r.path)
	if err != nil {
		return err
	}
	defer f.Close()

	maxRev := 0
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var rec Record
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			continue // skip malformed lines
		}
		if rec.Tags == nil {
			rec.Tags = []string{}
		}
		if rec.Ext == nil {
			rec.Ext = map[string]any{}
		}
		if rec.Quota == nil {
			rec.Quota = map[string]any{}
		}
		r.accounts[rec.Token] = &rec
		if rec.Revision > maxRev {
			maxRev = rec.Revision
		}
	}
	r.revision = maxRev
	return scanner.Err()
}

// flush writes the full in-memory state to the JSONL file atomically.
func (r *TxtRepository) flush() error {
	tmp := r.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, rec := range r.accounts {
		if err := enc.Encode(rec); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, r.path)
}

// deleteExtKeys removes keys from an ext map.
func deleteExtKeys(ext map[string]any, keys ...string) {
	for _, k := range keys {
		delete(ext, k)
	}
}
