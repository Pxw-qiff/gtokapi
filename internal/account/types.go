// Package account implements the multi-account pool: the persistent record
// model, quota windows, the state machine, the storage repository and the
// in-memory runtime directory used for fast account selection.
package account

import (
	"sort"

	"github.com/aurora-develop/grok2api/internal/platform"
)

// Status is the persistent lifecycle status of an account.
type Status string

const (
	StatusActive   Status = "active"
	StatusCooling  Status = "cooling"
	StatusExpired  Status = "expired"
	StatusDisabled Status = "disabled"
)

// StatusID is the dataplane integer encoding of a status.
type StatusID int

const (
	StatusIDActive   StatusID = 0
	StatusIDCooling  StatusID = 1
	StatusIDExpired  StatusID = 2
	StatusIDDisabled StatusID = 3
	StatusIDDeleted  StatusID = 4
)

func (s Status) ID() StatusID {
	switch s {
	case StatusActive:
		return StatusIDActive
	case StatusCooling:
		return StatusIDCooling
	case StatusExpired:
		return StatusIDExpired
	case StatusDisabled:
		return StatusIDDisabled
	}
	return StatusIDActive
}

func (s StatusID) String() string {
	switch s {
	case StatusIDActive:
		return "active"
	case StatusIDCooling:
		return "cooling"
	case StatusIDExpired:
		return "expired"
	case StatusIDDisabled:
		return "disabled"
	case StatusIDDeleted:
		return "deleted"
	}
	return "active"
}

// PoolID is the integer pool tier (0=basic, 1=super, 2=heavy).
type PoolID int

const (
	PoolBasic PoolID = 0
	PoolSuper PoolID = 1
	PoolHeavy PoolID = 2
)

func PoolFromName(name string) (PoolID, bool) {
	switch name {
	case "basic", "":
		return PoolBasic, true
	case "super":
		return PoolSuper, true
	case "heavy":
		return PoolHeavy, true
	}
	return PoolBasic, false
}

func (p PoolID) Name() string {
	switch p {
	case PoolSuper:
		return "super"
	case PoolHeavy:
		return "heavy"
	}
	return "basic"
}

// FeedbackKind is the outcome of one upstream request.
type FeedbackKind string

const (
	FbSuccess      FeedbackKind = "success"
	FbUnauthorized FeedbackKind = "unauthorized"
	FbForbidden    FeedbackKind = "forbidden"
	FbRateLimited  FeedbackKind = "rate_limited"
	FbServerError  FeedbackKind = "server_error"
	FbDisable      FeedbackKind = "disable"
	FbDelete       FeedbackKind = "delete"
	FbRestore      FeedbackKind = "restore"
)

// FeedbackKindFromStatus maps an HTTP status code to a feedback kind.
func FeedbackKindFromStatus(status int) FeedbackKind {
	switch {
	case status == 401:
		return FbUnauthorized
	case status == 403:
		return FbForbidden
	case status == 429:
		return FbRateLimited
	case status >= 500:
		return FbServerError
	case status >= 200 && status < 300:
		return FbSuccess
	}
	return FbServerError
}

// QuotaSource tracks how reliable a quota window value is.
type QuotaSource int

const (
	SourceDefault   QuotaSource = 0
	SourceReal      QuotaSource = 1
	SourceEstimated QuotaSource = 2
)

// QuotaWindow is the per-mode rate-limit window.
type QuotaWindow struct {
	Remaining     int         `json:"remaining"`
	Total         int         `json:"total"`
	WindowSeconds int         `json:"window_seconds"`
	ResetAt       *int64      `json:"reset_at"`
	SyncedAt      *int64      `json:"synced_at"`
	Source        QuotaSource `json:"source"`
}

func (w QuotaWindow) IsExhausted() bool { return w.Remaining <= 0 }

func (w QuotaWindow) IsWindowExpired(nowMs int64) bool {
	if w.ResetAt == nil {
		return false
	}
	return nowMs >= *w.ResetAt
}

// ToMap / FromMap helpers for JSON serialization in the ext/quota columns.
func (w QuotaWindow) ToMap() map[string]any {
	m := map[string]any{
		"remaining":      w.Remaining,
		"total":          w.Total,
		"window_seconds": w.WindowSeconds,
		"source":         int(w.Source),
	}
	if w.ResetAt != nil {
		m["reset_at"] = *w.ResetAt
	}
	if w.SyncedAt != nil {
		m["synced_at"] = *w.SyncedAt
	}
	return m
}

func QuotaWindowFromMap(m map[string]any) QuotaWindow {
	var w QuotaWindow
	if v, ok := m["remaining"]; ok {
		w.Remaining = toInt(v)
	}
	if v, ok := m["total"]; ok {
		w.Total = toInt(v)
	}
	if v, ok := m["window_seconds"]; ok {
		w.WindowSeconds = toInt(v)
	}
	if v, ok := m["source"]; ok {
		w.Source = QuotaSource(toInt(v))
	}
	if v, ok := m["reset_at"]; ok {
		n := toInt64(v)
		w.ResetAt = &n
	}
	if v, ok := m["synced_at"]; ok {
		n := toInt64(v)
		w.SyncedAt = &n
	}
	return w
}

// QuotaSet holds one window per mode (0..5).
type QuotaSet struct {
	Auto    QuotaWindow
	Fast    QuotaWindow
	Expert  QuotaWindow
	Heavy   *QuotaWindow
	Grok43  *QuotaWindow
	Console *QuotaWindow
}

func (q *QuotaSet) Get(modeID int) *QuotaWindow {
	switch modeID {
	case 0:
		return &q.Auto
	case 1:
		return &q.Fast
	case 2:
		return &q.Expert
	case 3:
		return q.Heavy
	case 4:
		return q.Grok43
	case 5:
		return q.Console
	}
	return nil
}

func (q *QuotaSet) Set(modeID int, w QuotaWindow) {
	switch modeID {
	case 0:
		q.Auto = w
	case 1:
		q.Fast = w
	case 2:
		q.Expert = w
	case 3:
		cw := w
		q.Heavy = &cw
	case 4:
		cw := w
		q.Grok43 = &cw
	case 5:
		cw := w
		q.Console = &cw
	}
}

// ToMap serializes the quota set (only non-empty windows for heavy/grok43/console).
func (q QuotaSet) ToMap() map[string]any {
	m := map[string]any{
		"auto":   q.Auto.ToMap(),
		"fast":   q.Fast.ToMap(),
		"expert": q.Expert.ToMap(),
	}
	if q.Heavy != nil {
		m["heavy"] = q.Heavy.ToMap()
	}
	if q.Grok43 != nil {
		m["grok_4_3"] = q.Grok43.ToMap()
	}
	if q.Console != nil {
		m["console"] = q.Console.ToMap()
	}
	return m
}

func QuotaSetFromMap(m map[string]any) QuotaSet {
	var q QuotaSet
	if v, ok := m["auto"].(map[string]any); ok {
		q.Auto = QuotaWindowFromMap(v)
	}
	if v, ok := m["fast"].(map[string]any); ok {
		q.Fast = QuotaWindowFromMap(v)
	}
	if v, ok := m["expert"].(map[string]any); ok {
		q.Expert = QuotaWindowFromMap(v)
	}
	if v, ok := m["heavy"].(map[string]any); ok {
		w := QuotaWindowFromMap(v)
		q.Heavy = &w
	}
	if v, ok := m["grok_4_3"].(map[string]any); ok {
		w := QuotaWindowFromMap(v)
		q.Grok43 = &w
	}
	if v, ok := m["console"].(map[string]any); ok {
		w := QuotaWindowFromMap(v)
		q.Console = &w
	}
	return q
}

// Record is the single source of truth for an account across all backends.
type Record struct {
	Token          string         `json:"token"`
	Pool           string         `json:"pool"`
	Status         Status         `json:"status"`
	CreatedAt      int64          `json:"created_at"`
	UpdatedAt      int64          `json:"updated_at"`
	Tags           []string       `json:"tags"`
	Quota          map[string]any `json:"quota"`
	UsageUseCount  int            `json:"usage_use_count"`
	UsageFailCount int            `json:"usage_fail_count"`
	UsageSyncCount int            `json:"usage_sync_count"`
	LastUseAt      *int64         `json:"last_use_at"`
	LastFailAt     *int64         `json:"last_fail_at"`
	LastFailReason *string        `json:"last_fail_reason"`
	LastSyncAt     *int64         `json:"last_sync_at"`
	LastClearAt    *int64         `json:"last_clear_at"`
	StateReason    *string        `json:"state_reason"`
	DeletedAt      *int64         `json:"deleted_at"`
	Ext            map[string]any `json:"ext"`
	Revision       int            `json:"revision"`
}

// NewRecord constructs a default ACTIVE basic-pool record.
func NewRecord(token string) *Record {
	now := platform.NowMs()
	qs := DefaultQuotaSet("basic")
	return &Record{
		Token:     token,
		Pool:      "basic",
		Status:    StatusActive,
		CreatedAt: now,
		UpdatedAt: now,
		Tags:      []string{},
		Quota:     qs.ToMap(),
		Ext:       map[string]any{},
	}
}

func (r *Record) QuotaSet() QuotaSet {
	if r.Quota == nil {
		return DefaultQuotaSet(r.Pool)
	}
	return QuotaSetFromMap(r.Quota)
}

func (r *Record) WithQuotaSet(qs QuotaSet) *Record {
	cp := *r
	cp.Quota = qs.ToMap()
	return &cp
}

func (r *Record) IsDeleted() bool { return r.DeletedAt != nil }

func (r *Record) IsNSFW() bool {
	for _, t := range r.Tags {
		if t == "nsfw" {
			return true
		}
	}
	return false
}

func (r *Record) PoolID() PoolID {
	p, _ := PoolFromName(r.Pool)
	return p
}

// DeriveStatus resolves the effective status (COOLING may become ACTIVE once
// cooldown expires).
func (r *Record) DeriveStatus(nowMs int64) Status {
	if r.Status != StatusCooling {
		return r.Status
	}
	if r.Ext == nil {
		return StatusCooling
	}
	v, _ := r.Ext["cooldown_until"].(float64)
	if v == 0 {
		return StatusCooling
	}
	if nowMs >= int64(v) {
		return StatusActive
	}
	return StatusCooling
}

// IsManageable returns true for an active or cooling account.
func (r *Record) IsManageable(nowMs int64) bool {
	if r.IsDeleted() {
		return false
	}
	s := r.DeriveStatus(nowMs)
	return s == StatusActive || s == StatusCooling
}

// IsSelectable returns true when the account can serve the given mode.
func (r *Record) IsSelectable(modeID int, nowMs int64) bool {
	if r.IsDeleted() {
		return false
	}
	if r.DeriveStatus(nowMs) != StatusActive {
		return false
	}
	qs := r.QuotaSet()
	w := qs.Get(modeID)
	if w == nil || w.WindowSeconds <= 0 {
		return false
	}
	if w.IsExhausted() && !w.IsWindowExpired(nowMs) {
		return false
	}
	return true
}

// SortTags dedups and sorts tags in place.
func SortTags(tags []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(tags))
	for _, t := range tags {
		if t == "" || seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// MergeTags applies add/remove set arithmetic to a tag slice.
func MergeTags(current []string, add, remove []string) []string {
	set := map[string]bool{}
	for _, t := range current {
		if t != "" {
			set[t] = true
		}
	}
	for _, t := range add {
		if t != "" {
			set[t] = true
		}
	}
	for _, t := range remove {
		delete(set, t)
	}
	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// --- helpers ---

func toInt(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	}
	return 0
}

func toInt64(v any) int64 {
	switch t := v.(type) {
	case int:
		return int64(t)
	case int64:
		return t
	case float64:
		return int64(t)
	}
	return 0
}
