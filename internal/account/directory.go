package account

import (
	"context"
	"math/rand"
	"sort"
	"sync"

	"github.com/aurora-develop/grok2api/internal/platform"
)

// Slot is the in-memory runtime view of an account used for fast selection.
type Slot struct {
	Token        string
	PoolID       PoolID
	StatusID     StatusID
	Quota        QuotaSet
	Inflight     int
	FailCount    int
	UseCount     int
	Health       float64
	LastUseAt    int64
	LastFailAt   int64
	CoolingUntil int64 // seconds (random strategy)
	Tags         []string
}

// Lease is a reserved account slot.
type Lease struct {
	ID         int64
	Token      string
	PoolID     PoolID
	ModeID     int
	SelectedAt int64
}

// Strategy is the active selection strategy.
type Strategy string

const (
	StrategyRandom Strategy = "random"
	StrategyQuota  Strategy = "quota"
)

// Directory is the in-memory account runtime table with selection/release/feedback.
type Directory struct {
	mu          sync.Mutex
	slots       map[string]*Slot
	byMode      map[modeKey]map[string]struct{}
	revision    int
	repo        Repository
	strategy    Strategy
	leaseSeq    int64
	maxInflight int
}

type modeKey struct {
	pool   PoolID
	modeID int
}

// NewDirectory creates a directory backed by the given repository.
func NewDirectory(repo Repository) *Directory {
	return &Directory{
		slots:       map[string]*Slot{},
		byMode:      map[modeKey]map[string]struct{}{},
		repo:        repo,
		strategy:    StrategyQuota,
		maxInflight: 8,
	}
}

// SetStrategy switches the active selection strategy.
func (d *Directory) SetStrategy(s Strategy) {
	d.mu.Lock()
	d.strategy = s
	d.mu.Unlock()
}

// Strategy returns the active strategy.
func (d *Directory) Strategy() Strategy {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.strategy
}

// SetMaxInflight sets the per-account inflight cap (random strategy).
func (d *Directory) SetMaxInflight(n int) {
	d.mu.Lock()
	d.maxInflight = n
	d.mu.Unlock()
}

// Bootstrap loads the full snapshot from the repository.
func (d *Directory) Bootstrap(ctx context.Context) error {
	snap, err := d.repo.RuntimeSnapshot(ctx)
	if err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.slots = map[string]*Slot{}
	d.byMode = map[modeKey]map[string]struct{}{}
	for _, rec := range snap.Items {
		d.upsertLocked(rec)
	}
	d.revision = snap.Revision
	return nil
}

// SyncIfChanged pulls incremental changes since the last known revision.
func (d *Directory) SyncIfChanged(ctx context.Context) (bool, error) {
	d.mu.Lock()
	since := d.revision
	d.mu.Unlock()
	cs, err := d.repo.ScanChanges(ctx, since, 5000)
	if err != nil {
		return false, err
	}
	if len(cs.Items) == 0 {
		return false, nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, rec := range cs.Items {
		if rec.IsDeleted() {
			if s, ok := d.slots[rec.Token]; ok {
				d.removeFromIndexesLocked(s)
				delete(d.slots, rec.Token)
			}
			continue
		}
		d.upsertLocked(rec)
	}
	d.revision = cs.Revision
	return true, nil
}

func (d *Directory) upsertLocked(rec *Record) {
	nowMs := platform.NowMs()
	pool, _ := PoolFromName(rec.Pool)
	qs := NormalizeQuotaSet(rec.Pool, rec.QuotaSet())
	s := &Slot{
		Token:      rec.Token,
		PoolID:     pool,
		StatusID:   rec.DeriveStatus(nowMs).ID(),
		Quota:      qs,
		Health:     1.0,
		Inflight:   0,
		FailCount:  rec.UsageFailCount,
		UseCount:   rec.UsageUseCount,
		LastUseAt:  ptrVal(rec.LastUseAt),
		LastFailAt: ptrVal(rec.LastFailAt),
		Tags:       rec.Tags,
	}
	if s.LastUseAt == 0 {
		s.LastUseAt = 0
	}
	coolingUntil := toInt64(rec.Ext["cooldown_until"])
	s.CoolingUntil = coolingUntil / 1000
	if old, ok := d.slots[rec.Token]; ok {
		s.Inflight = old.Inflight
		s.Health = old.Health
	}
	d.slots[rec.Token] = s
	d.reindexLocked(s)
}

func (d *Directory) reindexLocked(s *Slot) {
	// Remove from all current mode buckets.
	for k := range d.byMode {
		delete(d.byMode[k], s.Token)
	}
	if s.StatusID != StatusIDActive {
		return
	}
	for _, modeID := range SupportedModeIDs(s.PoolID.Name()) {
		w := s.Quota.Get(modeID)
		if w == nil || w.WindowSeconds <= 0 {
			continue
		}
		k := modeKey{s.PoolID, modeID}
		if d.byMode[k] == nil {
			d.byMode[k] = map[string]struct{}{}
		}
		d.byMode[k][s.Token] = struct{}{}
	}
}

func (d *Directory) removeFromIndexesLocked(s *Slot) {
	for k := range d.byMode {
		delete(d.byMode[k], s.Token)
	}
}

// Reserve picks the best account for the (pool, mode) candidates.
func (d *Directory) Reserve(poolCandidates []int, modeID int, excludeTokens []string, preferTags []string) (*Lease, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	nowMs := platform.NowMs()
	nowSec := nowMs / 1000
	exclude := map[string]struct{}{}
	for _, t := range excludeTokens {
		exclude[t] = struct{}{}
	}
	for _, poolID := range poolCandidates {
		var chosen *Slot
		if d.strategy == StrategyQuota {
			chosen = d.quotaSelectLocked(poolID, modeID, exclude, nowMs)
		} else {
			chosen = d.randomSelectLocked(poolID, modeID, exclude, nowSec)
		}
		if chosen != nil {
			chosen.Inflight++
			chosen.LastUseAt = nowMs
			d.leaseSeq++
			selected := modeID
			if modeID == -1 {
				// reserve_any: pick the first supported mode the account has quota for.
				for _, m := range SupportedModeIDs(chosen.PoolID.Name()) {
					if w := chosen.Quota.Get(m); w != nil && w.WindowSeconds > 0 {
						selected = m
						break
					}
				}
			}
			return &Lease{
				ID:         d.leaseSeq,
				Token:      chosen.Token,
				PoolID:     chosen.PoolID,
				ModeID:     selected,
				SelectedAt: nowMs,
			}, selected
		}
	}
	return nil, modeID
}

func (d *Directory) quotaSelectLocked(poolID int, modeID int, exclude map[string]struct{}, nowMs int64) *Slot {
	key := modeKey{PoolID(poolID), modeID}
	bucket := d.byMode[key]
	if len(bucket) == 0 {
		return nil
	}
	nowSec := nowMs / 1000
	var best *Slot
	bestScore := 0.0
	hasBest := false
	for tok := range bucket {
		if _, skip := exclude[tok]; skip {
			continue
		}
		s := d.slots[tok]
		if s == nil {
			continue
		}
		w := s.Quota.Get(modeID)
		if w == nil {
			continue
		}
		if w.IsWindowExpired(nowMs) {
			w.Remaining = w.Total
			reset := nowMs + int64(w.WindowSeconds)*1000
			w.ResetAt = &reset
			s.Quota.Set(modeID, *w)
		}
		if w.Remaining <= 0 || s.Inflight >= quotaMaxInflight {
			continue
		}
		score := quotaScore(s, w.Remaining, nowSec)
		if !hasBest || score > bestScore {
			best = s
			bestScore = score
			hasBest = true
		}
	}
	return best
}

func quotaScore(s *Slot, remaining int, nowSec int64) float64 {
	score := s.Health*100.0 + float64(remaining)*25.0 - float64(s.Inflight)*20.0
	failPenalty := float64(s.FailCount)
	if failPenalty > 10 {
		failPenalty = 10
	}
	score -= failPenalty * 4.0
	if s.LastUseAt > 0 {
		ageSec := nowSec - s.LastUseAt/1000
		if ageSec < 60 {
			score -= (1 - float64(ageSec)/60) * 15.0
		}
	}
	return score
}

func (d *Directory) randomSelectLocked(poolID int, modeID int, exclude map[string]struct{}, nowSec int64) *Slot {
	// Union of all mode buckets for the pool (random strategy ignores specific mode).
	candidates := []*Slot{}
	for _, m := range SupportedModeIDs(PoolID(poolID).Name()) {
		for tok := range d.byMode[modeKey{PoolID(poolID), m}] {
			if _, skip := exclude[tok]; skip {
				continue
			}
			s := d.slots[tok]
			if s == nil {
				continue
			}
			if s.CoolingUntil > nowSec {
				continue
			}
			if s.Inflight >= d.maxInflight {
				continue
			}
			if s.FailCount >= randomMaxFails {
				continue
			}
			candidates = append(candidates, s)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	return candidates[rand.Intn(len(candidates))]
}

// ReserveAny picks any active account in the given pools (mode-agnostic).
func (d *Directory) ReserveAny(poolCandidates []int, excludeTokens []string) *Lease {
	lease, _ := d.Reserve(poolCandidates, -1, excludeTokens, nil)
	return lease
}

// Release decrements the inflight counter for a lease.
func (d *Directory) Release(l *Lease) {
	if l == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	s, ok := d.slots[l.Token]
	if !ok {
		return
	}
	if s.Inflight > 0 {
		s.Inflight--
	}
}

// Feedback applies a request outcome to the reserved account's runtime state.
func (d *Directory) Feedback(token string, kind FeedbackKind, modeID int, remaining *int, resetAtMs *int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	s, ok := d.slots[token]
	if !ok {
		return
	}
	nowMs := platform.NowMs()
	switch kind {
	case FbSuccess:
		if d.strategy == StrategyQuota && modeID >= 0 {
			w := s.Quota.Get(modeID)
			if w != nil && w.WindowSeconds > 0 {
				if w.Remaining > 0 {
					w.Remaining--
				}
				s.Quota.Set(modeID, *w)
			}
		}
		s.Health = minF(1.0, s.Health+successStep)
		// 【修改说明】FbSuccess 时递增使用次数和更新最后使用时间
		// 原代码只更新了 quota 和 health，遗漏了 UseCount 和 LastUseAt，导致管理面板显示的使用次数始终为 0。
		s.UseCount++
		s.LastUseAt = nowMs
	case FbRateLimited:
		if modeID >= 0 {
			w := s.Quota.Get(modeID)
			if w != nil && w.WindowSeconds > 0 {
				w.Remaining = 0
				reset := nowMs + int64(w.WindowSeconds)*1000
				if resetAtMs != nil && *resetAtMs > 0 {
					reset = nowMs + *resetAtMs
				}
				w.ResetAt = &reset
				s.Quota.Set(modeID, *w)
			}
		}
		if d.strategy == StrategyRandom {
			cooling := nowMs/1000 + d.poolCoolingSec(s.PoolID, modeID)
			if cooling > s.CoolingUntil {
				s.CoolingUntil = cooling
			}
		}
		s.Health = maxF(minHealth, s.Health*rateLimitFactor)
		s.FailCount++
		s.LastFailAt = nowMs
	case FbUnauthorized:
		s.Health = maxF(minHealth, s.Health*authFactor)
		s.FailCount++
		s.LastFailAt = nowMs
		s.StatusID = StatusIDExpired
		d.reindexLocked(s)
	case FbForbidden:
		s.Health = maxF(minHealth, s.Health*forbiddenFactor)
		s.FailCount++
		s.LastFailAt = nowMs
	case FbServerError:
		s.Health = maxF(minHealth, s.Health*serverErrorFactor)
		s.FailCount++
		s.LastFailAt = nowMs
	}
	if d.strategy == StrategyQuota && remaining != nil && modeID >= 0 {
		w := s.Quota.Get(modeID)
		if w != nil && w.WindowSeconds > 0 {
			rem := *remaining
			if rem < 0 {
				rem = 0
			}
			if rem > 32767 {
				rem = 32767
			}
			w.Remaining = rem
			if resetAtMs != nil {
				s.Quota.Set(modeID, *w)
				if s.StatusID == StatusIDActive {
					k := modeKey{s.PoolID, modeID}
					if d.byMode[k] == nil {
						d.byMode[k] = map[string]struct{}{}
					}
					d.byMode[k][s.Token] = struct{}{}
				}
			}
		}
	}
}

func (d *Directory) poolCoolingSec(pool PoolID, modeID int) int64 {
	if modeID == 5 { // ModeConsole
		return consoleCoolingSec
	}
	switch pool {
	case PoolBasic:
		return 86400
	case PoolSuper:
		return 7200
	case PoolHeavy:
		return 7200
	}
	return 7200
}

// Snapshot returns the current slot list (for admin/status).
func (d *Directory) Snapshot() []*Slot {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]*Slot, 0, len(d.slots))
	for _, s := range d.slots {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Token < out[j].Token })
	return out
}

// SlotStats returns aggregate runtime statistics across all in-memory slots.
func (d *Directory) SlotStats() map[string]any {
	d.mu.Lock()
	defer d.mu.Unlock()
	statusCounts := map[string]int{}
	poolCounts := map[string]int{}
	inflight := 0
	useCount := 0
	failCount := 0
	var lastUseAt int64
	for _, s := range d.slots {
		statusCounts[s.StatusID.String()]++
		poolCounts[s.PoolID.Name()]++
		inflight += s.Inflight
		useCount += s.UseCount
		failCount += s.FailCount
		if s.LastUseAt > lastUseAt {
			lastUseAt = s.LastUseAt
		}
	}
	return map[string]any{
		"total_slots":      len(d.slots),
		"status_counts":    statusCounts,
		"pool_counts":      poolCounts,
		"total_inflight":   inflight,
		"total_use_count":  useCount,
		"total_fail_count": failCount,
		"last_use_at":      lastUseAt,
	}
}

// Size returns the number of active accounts.
func (d *Directory) Size() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.slots)
}

// HasPoolMode reports whether at least one active account is registered for
// the (pool, mode) bucket.
func (d *Directory) HasPoolMode(poolID, modeID int) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	bucket := d.byMode[modeKey{PoolID(poolID), modeID}]
	return len(bucket) > 0
}

// Revision returns the last synced repository revision.
func (d *Directory) Revision() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.revision
}

const (
	quotaMaxInflight  = 12
	randomMaxFails    = 5
	successStep       = 0.12
	authFactor        = 0.55
	forbiddenFactor   = 0.25
	rateLimitFactor   = 0.45
	serverErrorFactor = 0.75
	minHealth         = 0.05
	consoleCoolingSec = 14400
)

func ptrVal(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

func minF(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func maxF(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
