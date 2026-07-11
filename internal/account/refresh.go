package account

import (
	"context"
	"sync"
	"time"

	"github.com/aurora-develop/grok2api/internal/config"
	"github.com/aurora-develop/grok2api/internal/logger"
	"github.com/aurora-develop/grok2api/internal/platform"
)

// errInvalidCredentials is a sentinel returned by refreshOne when the upstream
// indicates the token is invalid or blocked. Callers can use errors.Is.
type errInvalidCredentials struct {
	Token string
	Err   error
}

func (e *errInvalidCredentials) Error() string {
	return "invalid credentials for token: " + e.Token + ": " + e.Err.Error()
}

func (e *errInvalidCredentials) Unwrap() error { return e.Err }

// ModeQuota is a single mode's live rate-limit window from the upstream API.
type ModeQuota struct {
	Remaining int
	Total     int
	WindowSec int
	ResetAtMs *int64
}

// UsageFetcher fetches live quota windows for a token/pool. Implemented by
// the grok transport package to avoid an import cycle.
type UsageFetcher interface {
	FetchAllQuotas(ctx context.Context, token, pool string, bootstrap bool) (map[int]ModeQuota, error)
	FetchModeQuota(ctx context.Context, token, pool string, modeID int) (*ModeQuota, error)
}

// RefreshService periodically syncs account quotas from the upstream API and
// records request failures.
type RefreshService struct {
	repo    Repository
	fetcher UsageFetcher

	onDemandMu       sync.Mutex
	lastOnDemand     time.Time
	minOnDemandDelta time.Duration
}

// NewRefreshService creates a refresh service bound to the repository.
func NewRefreshService(repo Repository, fetcher UsageFetcher) *RefreshService {
	return &RefreshService{
		repo:             repo,
		fetcher:          fetcher,
		minOnDemandDelta: 300 * time.Second,
	}
}

// RefreshTokens forces a full upstream quota refresh for the given tokens.
func (s *RefreshService) RefreshTokens(ctx context.Context, tokens []string) (int, int, error) {
	if s.fetcher == nil {
		return 0, 0, nil
	}
	refreshed := 0
	failed := 0
	for _, tok := range tokens {
		recs, err := s.repo.GetAccounts(ctx, []string{tok})
		if err != nil || len(recs) == 0 {
			failed++
			continue
		}
		rec := recs[0]
		if _, err := s.refreshOne(ctx, rec, true); err != nil {
			failed++
			continue
		}
		refreshed++
	}
	return refreshed, failed, nil
}

// RefreshScheduled refreshes all manageable accounts in a pool (or all pools).
func (s *RefreshService) RefreshScheduled(ctx context.Context, pool string) (int, int, error) {
	statusActive := StatusActive
	statusCooling := StatusCooling
	managed := 0
	refreshed := 0
	failed := 0
	for _, st := range []Status{statusActive, statusCooling} {
		page, err := s.repo.ListAccounts(ctx, ListQuery{Page: 1, PageSize: 2000, Status: &st})
		if err != nil {
			return refreshed, failed, err
		}
		for _, rec := range page.Items {
			if pool != "" && rec.Pool != pool {
				continue
			}
			managed++
			if _, err := s.refreshOne(ctx, rec, false); err != nil {
				failed++
				continue
			}
			refreshed++
		}
	}
	_ = managed
	return refreshed, failed, nil
}

// RefreshOnDemand is a throttled full refresh (called after a 429 in quota mode).
func (s *RefreshService) RefreshOnDemand(ctx context.Context) error {
	s.onDemandMu.Lock()
	if time.Since(s.lastOnDemand) < s.minOnDemandDelta {
		s.onDemandMu.Unlock()
		return nil
	}
	s.lastOnDemand = time.Now()
	s.onDemandMu.Unlock()
	_, _, _ = s.RefreshScheduled(ctx, "")
	return nil
}

func (s *RefreshService) refreshOne(ctx context.Context, rec *Record, bootstrap bool) (bool, error) {
	if s.fetcher == nil {
		return false, nil
	}
	quotas, err := s.fetcher.FetchAllQuotas(ctx, rec.Token, rec.Pool, bootstrap)
	if err != nil {
		// Check whether the upstream says the token is invalid.
		if appErr, ok := err.(*platform.AppError); ok {
			if (appErr.Status == 400 || appErr.Status == 401 || appErr.Status == 403) &&
				platform.IsInvalidCredentialsBody(appErr.Body) {
				// Mark as EXPIRED and persist immediately.
				_ = s.markCredentialsExpired(ctx, rec)
				return false, &errInvalidCredentials{Token: rec.Token, Err: err}
			}
		}
		// Record a failure and apply the fallback.
		_ = s.recordFailure(ctx, rec, err)
		return false, err
	}
	patch := Patch{Token: rec.Token}
	qs := rec.QuotaSet()
	for modeID, mq := range quotas {
		if !SupportsMode(rec.Pool, modeID) {
			continue
		}
		w := NormalizeQuotaWindow(rec.Pool, modeID, QuotaWindow{
			Remaining:     mq.Remaining,
			Total:         mq.Total,
			WindowSeconds: mq.WindowSec,
			ResetAt:       mq.ResetAtMs,
			SyncedAt:      ptrInt64(platform.NowMs()),
			Source:        SourceReal,
		})
		m := w.ToMap()
		switch modeID {
		case 0:
			patch.QuotaAuto = &m
		case 1:
			patch.QuotaFast = &m
		case 2:
			patch.QuotaExpert = &m
		case 3:
			patch.QuotaHeavy = &m
		case 4:
			patch.QuotaGrok43 = &m
		case 5:
			patch.QuotaConsole = &m
		}
		qs.Set(modeID, w)
	}
	// Infer pool from live quota data and patch if changed.
	inferred := InferPool(qs)
	if inferred != "" && inferred != rec.Pool {
		logger.Infof("account pool updated from live quota: token=%s... previous=%s current=%s", rec.Token[:10], rec.Pool, inferred)
		patch.Pool = &inferred
	}
	now := platform.NowMs()
	patch.LastSyncAt = &now
	patch.UsageSyncDelta = ptrInt(1)
	_, err = s.repo.PatchAccounts(ctx, []Patch{patch})
	return true, err
}

func (s *RefreshService) recordFailure(ctx context.Context, rec *Record, err error) error {
	reason := "fetch_failed"
	if err != nil {
		reason = err.Error()
	}
	now := platform.NowMs()
	patch := Patch{
		Token:          rec.Token,
		UsageFailDelta: ptrInt(1),
		LastFailAt:     &now,
		LastFailReason: &reason,
	}
	_, _ = s.repo.PatchAccounts(ctx, []Patch{patch})
	return nil
}

// RecordFailure records a single request failure (fire-and-forget).
// If the error indicates invalid credentials, the account is marked EXPIRED.
func (s *RefreshService) RecordFailure(ctx context.Context, token string, modeID int, err error) {
	// Check invalid credentials first.
	if appErr, ok := err.(*platform.AppError); ok {
		if (appErr.Status == 400 || appErr.Status == 401 || appErr.Status == 403) &&
			platform.IsInvalidCredentialsBody(appErr.Body) {
			recs, _ := s.repo.GetAccounts(ctx, []string{token})
			if len(recs) > 0 {
				_ = s.markCredentialsExpired(ctx, recs[0])
			}
			return
		}
	}

	reason := "rate_limited"
	if err != nil {
		reason = err.Error()
	}
	now := platform.NowMs()
	patch := Patch{
		Token:          token,
		UsageFailDelta: ptrInt(1),
		LastFailAt:     &now,
		LastFailReason: &reason,
	}
	_, _ = s.repo.PatchAccounts(ctx, []Patch{patch})
}

// markCredentialsExpired sets the account status to EXPIRED with reason.
func (s *RefreshService) markCredentialsExpired(ctx context.Context, rec *Record) error {
	now := platform.NowMs()
	reason := "invalid_credentials"
	ext := rec.Ext
	if ext == nil {
		ext = map[string]any{}
	}
	ext["expired_at"] = now
	ext["expired_reason"] = reason
	st := StatusExpired
	patch := Patch{
		Token:          rec.Token,
		Status:         &st,
		LastFailAt:     &now,
		LastFailReason: &reason,
		StateReason:    &reason,
		ExtMerge:       ext,
	}
	_, err := s.repo.PatchAccounts(ctx, []Patch{patch})
	logger.Warnf("account expired: token=%s... reason=%s", rec.Token[:10], reason)
	return err
}

// ResetExpiredConsoleWindows delegates to the repository.
func (s *RefreshService) ResetExpiredConsoleWindows(ctx context.Context) (int, error) {
	return s.repo.ResetExpiredConsoleWindows(ctx)
}

// RecoverConsoleExpiredAccounts delegates to the repository.
func (s *RefreshService) RecoverConsoleExpiredAccounts(ctx context.Context) (int, error) {
	return s.repo.RecoverConsoleExpiredAccounts(ctx)
}

// ReconcileRuntime applies the config-driven strategy and returns the
// effective strategy name ("quota" or "random").
func ReconcileRuntime(dir *Directory, enabled bool) Strategy {
	target := StrategyRandom
	if enabled {
		target = StrategyQuota
	}
	dir.SetStrategy(target)
	// Apply runtime inflight cap from config.
	if cfg := config.Global(); cfg != nil {
		dir.SetMaxInflight(cfg.GetInt("account.selection.max_inflight", 8))
	}
	return target
}

func ptrInt(v int) *int { return &v }
