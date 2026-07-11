package account

import (
	"strconv"

	"github.com/aurora-develop/grok2api/internal/platform"
)

// Policy governs the state machine transitions.
type Policy struct {
	FailThreshold    int // consecutive failures -> COOLING (advisory; cooldown_until drives actual transition)
	ForbiddenStrikes int // 403 count -> DISABLED
	DefaultCoolingMs int64
}

var DefaultPolicy = Policy{
	FailThreshold:    5,
	ForbiddenStrikes: 1,
	DefaultCoolingMs: 15 * 60 * 1000,
}

// Feedback describes the outcome of one upstream request applied to a record.
type Feedback struct {
	Kind           FeedbackKind
	ModeID         int
	At             int64
	StatusCode     int
	Reason         string
	QuotaWindow    *QuotaWindow
	RetryAfterMs   *int64
	ConfirmExpired bool
	ApplyUsage     bool
}

// FeedbackFromStatus builds a Feedback from an HTTP status (apply_usage=false).
// When status is 400/401/403 and the response body contains known invalid-credential
// markers, confirmExpired is forced to true automatically.
func FeedbackFromStatus(status int, modeID int, reason string, retryAfterMs *int64, confirmExpired bool) Feedback {
	if !confirmExpired && (status == 400 || status == 401 || status == 403) {
		if platform.IsInvalidCredentialsBody(reason) {
			confirmExpired = true
		}
	}
	fb := Feedback{
		Kind:           FeedbackKindFromStatus(status),
		ModeID:         modeID,
		StatusCode:     status,
		Reason:         reason,
		RetryAfterMs:   retryAfterMs,
		ConfirmExpired: confirmExpired,
		ApplyUsage:     false,
	}
	fb.At = platform.NowMs()
	return fb
}

// ApplyFeedback returns a new record with the feedback applied (immutable).
func ApplyFeedback(r *Record, fb Feedback, policy Policy) *Record {
	cp := *r
	if cp.Ext == nil {
		cp.Ext = map[string]any{}
	}
	now := fb.At
	if now == 0 {
		now = platform.NowMs()
	}
	cp.UpdatedAt = now
	qs := cp.QuotaSet()

	// Quota updates.
	if fb.QuotaWindow != nil {
		qs.Set(fb.ModeID, *fb.QuotaWindow)
		cp.LastSyncAt = ptrInt64(now)
		cp.UsageSyncCount++
	} else if fb.Kind == FbSuccess {
		w := qs.Get(fb.ModeID)
		if w != nil && w.WindowSeconds > 0 {
			if w.Remaining > 0 {
				w.Remaining--
			}
			qs.Set(fb.ModeID, *w)
		}
	} else if fb.Kind == FbRateLimited {
		w := qs.Get(fb.ModeID)
		if w != nil && w.WindowSeconds > 0 {
			w.Remaining = 0
			reset := now + int64(w.WindowSeconds)*1000
			if fb.RetryAfterMs != nil && *fb.RetryAfterMs > 0 {
				reset = now + *fb.RetryAfterMs
			}
			w.ResetAt = &reset
			qs.Set(fb.ModeID, *w)
		}
	}
	cp.Quota = qs.ToMap()

	// Usage counters.
	if fb.Kind == FbSuccess && fb.ApplyUsage {
		cp.UsageUseCount++
		cp.LastUseAt = ptrInt64(now)
	} else if fb.Kind != FbSuccess && fb.Kind != FbRestore && fb.Kind != FbDisable && fb.Kind != FbDelete {
		cp.UsageFailCount++
		cp.LastFailAt = ptrInt64(now)
		reason := fb.Reason
		if reason == "" {
			reason = intToStr(fb.StatusCode)
		}
		cp.LastFailReason = ptrString(reason)
	}

	// Status transitions.
	switch fb.Kind {
	case FbUnauthorized:
		if fb.ConfirmExpired {
			cp.Status = StatusExpired
			sr := "token_expired"
			cp.StateReason = &sr
			cp.Ext["expired_at"] = now
			cp.Ext["expired_reason"] = sr
		}
	case FbForbidden:
		strikes := toInt(cp.Ext["forbidden_strikes"]) + 1
		cp.Ext["forbidden_strikes"] = strikes
		if strikes >= policy.ForbiddenStrikes {
			cp.Status = StatusDisabled
			sr := "forbidden"
			cp.StateReason = &sr
			cp.Ext["disabled_at"] = now
			cp.Ext["disabled_reason"] = sr
		}
	case FbRateLimited:
		cp.Status = StatusCooling
		sr := "rate_limited"
		cp.StateReason = &sr
		cooling := now + policy.DefaultCoolingMs
		if fb.RetryAfterMs != nil && *fb.RetryAfterMs > 0 {
			cooling = now + *fb.RetryAfterMs
		}
		cp.Ext["cooldown_until"] = cooling
		cp.Ext["cooldown_reason"] = sr
	case FbSuccess:
		if cp.Status == StatusCooling {
			ct := toInt64(cp.Ext["cooldown_until"])
			if ct > 0 && now >= ct {
				cp.Status = StatusActive
				cp.StateReason = nil
				clearFailureKeys(&cp)
			}
		}
	case FbDisable:
		cp.Status = StatusDisabled
		sr := "operator_disabled"
		cp.StateReason = &sr
		cp.Ext["disabled_at"] = now
		cp.Ext["disabled_reason"] = sr
	case FbDelete:
		// caller sets DeletedAt
	case FbRestore:
		cp.Status = StatusActive
		cp.StateReason = nil
		clearFailureKeys(&cp)
		cp.Quota = DefaultQuotaSet(cp.Pool).ToMap()
	}
	return &cp
}

// ClearFailures resets failure counters and restores ACTIVE.
func ClearFailures(r *Record) *Record {
	cp := *r
	cp.Status = StatusActive
	cp.StateReason = nil
	cp.UsageFailCount = 0
	cp.LastFailAt = nil
	cp.LastFailReason = nil
	if cp.Ext == nil {
		cp.Ext = map[string]any{}
	}
	clearFailureKeys(&cp)
	cp.UpdatedAt = platform.NowMs()
	return &cp
}

func clearFailureKeys(r *Record) {
	for _, k := range []string{"cooldown_until", "cooldown_reason", "disabled_at", "disabled_reason",
		"expired_at", "expired_reason", "forbidden_strikes"} {
		delete(r.Ext, k)
	}
}

func ptrInt64(v int64) *int64    { return &v }
func ptrString(v string) *string { return &v }

func intToStr(i int) string {
	if i == 0 {
		return ""
	}
	return strconv.Itoa(i)
}
