package api

import (
	"context"
	"strings"

	"github.com/aurora-develop/grok2api/internal/account"
	"github.com/aurora-develop/grok2api/internal/config"
	"github.com/aurora-develop/grok2api/internal/model"
	"github.com/aurora-develop/grok2api/internal/platform"
)

// selectionMaxRetries mirrors _account_selection.selection_max_retries().
func selectionMaxRetries() int {
	strategy := config.Global().GetStr("account.refresh.strategy", "quota")
	if strategy == "random" {
		return 5
	}
	return config.Global().GetInt("retry.max_retries", 1)
}

// modeCandidates returns the candidate mode ids for a spec.
// AUTO mode falls back to (AUTO, FAST, EXPERT) when
// features.auto_chat_mode_fallback is true (default).
func modeCandidates(spec *model.Spec) []int {
	if spec.ModeId == model.ModeAuto && config.Global().GetBool("features.auto_chat_mode_fallback", true) {
		return []int{int(model.ModeAuto), int(model.ModeFast), int(model.ModeExpert)}
	}
	return []int{int(spec.ModeId)}
}

// reserveAccount picks an account for the spec, iterating candidate modes.
// Returns the lease and the actually selected mode id, or nil if none.
func reserveAccount(ctx context.Context, dir *account.Directory, spec *model.Spec, exclude []string) (*account.Lease, int) {
	for _, modeID := range modeCandidates(spec) {
		lease, _ := dir.Reserve(spec.PoolCandidates(), modeID, exclude, nil)
		if lease != nil {
			return lease, lease.ModeID
		}
	}
	return nil, int(spec.ModeId)
}

// shouldRetryUpstream returns true when an upstream error warrants a retry
// with a different account. Mirrors _should_retry_upstream.
func shouldRetryUpstream(err error) bool {
	if err == nil {
		return false
	}
	var appErr *platform.AppError
	if !asAppError(err, &appErr) {
		return false
	}
	if appErr.Kind == platform.ErrUpstream && platform.IsInvalidCredentialsBody(appErr.Body) {
		return true
	}
	codes := config.Global().GetList("retry.on_codes", []string{"429", "401", "503"})
	want := map[int]struct{}{}
	for _, c := range codes {
		var n int
		if _, perr := sscanfInt(strings.TrimSpace(c), &n); perr == nil {
			want[n] = struct{}{}
		}
	}
	if _, ok := want[appErr.Status]; ok {
		return true
	}
	return false
}

// asAppError is a thin wrapper around errors.As to avoid pulling errors
// at every call site.
func asAppError(err error, target **platform.AppError) bool {
	if err == nil {
		return false
	}
	for err != nil {
		if ae, ok := err.(*platform.AppError); ok {
			*target = ae
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// sscanfInt parses an integer like fmt.Sscanf("%d", ...).
func sscanfInt(s string, v *int) (int, error) {
	n := 0
	neg := false
	i := 0
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		neg = s[i] == '-'
		i++
	}
	if i == len(s) {
		return 0, errParseInt
	}
	for ; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, errParseInt
		}
		n = n*10 + int(c-'0')
	}
	if neg {
		n = -n
	}
	*v = n
	return 1, nil
}

var errParseInt = &parseIntErr{}

type parseIntErr struct{}

func (e *parseIntErr) Error() string { return "parseInt: invalid integer" }
