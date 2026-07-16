package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/aurora-develop/grok2api/internal/account"
	"github.com/aurora-develop/grok2api/internal/config"
	"github.com/aurora-develop/grok2api/internal/grok"
	"github.com/aurora-develop/grok2api/internal/logger"
	"github.com/aurora-develop/grok2api/internal/platform"
	"github.com/aurora-develop/grok2api/internal/storage"
)

// --- System endpoints ---

func (s *Server) handleStorageGet(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"type": "jsonl"})
}

func (s *Server) handleStatusGet(c *gin.Context) {
	if s.Directory == nil {
		writeAppError(c, platform.NewAppError("directory not initialised", platform.ErrServer, "directory_not_initialised", http.StatusServiceUnavailable))
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"status":             "ok",
		"size":               s.Directory.Size(),
		"revision":           s.Directory.Revision(),
		"selection_strategy": string(s.Directory.Strategy()),
	})
}

func (s *Server) handleDirectoryStats(c *gin.Context) {
	if s.Directory == nil {
		writeAppError(c, platform.NewAppError("directory not initialised", platform.ErrServer, "directory_not_initialised", http.StatusServiceUnavailable))
		return
	}
	c.JSON(http.StatusOK, s.Directory.SlotStats())
}

func (s *Server) handleAdminVideoJobs(c *gin.Context) {
	// 【修改说明】添加分页支持，与缓存列表 API 一致
	page := parseIntQuery(c, "page", 1)
	pageSize := parseIntQuery(c, "page_size", 50)
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 50
	}
	videoJobsMutex.Lock()
	jobs := make([]*videoJob, 0, len(videoJobsMap))
	for _, j := range videoJobsMap {
		jobs = append(jobs, j)
	}
	total := len(videoJobsMap)
	videoJobsMutex.Unlock()
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].CreatedAt > jobs[j].CreatedAt
	})
	start := (page - 1) * pageSize
	end := start + pageSize
	if start > total {
		start = total
	}
	if end > total {
		end = total
	}
	pageJobs := jobs[start:end]
	out := make([]map[string]any, 0, len(pageJobs))
	for _, j := range pageJobs {
		out = append(out, j.toDict())
	}
	c.JSON(http.StatusOK, gin.H{
		"jobs":      out,
		"total":     total,
		"page":      page,
		"page_size": pageSize,
	})
}

func (s *Server) handleSync(c *gin.Context) {
	if s.Directory == nil {
		writeAppError(c, platform.NewAppError("directory not initialised", platform.ErrServer, "directory_not_initialised", http.StatusServiceUnavailable))
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()
	changed, err := s.Directory.SyncIfChanged(ctx)
	if err != nil {
		writeAppError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"changed":  changed,
		"revision": s.Directory.Revision(),
	})
}

// --- Config ---

func (s *Server) handleConfigGet(c *gin.Context) {
	raw := config.Global().Raw()
	c.JSON(http.StatusOK, raw)
}

func (s *Server) handleConfigUpdate(c *gin.Context) {
	var patch map[string]any
	if err := readJSON(c, &patch); err != nil {
		writeAppError(c, err)
		return
	}
	if err := validatePatch(patch); err != nil {
		writeAppError(c, err)
		return
	}
	if err := config.Global().Update(patch); err != nil {
		writeAppError(c, platform.UpstreamError("config update failed: "+err.Error(), 500, ""))
		return
	}
	_ = config.Load()
	strategy := "random"
	if config.Global().GetBool("account.refresh.enabled", false) {
		strategy = "quota"
	}
	if s.Directory != nil {
		s.Directory.SetStrategy(account.Strategy(strategy))
	}
	c.JSON(http.StatusOK, gin.H{
		"status":             "success",
		"message":            "配置已更新",
		"selection_strategy": strategy,
	})
}

// validatePatch rejects startup-only config paths from runtime patches.
func validatePatch(patch map[string]any) error {
	for k := range patch {
		if config.IsStartupOnlyConfigKey(k) {
			return platform.ValidationErrorCode(
				"Config key '"+k+"' is reserved for startup", k, "startup_only_config")
		}
	}
	return nil
}

// --- Tokens CRUD ---

func (s *Server) handleTokensList(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()
	page, err := s.Repo.ListAccounts(ctx, account.ListQuery{Page: 1, PageSize: 5000, IncludeDeleted: false})
	if err != nil {
		writeAppError(c, err)
		return
	}
	// 【修改说明】获取内存中的 Slot 数据，合并 use_count 和 last_used_at
	// 持久化 Record 的 UsageUseCount 不实时更新，内存 Slot 的 UseCount 是实时递增的
	slots := map[string]*account.Slot{}
	if s.Directory != nil {
		for _, sl := range s.Directory.Snapshot() {
			slots[sl.Token] = sl
		}
	}
	out := []map[string]any{}
	for _, rec := range page.Items {
		out = append(out, s.serializeRecordWithSlot(rec, slots[rec.Token]))
	}
	c.JSON(http.StatusOK, gin.H{"tokens": out})
}

// serializeRecordWithSlot 合并持久化 Record 和内存 Slot 的数据
// 【修改说明】使用 Slot 的实时 UseCount 和 LastUseAt 替代 Record 中不实时更新的值
// 同时添加 last_fail_reason 和 last_fail_at 字段供前端显示 401 等错误原因
func (s *Server) serializeRecordWithSlot(rec *account.Record, slot *account.Slot) map[string]any {
	qs := rec.QuotaSet()
	quota := map[string]any{}
	addQ := func(name string, w *account.QuotaWindow) {
		if w == nil || w.WindowSeconds <= 0 {
			return
		}
		quota[name] = map[string]any{"remaining": w.Remaining, "total": w.Total}
	}
	auto := qs.Auto
	fast := qs.Fast
	expert := qs.Expert
	addQ("auto", &auto)
	addQ("fast", &fast)
	addQ("expert", &expert)
	addQ("heavy", qs.Heavy)
	addQ("console", qs.Console)

	// 优先使用内存 Slot 的实时数据，fallback 到持久化 Record
	useCount := rec.UsageUseCount
	lastUsed := int64(0)
	if rec.LastUseAt != nil {
		lastUsed = *rec.LastUseAt
	}
	if slot != nil {
		if slot.UseCount > useCount {
			useCount = slot.UseCount
		}
		if slot.LastUseAt > lastUsed {
			lastUsed = slot.LastUseAt
		}
	}

	lastFailAt := int64(0)
	if rec.LastFailAt != nil {
		lastFailAt = *rec.LastFailAt
	}
	lastFailReason := ""
	if rec.LastFailReason != nil {
		lastFailReason = *rec.LastFailReason
	}

	pool := rec.Pool
	if pool == "" {
		pool = "basic"
	}
	membershipExpiresAt := ""
	if rec.Ext != nil {
		if v, ok := rec.Ext["membership_expires_at"].(string); ok {
			membershipExpiresAt = v
		}
	}
	return map[string]any{
		"token":                 rec.Token,
		"pool":                  pool,
		"status":                string(rec.Status),
		"quota":                 quota,
		"use_count":             useCount,
		"last_used_at":          lastUsed,
		"tags":                  rec.Tags,
		"membership_expires_at": membershipExpiresAt,
		"last_fail_reason":      lastFailReason,
		"last_fail_at":          lastFailAt,
	}
}

func (s *Server) handleTokensReplace(c *gin.Context) {
	var body map[string]any
	if err := readJSON(c, &body); err != nil {
		writeAppError(c, err)
		return
	}
	total := 0
	for poolName, items := range body {
		if poolName == "" {
			continue
		}
		_, ok := account.PoolFromName(poolName)
		if !ok {
			continue
		}
		tokenList, _ := items.([]any)
		upserts := []account.Upsert{}
		for _, item := range tokenList {
			var token string
			var tags []string
			switch v := item.(type) {
			case string:
				token = v
			case map[string]any:
				if t, ok := v["token"].(string); ok {
					token = t
				}
				if tagList, ok := v["tags"].([]any); ok {
					for _, t := range tagList {
						if ts, ok := t.(string); ok {
							tags = append(tags, ts)
						}
					}
				}
			}
			token = platform.SanitizeToken(token)
			if token == "" {
				continue
			}
			upserts = append(upserts, account.Upsert{Token: token, Pool: poolName, Tags: account.SortTags(tags)})
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
		_, err := s.Repo.ReplacePool(ctx, poolName, upserts)
		cancel()
		if err != nil {
			writeAppError(c, err)
			return
		}
		total += len(upserts)
	}
	c.JSON(http.StatusOK, gin.H{"status": "success", "count": total})
}

func (s *Server) handleTokensAdd(c *gin.Context) {
	var body struct {
		Tokens    []string `json:"tokens"`
		Pool      string   `json:"pool"`
		Tags      []string `json:"tags"`
		ExpiresAt string   `json:"expires_at"`
	}
	if err := readJSON(c, &body); err != nil {
		writeAppError(c, err)
		return
	}
	pool := body.Pool
	if pool == "" {
		pool = "basic"
	}
	autoDetect := pool == "auto"
	if autoDetect {
		pool = "basic" // temporary; will be inferred after refresh
	}
	if _, ok := account.PoolFromName(pool); !ok && !autoDetect {
		writeAppError(c, platform.ValidationErrorCode("Invalid pool '"+pool+"'", "pool", "invalid_pool"))
		return
	}
	tags := account.SortTags(body.Tags)
	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()

	existing, err := s.Repo.GetAccounts(ctx, body.Tokens)
	if err != nil {
		writeAppError(c, err)
		return
	}
	existingActive := map[string]bool{}
	for _, rec := range existing {
		if rec != nil && !rec.IsDeleted() {
			existingActive[rec.Token] = true
		}
	}
	var newTokens []string
	upserts := []account.Upsert{}
	skipped := 0
	seen := map[string]bool{}
	for _, raw := range body.Tokens {
		tok := platform.SanitizeToken(raw)
		if tok == "" || seen[tok] {
			continue
		}
		seen[tok] = true
		if existingActive[tok] {
			skipped++
			continue
		}
		// 【修改说明】导入时把会员到期时间存到 Ext
		ext := map[string]any{}
		if body.ExpiresAt != "" {
			ext["membership_expires_at"] = body.ExpiresAt
		}
		upserts = append(upserts, account.Upsert{Token: tok, Pool: pool, Tags: tags, Ext: ext})
		newTokens = append(newTokens, tok)
	}
	if len(upserts) > 0 {
		if _, err := s.Repo.UpsertAccounts(ctx, upserts); err != nil {
			writeAppError(c, err)
			return
		}
	}

	// Trigger async refresh + auto-detect pool for newly imported tokens.
	if len(newTokens) > 0 && s.Refresh != nil {
		if autoDetect {
			go func() {
				refCtx, refCancel := context.WithTimeout(context.Background(), 120*time.Second)
				defer refCancel()
				refreshed, failed, err := s.Refresh.RefreshTokens(refCtx, newTokens)
				if err != nil {
					return
				}
				_ = refreshed
				_ = failed
				// After refresh, check if pool was auto-inferred.
				checkCtx, checkCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer checkCancel()
				recs, _ := s.Repo.GetAccounts(checkCtx, newTokens)
				for _, rec := range recs {
					if rec == nil || rec.IsDeleted() {
						continue
					}
					inferred := account.InferPool(rec.QuotaSet())
					if inferred != "" && inferred != rec.Pool {
						patchCtx, patchCancel := context.WithTimeout(context.Background(), 10*time.Second)
						p := rec.Pool
						patch := account.Patch{Token: rec.Token, Pool: &inferred}
						_, _ = s.Repo.PatchAccounts(patchCtx, []account.Patch{patch})
						patchCancel()
						logger.Infof("admin auto-detect pool: token=%s... previous=%s current=%s", rec.Token[:10], p, inferred)
					}
				}
			}()
		} else {
			go func() {
				refCtx, refCancel := context.WithTimeout(context.Background(), 120*time.Second)
				defer refCancel()
				_, _, _ = s.Refresh.RefreshTokens(refCtx, newTokens)
			}()
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"count":   len(upserts),
		"skipped": skipped,
	})
}

func (s *Server) handleTokensDelete(c *gin.Context) {
	var tokens []string
	if err := readJSON(c, &tokens); err != nil {
		writeAppError(c, err)
		return
	}
	clean := []string{}
	seen := map[string]bool{}
	for _, t := range tokens {
		t = platform.SanitizeToken(t)
		if t == "" || seen[t] {
			continue
		}
		seen[t] = true
		clean = append(clean, t)
	}
	if len(clean) == 0 {
		writeAppError(c, platform.ValidationError("No valid tokens provided", "tokens"))
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()
	result, err := s.Repo.DeleteAccounts(ctx, clean)
	if err != nil {
		writeAppError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": result.Deleted})
}

func (s *Server) handleTokensDeleteInvalid(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()
	page, err := s.Repo.ListAccounts(ctx, account.ListQuery{Page: 1, PageSize: 5000, IncludeDeleted: false})
	if err != nil {
		writeAppError(c, err)
		return
	}
	tokens := []string{}
	for _, rec := range page.Items {
		if rec.Status == account.StatusActive || rec.Status == account.StatusCooling || rec.Status == account.StatusDisabled {
			continue
		}
		tokens = append(tokens, rec.Token)
	}
	if len(tokens) == 0 {
		c.JSON(http.StatusOK, gin.H{"deleted": 0})
		return
	}
	result, err := s.Repo.DeleteAccounts(ctx, tokens)
	if err != nil {
		writeAppError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": result.Deleted})
}

func (s *Server) handleTokensEdit(c *gin.Context) {
	var body struct {
		OldToken  string `json:"old_token"`
		Token     string `json:"token"`
		Pool      string `json:"pool"`
		ExpiresAt string `json:"expires_at"`
	}
	if err := readJSON(c, &body); err != nil {
		writeAppError(c, err)
		return
	}
	old := platform.SanitizeToken(body.OldToken)
	newTok := platform.SanitizeToken(body.Token)
	pool := body.Pool
	if pool == "" {
		pool = "basic"
	}
	if old == "" || newTok == "" {
		writeAppError(c, platform.ValidationError("Missing token", "body"))
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()
	recs, err := s.Repo.GetAccounts(ctx, []string{old, newTok})
	if err != nil {
		writeAppError(c, err)
		return
	}
	var oldRec, newRec *account.Record
	for _, rec := range recs {
		if rec.Token == old {
			oldRec = rec
		} else if rec.Token == newTok {
			newRec = rec
		}
	}
	if oldRec == nil {
		writeAppError(c, platform.ValidationErrorCode("Account not found", "old_token", "account_not_found"))
		return
	}
	if old != newTok && newRec != nil && !newRec.IsDeleted() {
		writeAppError(c, platform.NewAppError("Token conflict", platform.ErrValidation, "token_conflict", http.StatusConflict))
		return
	}
	tags := oldRec.Tags
	ext := oldRec.Ext
	if ext == nil {
		ext = map[string]any{}
	}
	// 【修改说明】编辑时更新会员到期时间，空字符串则清除
	if body.ExpiresAt != "" {
		ext["membership_expires_at"] = body.ExpiresAt
	} else {
		delete(ext, "membership_expires_at")
	}
	upserts := []account.Upsert{{Token: newTok, Pool: pool, Tags: tags, Ext: ext}}
	if _, err := s.Repo.UpsertAccounts(ctx, upserts); err != nil {
		writeAppError(c, err)
		return
	}
	if old != newTok {
		if _, err := s.Repo.DeleteAccounts(ctx, []string{old}); err != nil {
			writeAppError(c, err)
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"status": "success", "token": newTok, "pool": pool})
}

func (s *Server) handleTokensToggleDisabled(c *gin.Context) {
	var body struct {
		Token    string `json:"token"`
		Disabled bool   `json:"disabled"`
	}
	if err := readJSON(c, &body); err != nil {
		writeAppError(c, err)
		return
	}
	token := platform.SanitizeToken(body.Token)
	if token == "" {
		writeAppError(c, platform.ValidationError("Missing token", "token"))
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()
	recs, err := s.Repo.GetAccounts(ctx, []string{token})
	if err != nil || len(recs) == 0 {
		writeAppError(c, platform.ValidationErrorCode("Account not found", "token", "account_not_found"))
		return
	}
	patches := []account.Patch{buildTogglePatch(token, body.Disabled)}
	if _, err := s.Repo.PatchAccounts(ctx, patches); err != nil {
		writeAppError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "success", "token": token, "disabled": body.Disabled})
}

func (s *Server) handleTokensToggleDisabledBatch(c *gin.Context) {
	var body struct {
		Tokens   []string `json:"tokens"`
		Disabled bool     `json:"disabled"`
	}
	if err := readJSON(c, &body); err != nil {
		writeAppError(c, err)
		return
	}
	clean := []string{}
	seen := map[string]bool{}
	for _, t := range body.Tokens {
		t = platform.SanitizeToken(t)
		if t == "" || seen[t] {
			continue
		}
		seen[t] = true
		clean = append(clean, t)
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()
	recs, err := s.Repo.GetAccounts(ctx, clean)
	if err != nil || len(recs) == 0 {
		writeAppError(c, platform.ValidationErrorCode("No accounts found", "tokens", "account_not_found"))
		return
	}
	patches := []account.Patch{}
	for _, rec := range recs {
		patches = append(patches, buildTogglePatch(rec.Token, body.Disabled))
	}
	result, err := s.Repo.PatchAccounts(ctx, patches)
	if err != nil {
		writeAppError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"status":   "success",
		"disabled": body.Disabled,
		"summary": gin.H{
			"total": len(patches), "ok": result.Patched, "fail": len(patches) - result.Patched,
		},
	})
}

func buildTogglePatch(token string, disabled bool) account.Patch {
	now := platform.NowMs()
	p := account.Patch{Token: token}
	if disabled {
		st := account.StatusDisabled
		p.Status = &st
		reason := "operator_disabled"
		p.StateReason = &reason
		p.ExtMerge = map[string]any{
			"disabled_at":     now,
			"disabled_reason": "operator_disabled",
		}
	} else {
		st := account.StatusActive
		p.Status = &st
		p.ClearFailures = true
	}
	return p
}

func (s *Server) handlePoolReplace(c *gin.Context) {
	var body struct {
		Pool   string   `json:"pool"`
		Tokens []string `json:"tokens"`
		Tags   []string `json:"tags"`
	}
	if err := readJSON(c, &body); err != nil {
		writeAppError(c, err)
		return
	}
	pool := body.Pool
	if pool == "" {
		pool = "basic"
	}
	if _, ok := account.PoolFromName(pool); !ok {
		writeAppError(c, platform.ValidationErrorCode("Invalid pool '"+pool+"'", "pool", "invalid_pool"))
		return
	}
	tags := account.SortTags(body.Tags)
	upserts := []account.Upsert{}
	seen := map[string]bool{}
	for _, raw := range body.Tokens {
		tok := platform.SanitizeToken(raw)
		if tok == "" || seen[tok] {
			continue
		}
		seen[tok] = true
		upserts = append(upserts, account.Upsert{Token: tok, Pool: pool, Tags: tags})
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()
	if _, err := s.Repo.ReplacePool(ctx, pool, upserts); err != nil {
		writeAppError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"pool": pool, "count": len(upserts)})
}

// --- Batch operations ---

func (s *Server) handleBatchNSFW(c *gin.Context) {
	var body struct {
		Tokens []string `json:"tokens"`
	}
	if err := readJSON(c, &body); err != nil {
		writeAppError(c, err)
		return
	}
	enabled := true
	if c.Query("enabled") == "false" {
		enabled = false
	}
	tokens := sanitizeTokenList(body.Tokens)
	if len(tokens) == 0 {
		writeAppError(c, platform.ValidationError("No tokens provided", "tokens"))
		return
	}
	results := map[string]any{}
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	conc := clampInt(parseIntQuery(c, "concurrency", 50), 1, 80)
	sem := make(chan struct{}, conc)
	for _, tok := range tokens {
		wg.Add(1)
		go func(t string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			err := s.runNSFWOne(c.Request.Context(), t, enabled)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				results[maskToken(t)] = gin.H{"error": err.Error()}
			} else {
				results[maskToken(t)] = gin.H{"success": true}
			}
		}(tok)
	}
	wg.Wait()
	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"results": results,
	})
}

func (s *Server) runNSFWOne(ctx context.Context, token string, enabled bool) error {
	if enabled {
		if err := grok.NSFWSequence(ctx, s.Transport, token); err != nil {
			return err
		}
	} else {
		if _, err := grok.DisableNSFW(ctx, s.Transport, token); err != nil {
			return err
		}
	}
	// 【修改说明】NSFW 操作成功后更新账号 tags，使前端能正确显示 NSFW 状态
	if s.Repo != nil {
		p := account.Patch{Token: token}
		if enabled {
			p.AddTags = []string{"nsfw"}
		} else {
			p.RemoveTags = []string{"nsfw"}
		}
		_, _ = s.Repo.PatchAccounts(ctx, []account.Patch{p})
	}
	return nil
}

func (s *Server) handleBatchRefresh(c *gin.Context) {
	var body struct {
		Tokens []string `json:"tokens"`
	}
	if err := readJSON(c, &body); err != nil {
		writeAppError(c, err)
		return
	}
	tokens := sanitizeTokenList(body.Tokens)
	if len(tokens) == 0 {
		writeAppError(c, platform.ValidationError("No tokens provided", "tokens"))
		return
	}
	if s.Refresh == nil {
		writeAppError(c, platform.NewAppError("refresh service not available", platform.ErrServer, "no_refresh", 503))
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Minute)
	defer cancel()
	results := map[string]any{}
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	conc := clampInt(parseIntQuery(c, "concurrency", 50), 1, 80)
	sem := make(chan struct{}, conc)
	for _, tok := range tokens {
		wg.Add(1)
		go func(t string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			refreshed, _, err := s.Refresh.RefreshTokens(ctx, []string{t})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				results[maskToken(t)] = gin.H{"error": err.Error()}
			} else {
				results[maskToken(t)] = gin.H{"refreshed": refreshed > 0}
			}
		}(tok)
	}
	wg.Wait()
	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"results": results,
	})
}

func (s *Server) handleBatchCacheClear(c *gin.Context) {
	var body struct {
		Tokens []string `json:"tokens"`
	}
	if err := readJSON(c, &body); err != nil {
		writeAppError(c, err)
		return
	}
	tokens := sanitizeTokenList(body.Tokens)
	if s.Refresh == nil {
		writeAppError(c, platform.NewAppError("refresh service not available", platform.ErrServer, "no_refresh", 503))
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Minute)
	defer cancel()
	results := map[string]any{}
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	conc := clampInt(parseIntQuery(c, "concurrency", 50), 1, 80)
	sem := make(chan struct{}, conc)
	for _, tok := range tokens {
		wg.Add(1)
		go func(t string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			deleted, err := s.clearTokenAssets(ctx, t)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				results[maskToken(t)] = gin.H{"error": err.Error()}
			} else {
				results[maskToken(t)] = gin.H{"deleted": deleted}
			}
		}(tok)
	}
	wg.Wait()
	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"results": results,
	})
}

// handleTokensRestore 恢复 expired/cooling/disabled 状态的账号为 active
func (s *Server) handleTokensRestore(c *gin.Context) {
	var body struct {
		Token string `json:"token"`
	}
	if err := readJSON(c, &body); err != nil {
		writeAppError(c, err)
		return
	}
	tok := platform.SanitizeToken(body.Token)
	if tok == "" {
		writeAppError(c, platform.NewAppError("token is required", platform.ErrValidation, "empty_token", 400))
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()
	active := account.StatusActive
	result, err := s.Repo.PatchAccounts(ctx, []account.Patch{
		{Token: tok, Status: &active, ClearFailures: true},
	})
	if err != nil {
		writeAppError(c, err)
		return
	}
	if result.Patched == 0 {
		writeAppError(c, platform.NewAppError("token not found", platform.ErrServer, "token_not_found", 404))
		return
	}
	// 同步内存 Directory 中的状态
	if s.Directory != nil {
		s.Directory.RestoreToken(tok)
	}
	c.JSON(http.StatusOK, gin.H{"status": "success", "token": tok})
}

func (s *Server) clearTokenAssets(ctx context.Context, token string) (int, error) {
	resp, err := grok.ListAssets(ctx, s.Transport, token)
	if err != nil {
		return 0, err
	}
	items := extractAssetItems(resp)
	deleted := 0
	for _, assetID := range items {
		if assetID == "" {
			continue
		}
		if _, err := grok.DeleteAsset(ctx, s.Transport, token, assetID); err == nil {
			deleted++
		}
	}
	return deleted, nil
}

// extractAssetItems returns the list of asset IDs from a ListAssets response.
func extractAssetItems(resp map[string]any) []string {
	raw, _ := resp["assets"].([]any)
	if raw == nil {
		raw, _ = resp["items"].([]any)
	}
	out := make([]string, 0, len(raw))
	for _, it := range raw {
		item, ok := it.(map[string]any)
		if !ok {
			continue
		}
		if id, _ := item["id"].(string); id != "" {
			out = append(out, id)
		} else if id, _ := item["assetId"].(string); id != "" {
			out = append(out, id)
		}
	}
	return out
}

// extractAssetRows returns a normalized list of asset rows from a ListAssets response.
func extractAssetRows(token string, resp map[string]any, err error) map[string]any {
	if err != nil {
		return map[string]any{
			"token":  token,
			"masked": maskToken(token),
			"count":  0,
			"assets": []any{},
			"error":  err.Error(),
		}
	}
	raw, _ := resp["assets"].([]any)
	if raw == nil {
		raw, _ = resp["items"].([]any)
	}
	rows := make([]map[string]any, 0, len(raw))
	for _, it := range raw {
		item, ok := it.(map[string]any)
		if !ok {
			continue
		}
		id, _ := item["id"].(string)
		if id == "" {
			id, _ = item["assetId"].(string)
		}
		name, _ := item["fileName"].(string)
		if name == "" {
			name, _ = item["name"].(string)
		}
		filePath, _ := item["filePath"].(string)
		if filePath == "" {
			filePath, _ = item["file_path"].(string)
		}
		contentType, _ := item["contentType"].(string)
		if contentType == "" {
			contentType, _ = item["content_type"].(string)
		}
		size := 0
		if v, ok := item["fileSize"].(float64); ok {
			size = int(v)
		} else if v, ok := item["size"].(float64); ok {
			size = int(v)
		}
		createdAt, _ := item["createdAt"].(string)
		if createdAt == "" {
			createdAt, _ = item["created_at"].(string)
		}
		rows = append(rows, map[string]any{
			"id":           id,
			"name":         name,
			"file_path":    filePath,
			"content_type": contentType,
			"size":         size,
			"created_at":   createdAt,
		})
	}
	return map[string]any{
		"token":  token,
		"masked": maskToken(token),
		"count":  len(rows),
		"assets": rows,
	}
}

// --- Assets ---

func (s *Server) handleAssetsList(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()
	page, err := s.Repo.ListAccounts(ctx, account.ListQuery{Page: 1, PageSize: 5000, IncludeDeleted: false})
	if err != nil {
		writeAppError(c, err)
		return
	}
	results := []map[string]any{}
	totalAssets := 0
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	sem := make(chan struct{}, 20)
	for _, rec := range page.Items {
		wg.Add(1)
		go func(t string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			resp, err := grok.ListAssets(ctx, s.Transport, t)
			row := extractAssetRows(t, resp, err)
			mu.Lock()
			defer mu.Unlock()
			results = append(results, row)
			if count, _ := row["count"].(int); count > 0 {
				totalAssets += count
			}
		}(rec.Token)
	}
	wg.Wait()
	c.JSON(http.StatusOK, gin.H{
		"tokens":       results,
		"total_assets": totalAssets,
	})
}

func (s *Server) handleAssetsDeleteItem(c *gin.Context) {
	var body struct {
		Token   string `json:"token"`
		AssetID string `json:"asset_id"`
	}
	if err := readJSON(c, &body); err != nil {
		writeAppError(c, err)
		return
	}
	token := platform.SanitizeToken(body.Token)
	if token == "" || body.AssetID == "" {
		writeAppError(c, platform.ValidationError("Missing token or asset_id", "body"))
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()
	if _, err := grok.DeleteAsset(ctx, s.Transport, token, body.AssetID); err != nil {
		writeAppError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func (s *Server) handleAssetsClearToken(c *gin.Context) {
	var body struct {
		Token string `json:"token"`
	}
	if err := readJSON(c, &body); err != nil {
		writeAppError(c, err)
		return
	}
	token := platform.SanitizeToken(body.Token)
	if token == "" {
		writeAppError(c, platform.ValidationError("Missing token", "token"))
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()
	deleted, err := s.clearTokenAssets(ctx, token)
	if err != nil {
		writeAppError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "success", "deleted": deleted})
}

// --- Cache ---

func (s *Server) handleCacheStats(c *gin.Context) {
	imgStats := cacheStatsFor(storage.MediaImage)
	vidStats := cacheStatsFor(storage.MediaVideo)
	c.JSON(http.StatusOK, gin.H{
		"local_image": imgStats,
		"local_video": vidStats,
	})
}

func cacheStatsFor(mediaType storage.MediaType) map[string]any {
	var dir string
	var err error
	if mediaType == storage.MediaImage {
		dir, err = storage.ImageFilesDir()
	} else {
		dir, err = storage.VideoFilesDir()
	}
	if err != nil {
		return gin.H{"count": 0, "size_bytes": 0, "error": err.Error()}
	}
	entries, _ := os.ReadDir(dir)
	count := 0
	var size int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		count++
		size += info.Size()
	}
	limitMB := config.Global().GetInt("cache.local."+string(mediaType)+"_max_mb", 0)
	limitBytes := int64(limitMB) * 1024 * 1024
	usageRatio := 0.0
	usagePercent := 0.0
	if limitBytes > 0 {
		usageRatio = float64(size) / float64(limitBytes)
		usagePercent = usageRatio * 100.0
	}
	return gin.H{
		"count":         count,
		"size_mb":       float64(size) / 1024.0 / 1024.0,
		"size_bytes":    size,
		"limit_mb":      limitMB,
		"limit_bytes":   limitBytes,
		"limited":       limitBytes > 0,
		"usage_ratio":   usageRatio,
		"usage_percent": usagePercent,
	}
}

func (s *Server) handleCacheList(c *gin.Context) {
	cacheType := c.Query("cache_type")
	if cacheType == "" {
		cacheType = c.Query("type")
	}
	if cacheType == "" {
		cacheType = "image"
	}
	page := parseIntQuery(c, "page", 1)
	pageSize := parseIntQuery(c, "page_size", 1000)
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 1000
	}
	mediaType := storage.MediaImage
	if cacheType == "video" {
		mediaType = storage.MediaVideo
	}
	var dir string
	var err error
	if mediaType == storage.MediaImage {
		dir, err = storage.ImageFilesDir()
	} else {
		dir, err = storage.VideoFilesDir()
	}
	if err != nil {
		writeAppError(c, err)
		return
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			c.JSON(http.StatusOK, gin.H{"status": "success", "total": 0, "page": page, "page_size": pageSize, "items": []any{}})
			return
		}
		writeAppError(c, err)
		return
	}
	type fileItem struct {
		name       string
		size       int64
		modifiedAt float64
	}
	items := []fileItem{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		items = append(items, fileItem{name: e.Name(), size: info.Size(), modifiedAt: float64(info.ModTime().UnixMilli())})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].modifiedAt > items[j].modifiedAt })
	total := len(items)
	start := (page - 1) * pageSize
	end := start + pageSize
	if start > total {
		start = total
	}
	if end > total {
		end = total
	}
	pageItems := items[start:end]
	out := []map[string]any{}
	for _, it := range pageItems {
		out = append(out, map[string]any{
			"name":        it.name,
			"size_bytes":  it.size,
			"modified_at": it.modifiedAt,
		})
	}
	c.JSON(http.StatusOK, gin.H{
		"status":    "success",
		"total":     total,
		"page":      page,
		"page_size": pageSize,
		"items":     out,
	})
}

func (s *Server) handleCacheClear(c *gin.Context) {
	var body struct {
		Type string `json:"type"`
	}
	_ = readJSON(c, &body)
	cacheType := body.Type
	if cacheType == "" {
		cacheType = "image"
	}
	mediaType := storage.MediaImage
	if cacheType == "video" {
		mediaType = storage.MediaVideo
	}
	removed, err := s.Media.Clear(mediaType)
	if err != nil {
		writeAppError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"result": gin.H{"removed": removed},
	})
}

func (s *Server) handleCacheItemDelete(c *gin.Context) {
	var body struct {
		Type string `json:"type"`
		Name string `json:"name"`
	}
	if err := readJSON(c, &body); err != nil {
		writeAppError(c, err)
		return
	}
	if body.Name == "" {
		writeAppError(c, platform.ValidationErrorCode("Missing file name", "name", "missing_file_name"))
		return
	}
	cacheType := body.Type
	if cacheType == "" {
		cacheType = "image"
	}
	mediaType := storage.MediaImage
	if cacheType == "video" {
		mediaType = storage.MediaVideo
	}
	ok, err := s.Media.Delete(mediaType, body.Name)
	if err != nil {
		writeAppError(c, platform.ValidationErrorCode(err.Error(), "name", "invalid_file_name"))
		return
	}
	if !ok {
		writeAppError(c, platform.ValidationErrorCode("File not found", "name", "file_not_found"))
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"result": gin.H{"deleted": body.Name},
	})
}

func (s *Server) handleCacheItemsDelete(c *gin.Context) {
	var body struct {
		Type  string   `json:"type"`
		Names []string `json:"names"`
	}
	if err := readJSON(c, &body); err != nil {
		writeAppError(c, err)
		return
	}
	clean := []string{}
	for _, n := range body.Names {
		n = strings.TrimSpace(n)
		if n != "" {
			clean = append(clean, n)
		}
	}
	if len(clean) == 0 {
		writeAppError(c, platform.ValidationErrorCode("Missing file names", "names", "missing_file_names"))
		return
	}
	cacheType := body.Type
	if cacheType == "" {
		cacheType = "image"
	}
	mediaType := storage.MediaImage
	if cacheType == "video" {
		mediaType = storage.MediaVideo
	}
	deleted := 0
	missing := 0
	for _, name := range clean {
		ok, err := s.Media.Delete(mediaType, name)
		if err != nil || !ok {
			missing++
			continue
		}
		deleted++
	}
	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"result": gin.H{"deleted": deleted, "missing": missing},
	})
}

// --- helpers ---

func sanitizeTokenList(raw []string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, t := range raw {
		t = platform.SanitizeToken(t)
		if t == "" || seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, t)
	}
	return out
}

func maskToken(t string) string {
	if len(t) <= 20 {
		return t
	}
	return t[:8] + "..." + t[len(t)-8:]
}

func parseIntQuery(c *gin.Context, key string, def int) int {
	v := c.Query(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return def
	}
	return n
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// --- Logs ---

// handleAdminLogs 返回日志文件列表和指定文件的尾部 N 行。
//
// 【修改说明】
// 修改背景：用户希望在管理面板直接查看运行日志，方便排查媒体下载等问题
// 解决问题：新增 GET /admin/api/logs?file=xxx&lines=300 接口
// 设计考虑：默认返回最新日志文件尾部 300 行；lines 上限 2000 防止响应过大
func (s *Server) handleAdminLogs(c *gin.Context) {
	logDir := platform.LogDir()
	entries, err := os.ReadDir(logDir)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"files": []any{}, "lines": []string{}, "error": "日志目录不存在: " + logDir})
		return
	}

	type fileInfo struct {
		Name string `json:"name"`
		Size int64  `json:"size"`
	}
	var files []fileInfo
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, fileInfo{Name: name, Size: info.Size()})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Name > files[j].Name })

	if len(files) == 0 {
		c.JSON(http.StatusOK, gin.H{"files": []any{}, "lines": []string{}, "current": ""})
		return
	}

	fileName := c.Query("file")
	if fileName == "" {
		fileName = files[0].Name
	}

	lines := clampInt(parseIntQuery(c, "lines", 300), 1, 2000)

	// 安全检查：只允许读取日志目录下的 .log 文件
	cleanName := filepath.Clean(fileName)
	if strings.Contains(cleanName, "..") || !strings.HasSuffix(cleanName, ".log") {
		writeAppError(c, platform.ValidationError("Invalid log file name", "file"))
		return
	}

	logPath := filepath.Join(logDir, cleanName)
	content, err := os.ReadFile(logPath)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"files": files, "lines": []string{}, "current": cleanName, "error": err.Error()})
		return
	}

	allLines := strings.Split(string(content), "\n")
	start := len(allLines) - lines
	if start < 0 {
		start = 0
	}
	tail := allLines[start:]

	c.JSON(http.StatusOK, gin.H{
		"files":   files,
		"current": cleanName,
		"lines":   tail,
		"total":   len(allLines),
	})
}

// _ unused imports to silence linter when paths evolve.
var (
	_ = filepath.Join
	_ = json.Marshal
	_ = fmt.Sprintf
	_ = grok.DefaultUserAgent
)
