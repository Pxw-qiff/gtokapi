package grok

// 【修改说明】
// 修改背景：aurora 原有 statsig 为纯本地计算，依赖内置 seed/HEX 对，Grok 更新算法后会失效。
// 解决问题：新增远程签名服务模式，通过配置 proxy.clearance.statsig_signer_url 切换，
// 将签名工作委托给外部服务（参考 chenyme/grok2api 的实现），自动跟进算法变化。
// 设计考虑：不替换原有本地计算，作为可选模式叠加，未配置时行为不变。
// 注意事项：签名服务为第三方，需自行评估信任风险。

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aurora-develop/grok2api/internal/config"
	"github.com/aurora-develop/grok2api/internal/logger"
	tlsclient "github.com/aurora-develop/grok2api/internal/tlsclient"
)

const (
	defaultStatsigSignerURL  = "https://grok.wodf.de/sign"
	remoteStatsigCacheTTL    = time.Hour
	remoteStatsigBodyLimit   = 4 << 20 // 4MB
	remoteStatsigRespLimit   = 4 << 10 // 4KB
	remoteStatsigTimeout     = 15 * time.Second
	remoteStatsigSignerTimeout = 12 * time.Second
)

// remoteStatsigCacheEntry 缓存一个签名结果
type remoteStatsigCacheEntry struct {
	value     string
	expiresAt time.Time
}

// remoteStatsigSigner 远程签名服务客户端，带缓存
type remoteStatsigSigner struct {
	mu      sync.Mutex
	entries map[string]remoteStatsigCacheEntry
}

var (
	remoteStatsigSignerInstance = &remoteStatsigSigner{
		entries: make(map[string]remoteStatsigCacheEntry),
	}

	// 复用 statsig_html.go 中的正则模式，提取 grok-site-verification meta 标签
	remoteStatsigMetaTagRE = regexp.MustCompile(`(?is)<meta\b[^>]*\bname=["']grok-site[—‐‑‒–−―]verification["'][^>]*>`)
	remoteStatsigContentRE = regexp.MustCompile(`(?is)\bcontent=["']([^"']+)["']`)
)

// statsigRemoteSign 通过远程签名服务获取 x-statsig-id
// 流程：1. 抓取 grok.com/index 获取 metaContent  2. POST 到签名服务  3. 返回签名
func statsigRemoteSign(pathname, method, signerURL string) (string, error) {
	if pathname == "" {
		pathname = "/rest/app-chat/conversations/new"
	}
	if method == "" {
		method = "POST"
	}

	// 1. 检查缓存
	cacheKey := signerURL + "\x00" + strings.ToUpper(method) + "\x00" + pathname
	if v, ok := remoteStatsigSignerInstance.cached(cacheKey); ok {
		return v, nil
	}

	// 2. 抓取 metaContent
	metaContent, err := fetchStatsigMetaContent(context.Background())
	if err != nil {
		logger.Warnf("远程签名：抓取 metaContent 失败: %v", err)
		return "", fmt.Errorf("远程签名：抓取 metaContent 失败: %w", err)
	}

	// 3. 请求签名服务
	signature, err := requestRemoteSignature(context.Background(), signerURL, method, pathname, metaContent)
	if err != nil {
		logger.Warnf("远程签名：签名服务请求失败 (url=%s, method=%s, path=%s): %v", signerURL, method, pathname, err)
		return "", fmt.Errorf("远程签名：签名服务请求失败: %w", err)
	}

	// 4. 写入缓存
	remoteStatsigSignerInstance.store(cacheKey, signature, time.Now().Add(remoteStatsigCacheTTL))
	logger.Infof("远程签名成功: method=%s, path=%s, url=%s", method, pathname, signerURL)
	return signature, nil
}

// fetchStatsigMetaContent 从 grok.com/index 抓取 grok-site-verification meta 标签的 content
func fetchStatsigMetaContent(ctx context.Context) (string, error) {
	cfg := config.Global()
	var opts []tlsclient.Option
	proxyURL := normalizeProxyURL(strings.TrimSpace(cfg.GetStr("proxy.egress.proxy_url", "")))
	if proxyURL != "" {
		opts = append(opts, tlsclient.WithProxy(proxyURL))
	}
	client := tlsclient.New(opts...)

	reqCtx, cancel := context.WithTimeout(ctx, remoteStatsigTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, "https://grok.com/index", nil)
	if err != nil {
		return "", err
	}
	profile := resolveProxyProfile()
	ua := profile.UserAgent
	if ua == "" {
		ua = DefaultUserAgent
	}
	req.Header = http.Header{}
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Encoding", "gzip, deflate, br, zstd")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("User-Agent", ua)
	if cookie := htmlCookieHeader(profile); cookie != "" {
		req.Header.Set("Cookie", cookie)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("grok.com/index 返回 %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, remoteStatsigBodyLimit+1))
	if err != nil {
		return "", err
	}
	if len(body) > remoteStatsigBodyLimit {
		return "", fmt.Errorf("grok.com/index 响应体过大")
	}

	// 提取 grok-site-verification meta 标签的 content
	tag := remoteStatsigMetaTagRE.Find(body)
	if len(tag) == 0 {
		return "", fmt.Errorf("grok.com/index 未找到 grok-site-verification meta 标签")
	}
	m := remoteStatsigContentRE.FindSubmatch(tag)
	if len(m) < 2 {
		return "", fmt.Errorf("grok.com/index meta 标签无 content 属性")
	}
	return strings.TrimSpace(string(m[1])), nil
}

// requestRemoteSignature 向签名服务发送 POST 请求获取 x-statsig-id
func requestRemoteSignature(ctx context.Context, signerURL, method, path, metaContent string) (string, error) {
	payload, _ := json.Marshal(map[string]any{
		"method": strings.ToUpper(strings.TrimSpace(method)),
		"path":   path,
		"environment": map[string]string{
			"metaContent": metaContent,
		},
	})

	reqCtx, cancel := context.WithTimeout(ctx, remoteStatsigSignerTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, signerURL, bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout: remoteStatsigSignerTimeout,
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, remoteStatsigRespLimit+1))
	if err != nil {
		return "", err
	}
	if len(body) > remoteStatsigRespLimit {
		return "", fmt.Errorf("签名服务响应超过安全上限")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("签名服务返回 %d", resp.StatusCode)
	}

	var result struct {
		StatsigID string `json:"x-statsig-id"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("签名服务响应解析失败: %w", err)
	}
	if strings.TrimSpace(result.StatsigID) == "" {
		return "", fmt.Errorf("签名服务返回空 x-statsig-id")
	}
	return result.StatsigID, nil
}

// cached 从缓存中读取未过期的签名
func (s *remoteStatsigSigner) cached(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[key]
	if !ok || entry.value == "" || time.Now().After(entry.expiresAt) {
		return "", false
	}
	return entry.value, true
}

// store 写入签名到缓存，同时清理过期条目
func (s *remoteStatsigSigner) store(key, value string, expiresAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for k, e := range s.entries {
		if now.After(e.expiresAt) {
			delete(s.entries, k)
		}
	}
	s.entries[key] = remoteStatsigCacheEntry{value: value, expiresAt: expiresAt}
}

// InvalidateRemoteStatsigCache 清除所有远程签名缓存，用于 403 anti-bot 时强制重新获取签名
func InvalidateRemoteStatsigCache() {
	remoteStatsigSignerInstance.mu.Lock()
	defer remoteStatsigSignerInstance.mu.Unlock()
	remoteStatsigSignerInstance.entries = make(map[string]remoteStatsigCacheEntry)
	logger.Infof("远程签名缓存已清除")
}