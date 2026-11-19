package helps

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

// SubscriptionQuotaError contains no upstream body or credential material.
type SubscriptionQuotaError struct {
	Status     int
	RetryAfter time.Duration
}

// SubscriptionQuotaAccountID pins a ChatGPT usage read to the stored account.
func SubscriptionQuotaAccountID(a *coreauth.Auth) string {
	if a == nil {
		return ""
	}
	for _, key := range []string{"account_id", "chatgpt_account_id"} {
		if value, _ := a.Metadata[key].(string); strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	if id, _ := a.Metadata["id_token"].(string); id != "" {
		if claims, err := codex.ParseJWTToken(id); err == nil {
			return claims.CodexAuthInfo.ChatgptAccountID
		}
	}
	return ""
}

func (e *SubscriptionQuotaError) Error() string { return "subscription quota read failed" }

// FetchSubscriptionQuota is deliberately GET-only. It cannot spend reset credits
// or generate tokens, and never follows a redirect with subscription credentials.
func FetchSubscriptionQuota(ctx context.Context, cfg *config.Config, a *coreauth.Auth) ([]byte, error) {
	if a == nil || (a.Provider != "claude" && a.Provider != "codex") || a.Disabled || a.Status == coreauth.StatusDisabled || a.RequiresSignIn || strings.TrimSpace(a.Attributes["api_key"]) != "" || strings.TrimSpace(a.Attributes["base_url"]) != "" {
		return nil, errors.New("subscription quota unsupported")
	}
	token, _ := a.Metadata["access_token"].(string)
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("subscription credential unavailable")
	}
	target := "https://api.anthropic.com/api/oauth/usage?skip_spend=1"
	if a.Provider == "codex" {
		target = "https://chatgpt.com/backend-api/wham/usage"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Cache-Control", "no-cache")
	client := NewProxyAwareHTTPClient(ctx, cfg, a, 0)
	if a.Provider == "claude" {
		req.Header.Set("User-Agent", "claude-cli/2.1.280 (external, cli)")
		req.Header.Set("Anthropic-Beta", "oauth-2025-04-20")
		client = NewUtlsHTTPClient(ctx, cfg, a, 0)
	} else {
		req.Header.Set("User-Agent", "codex-cli")
		account := SubscriptionQuotaAccountID(a)
		if account != "" {
			req.Header.Set("Chatgpt-Account-Id", account)
		}
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.New("subscription quota connection failed")
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.Debug("subscription quota response close failed")
		}
	}()
	if resp.StatusCode != http.StatusOK {
		var retry time.Duration
		if seconds, e := strconv.ParseInt(resp.Header.Get("Retry-After"), 10, 64); e == nil && seconds > 0 && seconds <= 86400 {
			retry = time.Duration(seconds) * time.Second
		}
		return nil, &SubscriptionQuotaError{Status: resp.StatusCode, RetryAfter: retry}
	}
	const maxBytes = 1024 * 1024
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil || len(body) > maxBytes {
		return nil, errors.New("subscription quota response unreadable")
	}
	return body, nil
}
