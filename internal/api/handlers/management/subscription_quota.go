package management

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func trustedSubscriptionUsageHeaders(a *coreauth.Auth, headers map[string]string) bool {
	if a == nil {
		return false
	}
	bearers := 0
	account := ""
	accountHeaders := 0
	for name, value := range headers {
		switch strings.ToLower(name) {
		case "authorization":
			if value != "Bearer $TOKEN$" {
				return false
			}
			bearers++
		case "chatgpt-account-id":
			accountHeaders++
			account = value
		case "accept", "content-type", "user-agent", "anthropic-beta":
		default:
			return false
		}
	}
	if bearers != 1 {
		return false
	}
	if a.Provider == "codex" {
		expected := helps.SubscriptionQuotaAccountID(a)
		return expected != "" && accountHeaders == 1 && account == expected
	}
	return account == ""
}

func trustedSubscriptionUsageRequest(provider, method, target string) bool {
	if method != http.MethodGet {
		return false
	}
	u, err := url.Parse(target)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Fragment != "" {
		return false
	}
	if provider == "claude" {
		if u.Host != "api.anthropic.com" || u.Path != "/api/oauth/usage" {
			return false
		}
		q, errQuery := url.ParseQuery(u.RawQuery)
		if errQuery != nil {
			return false
		}
		for key, values := range q {
			if key != "cedar_ember" && key != "skip_spend" || len(values) != 1 || values[0] != "1" {
				return false
			}
		}
		return true
	}
	return provider == "codex" && u.Host == "chatgpt.com" && u.Path == "/backend-api/wham/usage" && u.RawQuery == ""
}

// RefreshSubscriptionQuota checks provider usage without spending a reset. A
// remaining exhausted window or an unrelated error retains its routing block.
func (h *Handler) RefreshSubscriptionQuota(c *gin.Context) {
	if h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}
	var req struct {
		AuthIndex string `json:"auth_index"`
	}
	if c.ShouldBindJSON(&req) != nil || strings.TrimSpace(req.AuthIndex) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "auth_index is required"})
		return
	}
	if h.authManager.HomeEnabled() {
		// A confirmed provider reset must not be reported as failed merely
		// because Home, rather than this process, owns routing cooldowns.
		c.JSON(http.StatusOK, gin.H{"status": "ok", "auth_index": strings.TrimSpace(req.AuthIndex), "recovered": false, "managed_by": "home"})
		return
	}
	a := h.authByIndex(strings.TrimSpace(req.AuthIndex))
	if a == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "auth not found"})
		return
	}
	if a.Disabled || a.RequiresSignIn || a.Attributes["api_key"] != "" || a.Attributes["base_url"] != "" || a.Provider != "claude" && a.Provider != "codex" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "subscription quota unavailable for this credential"})
		return
	}
	body, errFetch := helps.FetchSubscriptionQuota(c.Request.Context(), h.cfg, a)
	if errFetch != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "provider quota could not be refreshed"})
		return
	}
	recovered, errReconcile := h.authManager.ReconcileSubscriptionQuota(c.Request.Context(), a, body)
	if errReconcile != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "quota recovery could not be saved"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "auth_index": a.EnsureIndex(), "recovered": recovered})
}
