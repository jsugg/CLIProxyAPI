package management

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

type subscriptionTransport func(*http.Request) (*http.Response, error)

func (f subscriptionTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRefreshSubscriptionQuota(t *testing.T) {
	for _, used := range []int{0, 50, 100} {
		t.Run(fmt.Sprint(used), func(t *testing.T) {
			m := coreauth.NewManager(nil, nil, nil)
			a := &coreauth.Auth{ID: t.Name(), Provider: "claude", Status: coreauth.StatusActive, Metadata: map[string]any{"access_token": "fixture"}}
			idx := a.EnsureIndex()
			_, _ = m.Register(context.Background(), a)
			retry := 72 * time.Hour
			m.MarkResult(context.Background(), coreauth.Result{AuthID: a.ID, Provider: "claude", Model: "claude-opus-5-5", CredentialScope: true, RetryAfter: &retry, Error: &coreauth.Error{HTTPStatus: 429}})
			rt := subscriptionTransport(func(r *http.Request) (*http.Response, error) {
				if r.Method != "GET" || r.URL.Path != "/api/oauth/usage" {
					t.Fatalf("unexpected request %s %s", r.Method, r.URL)
				}
				body := fmt.Sprintf(`{"five_hour":null,"seven_day":{"utilization":%d,"resets_at":%q}}`, used, time.Now().Add(retry).Format(time.RFC3339))
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
			})
			h := NewHandlerWithoutConfigFilePath(&config.Config{}, m)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest("POST", "/v8/management/routing/cooldown/refresh", strings.NewReader(`{"auth_index":"`+idx+`"}`))
			c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), "cliproxy.roundtripper", http.RoundTripper(rt)))
			c.Request.Header.Set("Content-Type", "application/json")
			h.RefreshSubscriptionQuota(c)
			if rec.Code != 200 {
				t.Fatalf("status=%d %s", rec.Code, rec.Body.String())
			}
			stored, _ := m.GetByID(a.ID)
			if stored.Quota.Exceeded != (used == 100) {
				t.Fatalf("quota=%+v", stored.Quota)
			}
			if strings.Contains(rec.Body.String(), "fixture") {
				t.Fatal("credential leaked")
			}
		})
	}
}

func TestSubscriptionAPICallRequiresTrustedRequest(t *testing.T) {
	for _, tc := range []struct {
		method, target string
		want           bool
	}{
		{"GET", "https://api.anthropic.com/api/oauth/usage", true},
		{"GET", "https://api.anthropic.com/api/oauth/usage?cedar_ember=1&skip_spend=1", true},
		{"POST", "https://api.anthropic.com/api/oauth/usage", false},
		{"GET", "http://api.anthropic.com/api/oauth/usage", false},
		{"GET", "https://api.anthropic.com.evil.test/api/oauth/usage", false},
		{"GET", "https://api.anthropic.com/api/oauth/usage?at_wall=1", false},
		{"GET", "https://chatgpt.com/backend-api/wham/usage", false},
	} {
		t.Run(tc.target+tc.method, func(t *testing.T) {
			if got := trustedSubscriptionUsageRequest("claude", tc.method, tc.target); got != tc.want {
				t.Fatalf("trusted=%v want %v", got, tc.want)
			}
		})
	}
}

func TestSubscriptionUsageHeadersBindCredential(t *testing.T) {
	a := &coreauth.Auth{Provider: "codex", Metadata: map[string]any{"account_id": "expected"}}
	for _, tc := range []struct {
		header map[string]string
		want   bool
	}{
		{map[string]string{"Authorization": "Bearer $TOKEN$", "Chatgpt-Account-Id": "expected"}, true},
		{map[string]string{"Authorization": "Bearer $TOKEN$", "Chatgpt-Account-Id": "other"}, false},
		{map[string]string{"Authorization": "Bearer $TOKEN$"}, false},
		{map[string]string{"Authorization": "Bearer $TOKEN$", "Chatgpt-Account-Id": "expected", "Host": "other"}, false},
		{map[string]string{"Authorization": "Bearer $TOKEN$", "authorization": "Bearer other", "Chatgpt-Account-Id": "expected"}, false},
		{map[string]string{"Authorization": "Bearer other", "Chatgpt-Account-Id": "expected"}, false},
		{map[string]string{"Authorization": "Bearer $TOKEN$", "Chatgpt-Account-Id": "expected", "chatgpt-account-id": "other"}, false},
	} {
		if got := trustedSubscriptionUsageHeaders(a, tc.header); got != tc.want {
			t.Fatalf("trusted=%v want %v", got, tc.want)
		}
	}
}

func TestRefreshSubscriptionQuotaDefersToHome(t *testing.T) {
	m := coreauth.NewManager(nil, nil, nil)
	cfg := &config.Config{}
	cfg.Home.Enabled = true
	m.SetConfig(cfg)
	h := NewHandlerWithoutConfigFilePath(cfg, m)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v8/management/routing/cooldown/refresh", strings.NewReader(`{"auth_index":"remote-index"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	h.RefreshSubscriptionQuota(c)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"managed_by":"home"`) || !strings.Contains(rec.Body.String(), `"recovered":false`) {
		t.Fatalf("home ownership acknowledgement: %d %s", rec.Code, rec.Body.String())
	}
}

func TestCopilotSubscriptionQuotaManagementAndTrustedAPICall(t *testing.T) {
	if !trustedSubscriptionUsageRequest("copilot", http.MethodGet, "https://api.github.com/copilot_internal/user") {
		t.Fatal("expected copilot_internal/user GET to be trusted")
	}
	if trustedSubscriptionUsageRequest("copilot", http.MethodPost, "https://api.github.com/copilot_internal/user") {
		t.Fatal("POST must not be trusted")
	}
	if trustedSubscriptionUsageRequest("copilot", http.MethodGet, "https://api.github.com.evil.test/copilot_internal/user") {
		t.Fatal("untrusted host accepted")
	}

	a := &coreauth.Auth{ID: t.Name(), Provider: "copilot", Status: coreauth.StatusActive, Metadata: map[string]any{"refresh_token": "gho-secret-fixture"}}
	if !trustedSubscriptionUsageHeaders(a, map[string]string{"Authorization": "Bearer gho-secret-fixture", "Accept": "application/json", "User-Agent": "copilot/1.0.114"}) {
		t.Fatal("matching Copilot refresh_token header rejected")
	}
	if trustedSubscriptionUsageHeaders(a, map[string]string{"Authorization": "Bearer wrong-token"}) {
		t.Fatal("mismatched Copilot token accepted")
	}

	m := coreauth.NewManager(nil, nil, nil)
	idx := a.EnsureIndex()
	_, _ = m.Register(context.Background(), a)
	reset := time.Now().Add(48 * time.Hour).UTC().Format(time.RFC3339)
	exhausted := true
	rt := subscriptionTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.String() != "https://api.github.com/copilot_internal/user" || r.Header.Get("Authorization") != "Bearer gho-secret-fixture" {
			t.Fatalf("unexpected Copilot quota request: %s %v", r.URL, r.Header)
		}
		body := fmt.Sprintf(`{"quota_reset_date_utc":%q,"quota_snapshots":{"premium_interactions":{"unlimited":false,"overage_permitted":false,"has_quota":false,"percent_remaining":0.0,"remaining":0,"entitlement":50000}}}`, reset)
		if !exhausted {
			body = fmt.Sprintf(`{"quota_reset_date_utc":%q,"quota_snapshots":{"premium_interactions":{"unlimited":true,"overage_permitted":true,"has_quota":true,"percent_remaining":100.0}}}`, reset)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})
	h := NewHandlerWithoutConfigFilePath(&config.Config{}, m)

	invoke := func() *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v8/management/routing/cooldown/refresh", strings.NewReader(`{"auth_index":"`+idx+`"}`))
		c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), "cliproxy.roundtripper", http.RoundTripper(rt)))
		c.Request.Header.Set("Content-Type", "application/json")
		h.RefreshSubscriptionQuota(c)
		return rec
	}

	rec := invoke()
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"recovered":false`) {
		t.Fatalf("exhausted refresh = %d %s", rec.Code, rec.Body.String())
	}
	stored, _ := m.GetByID(a.ID)
	if !stored.Quota.Exceeded || stored.Quota.Reason != "credential_quota" {
		t.Fatalf("exhausted refresh did not cool Copilot seat: %+v", stored.Quota)
	}

	exhausted = false
	rec = invoke()
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"recovered":true`) {
		t.Fatalf("restored refresh = %d %s", rec.Code, rec.Body.String())
	}
	stored, _ = m.GetByID(a.ID)
	if stored.Quota.Exceeded || stored.Unavailable {
		t.Fatalf("restored refresh did not release Copilot seat: unavailable=%v quota=%+v", stored.Unavailable, stored.Quota)
	}
}
