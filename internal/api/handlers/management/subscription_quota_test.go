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
