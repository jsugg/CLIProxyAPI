package helps

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

type subscriptionTransport func(*http.Request) (*http.Response, error)

func (f subscriptionTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestFetchSubscriptionQuotaOnlyReadsUsage(t *testing.T) {
	for _, provider := range []string{"claude", "codex"} {
		t.Run(provider, func(t *testing.T) {
			a := &auth.Auth{ID: "test", Provider: provider, Metadata: map[string]any{"access_token": "test-bearer", "account_id": "account-test"}}
			calls := 0
			rt := subscriptionTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				want := "https://api.anthropic.com/api/oauth/usage?skip_spend=1"
				if provider == "codex" {
					want = "https://chatgpt.com/backend-api/wham/usage"
					if r.Header.Get("Chatgpt-Account-Id") != "account-test" {
						t.Fatal("account missing")
					}
				}
				if r.Method != "GET" || r.URL.String() != want || r.Body != nil {
					t.Fatalf("unexpected request %s %s", r.Method, r.URL)
				}
				if r.Header.Get("Authorization") != "Bearer test-bearer" {
					t.Fatal("bearer missing")
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"usage":"fixture"}`)), Header: make(http.Header)}, nil
			})
			ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", http.RoundTripper(rt))
			body, err := FetchSubscriptionQuota(ctx, &config.Config{}, a)
			if err != nil || string(body) != `{"usage":"fixture"}` || calls != 1 {
				t.Fatalf("body=%s calls=%d err=%v", body, calls, err)
			}
		})
	}
}

func TestFetchSubscriptionQuotaRefusesUnsafeCredentials(t *testing.T) {
	for _, a := range []*auth.Auth{nil, {Provider: "gemini"}, {Provider: "claude", Metadata: map[string]any{"access_token": "test"}, Attributes: map[string]string{"base_url": "https://custom.invalid"}}, {Provider: "claude", Metadata: map[string]any{"access_token": "test"}, Attributes: map[string]string{"api_key": "key"}}} {
		if _, err := FetchSubscriptionQuota(context.Background(), &config.Config{}, a); err == nil {
			t.Fatal("unsupported credential accepted")
		}
	}
}

func TestFetchSubscriptionQuotaRejectsErrorsAndOversize(t *testing.T) {
	for _, status := range []int{401, 429, 500, 200} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			rt := subscriptionTransport(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(strings.Repeat("x", 1024*1024+1))), Header: make(http.Header)}, nil
			})
			ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", http.RoundTripper(rt))
			if _, err := FetchSubscriptionQuota(ctx, &config.Config{}, &auth.Auth{Provider: "claude", Metadata: map[string]any{"access_token": "test"}}); err == nil {
				t.Fatal("bad response accepted")
			}
		})
	}
}
