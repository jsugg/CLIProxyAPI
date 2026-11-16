package quotahistory

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MoeForge/moego/json"
	bolt "go.etcd.io/bbolt"
)

func TestCollectorConcurrencyBackoffAndCancellation(t *testing.T) {
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	var active, peak, calls atomic.Int32
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v8/management/credentials" {
			files := make([]map[string]any, 20)
			for i := range files {
				files[i] = map[string]any{"provider": "codex", "auth_index": fmt.Sprint("idx", i), "disabled": false}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"files": files})
			return
		}
		count := active.Add(1)
		defer active.Add(-1)
		for {
			old := peak.Load()
			if old >= count || peak.CompareAndSwap(old, count) {
				break
			}
		}
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(probeResponse{StatusCode: 429, Header: map[string][]string{"Retry-After": {"36000"}}, Body: `{"error":"sensitive"}`})
	}))
	defer native.Close()
	s := openTestStore(t)
	c, err := NewCollector(s, native.URL, "secret", Options{Now: func() time.Time { return now }, Jitter: func(time.Duration) time.Duration { return 0 }})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Poll(context.Background()); err == nil {
		t.Fatal("expected failure")
	}
	if peak.Load() > 2 || calls.Load() != 20 {
		t.Fatalf("peak %d calls %d", peak.Load(), calls.Load())
	}
	if err = c.Poll(context.Background()); err == nil {
		t.Fatal("backoff must retain failure")
	}
	if calls.Load() != 20 {
		t.Fatal("ignored Retry-After")
	}
	if len(c.retries) != 20 {
		t.Fatal("lost backoff state")
	}
	now = now.Add(time.Hour)
	if err = c.Poll(context.Background()); err == nil {
		t.Fatal("expected failure")
	}
	if calls.Load() != 40 {
		t.Fatal("Retry-After not bounded")
	}
}

func TestCollectorRedirectDoesNotForwardKey(t *testing.T) {
	var leaked atomic.Bool
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Store(true) }))
	defer sink.Close()
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, sink.URL, 302) }))
	defer native.Close()
	c, err := NewCollector(openTestStore(t), native.URL, "secret", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Poll(context.Background()); err == nil {
		t.Fatal("redirect accepted")
	}
	if leaked.Load() {
		t.Fatal("redirect followed")
	}
}

func TestIdentityMappingSurvivesProfileFailureAndRestart(t *testing.T) {
	s := openTestStore(t)
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	fail := false
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v8/management/credentials" {
			_, _ = w.Write([]byte(`{"files":[{"provider":"claude","auth_index":"idx","disabled":false}]}`))
			return
		}
		var req probeRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if fail {
			_ = json.NewEncoder(w).Encode(probeResponse{StatusCode: 503})
			return
		}
		if req.URL == claudeProfileURL {
			_ = json.NewEncoder(w).Encode(probeResponse{StatusCode: 200, Body: `{"account":{"uuid":"account-uuid"},"organization":{"uuid":"org-uuid","rate_limit_tier":"default_claude_max_5x"}}`})
			return
		}
		_ = json.NewEncoder(w).Encode(probeResponse{StatusCode: 200, Body: `{"five_hour":{"utilization":25,"resets_at":"2026-11-16T15:00:00Z"}}`})
	}))
	defer native.Close()
	c, err := NewCollector(s, native.URL, "secret", Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	p, err := s.Query(Query{From: now.Add(-time.Hour), To: now.Add(time.Hour), Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	original := p.Samples[0].AccountID
	fail = true
	now = now.Add(PollInterval)
	// A fresh collector must reuse the persisted auth-index to opaque account mapping.
	c, err = NewCollector(s, native.URL, "secret", Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Poll(context.Background()); err == nil {
		t.Fatal("failure not surfaced")
	}
	p, err = s.Query(Query{From: now.Add(-time.Hour), To: now.Add(time.Hour), Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Samples) != 2 || p.Samples[1].AccountID != original || p.Samples[1].IdentityQuality != "provider" || p.Samples[1].State != "unknown" {
		t.Fatalf("lost identity %#v", p.Samples)
	}
}

func TestReadKeyRejectsUnsafeFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "key")
	if err := os.WriteFile(path, []byte("admin-key\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if key, err := ReadKey(path); err != nil || key != "admin-key" {
		t.Fatalf("read %q %v", key, err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadKey(link); err == nil {
		t.Fatal("symlink accepted")
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadKey(path); err == nil {
		t.Fatal("public key file accepted")
	}
}

func TestRunImmediateAndInjectedWait(t *testing.T) {
	calls := 0
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; _, _ = w.Write([]byte(`{"files":[]}`)) }))
	defer native.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waits := 0
	c, err := NewCollector(openTestStore(t), native.URL, "secret", Options{Wait: func(ctx context.Context, d time.Duration) error {
		waits++
		if d != PollInterval {
			t.Error("wrong interval")
		}
		cancel()
		return ctx.Err()
	}})
	if err != nil {
		t.Fatal(err)
	}
	c.Run(ctx, nil)
	if calls != 1 || waits != 1 {
		t.Fatalf("polls %d waits %d", calls, waits)
	}
}

func TestCredentialEnumerationLimits(t *testing.T) {
	for _, tc := range []struct{ name, body string }{{"malformed", `{"files":null}`}, {"invalid-index", `{"files":[{"provider":"codex","auth_index":"person@example.com","disabled":false}]}`}, {"oversized", strings.Repeat("x", maxResponseBytes+1)}} {
		t.Run(tc.name, func(t *testing.T) {
			native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(tc.body)) }))
			defer native.Close()
			c, err := NewCollector(openTestStore(t), native.URL, "secret", Options{})
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Poll(context.Background()); err == nil {
				t.Fatal("accepted bad enumeration")
			}
		})
	}
}

func FuzzProviderPayload(f *testing.F) {
	for _, raw := range []string{`null`, `{}`, `{"rate_limit":{"primary_window":{"used_percent":"NaN"}}}`, `{"five_hour":{"utilization":0}}`} {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > 1<<20 {
			t.Skip()
		}
		for _, provider := range []string{"claude", "codex"} {
			rows := parseUsage(credential{Provider: provider, AuthIndex: "idx"}, []byte(raw), time.Unix(1794844800, 0))
			if len(rows) == 0 || len(rows) > 64 {
				t.Fatal("unbounded or empty")
			}
			for _, row := range rows {
				if row.UsedPercent != nil && (*row.UsedPercent < 0 || *row.UsedPercent > 100) {
					t.Fatal("invalid percentage")
				}
				if row.State == "unknown" && row.UsedPercent != nil {
					t.Fatal("unknown has percentage")
				}
			}
		}
	})
}

func TestCodexPlanChangeSplitsSeries(t *testing.T) {
	s := openTestStore(t)
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	plan := "pro"
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v8/management/credentials" {
			_, _ = w.Write([]byte(`{"files":[{"provider":"codex","auth_index":"idx","disabled":false}]}`))
			return
		}
		_ = json.NewEncoder(w).Encode(probeResponse{StatusCode: 200, Body: fmt.Sprintf(`{"plan_type":%q,"rate_limit":{"primary_window":{"used_percent":25,"limit_window_seconds":18000,"reset_at":1794844800}}}`, plan)})
	}))
	defer native.Close()
	c, err := NewCollector(s, native.URL, "secret", Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	plan = "team"
	now = now.Add(PollInterval)
	if err = c.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	p, err := s.Query(Query{From: now.Add(-time.Hour), To: now.Add(time.Hour), Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Samples) != 2 || p.Samples[0].SeriesID == p.Samples[1].SeriesID {
		t.Fatalf("plan change not split %#v", p.Samples)
	}
}

func TestEnumerationFailurePersistsAccountWideGap(t *testing.T) {
	s := openTestStore(t)
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	fail := false
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v8/management/credentials" {
			if fail {
				w.WriteHeader(503)
				return
			}
			_, _ = w.Write([]byte(`{"files":[{"provider":"codex","auth_index":"idx","disabled":false}]}`))
			return
		}
		_ = json.NewEncoder(w).Encode(probeResponse{StatusCode: 200, Body: `{"plan_type":"pro","rate_limit":{"primary_window":{"used_percent":25,"limit_window_seconds":18000,"reset_at":1794844800}}}`})
	}))
	defer native.Close()
	c, err := NewCollector(s, native.URL, "secret", Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	fail = true
	now = now.Add(PollInterval)
	if err = c.Poll(context.Background()); err == nil {
		t.Fatal("failure not surfaced")
	}
	fail = false
	now = now.Add(PollInterval)
	if err = c.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	p, err := s.Query(Query{From: now.Add(-time.Hour), To: now.Add(time.Hour), Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Samples) != 3 || p.Samples[1].LimitID != "unknown" || p.Samples[1].State != "unknown" || p.Samples[1].AccountID != p.Samples[0].AccountID {
		t.Fatalf("gap not persisted %#v", p.Samples)
	}
}

func TestDisappearingLimitPersistsUnknown(t *testing.T) {
	s := openTestStore(t)
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	missing := false
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v8/management/credentials" {
			_, _ = w.Write([]byte(`{"files":[{"provider":"codex","auth_index":"idx","disabled":false}]}`))
			return
		}
		body := `{"rate_limit":{"primary_window":{"used_percent":25,"limit_window_seconds":18000},"secondary_window":{"used_percent":10,"limit_window_seconds":604800}}}`
		if missing {
			body = `{"rate_limit":{"secondary_window":{"used_percent":11,"limit_window_seconds":604800}}}`
		}
		_ = json.NewEncoder(w).Encode(probeResponse{StatusCode: 200, Body: body})
	}))
	defer native.Close()
	c, err := NewCollector(s, native.URL, "secret", Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	missing = true
	now = now.Add(PollInterval)
	if err = c.Poll(context.Background()); err == nil {
		t.Fatal("disappeared usage must mark cycle incomplete")
	}
	p, err := s.Query(Query{From: now.Add(-time.Second), To: now.Add(time.Second), Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range p.Samples {
		if row.LimitID == "five-hour" && row.State == "unknown" && row.UsedPercent == nil {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing window silently omitted %#v", p.Samples)
	}
}

func TestStoreReopenAndUnsafePaths(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	id := s.ID("provider", "account")
	if s.db.MaxSize != maxDBSize {
		t.Fatal("database file unbounded")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if s.ID("provider", "account") != id {
		t.Fatal("identity secret changed on reopen")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "link")
	if err = os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	if _, err = Open(link); err == nil {
		t.Fatal("state symlink accepted")
	}
	if err = os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err = Open(dir); err == nil {
		t.Fatal("public state accepted")
	}
}

func TestRetryAfterBoundaries(t *testing.T) {
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		raw  string
		want time.Duration
	}{{"", 0}, {"invalid", 0}, {"-1", 0}, {"10", 10 * time.Second}, {"9999999999", time.Hour}, {now.Add(2 * time.Hour).Format(http.TimeFormat), time.Hour}, {now.Add(-time.Second).Format(http.TimeFormat), 0}} {
		if got := retryAfter(tc.raw, now); got != tc.want {
			t.Errorf("%q=%v want %v", tc.raw, got, tc.want)
		}
	}
}

func TestExtendedProviderWindows(t *testing.T) {
	now := time.Unix(1794844800, 0)
	rows := parseUsage(credential{Provider: "claude"}, []byte(`{"iguana_necktie":{"utilization":40,"limit_dollars":100},"limits":[{"kind":"weekly_scoped","scope":{"model":{"display_name":"Fable"}},"percent":30,"is_active":true}]}`), now)
	if len(rows) != 2 || rows[0].LimitID != "cloud-session-credits" || rows[0].PeriodSeconds != nil || rows[1].LimitID != "seven-day-fable" {
		t.Fatalf("claude extras %#v", rows)
	}
	rows = parseUsage(credential{Provider: "codex"}, []byte(`{"additional_rate_limits":[{"limit_name":"sensitive label person@example.com","rateLimit":{"primaryWindow":{"usedPercent":0,"limitWindowSeconds":7200}}}]}`), now)
	if len(rows) != 1 || !strings.HasPrefix(rows[0].LimitID, "additional-") || strings.Contains(rows[0].LimitID, "person") || rows[0].UsedPercent == nil || *rows[0].UsedPercent != 0 {
		t.Fatalf("additional windows %#v", rows)
	}
	rows = parseUsage(credential{Provider: "claude"}, []byte(`{"five_hour":null,"seven_day":{"utilization":10}}`), now)
	if len(rows) != 1 || rows[0].LimitID != "seven-day" || rows[0].State != "ok" {
		t.Fatalf("inapplicable null window was not skipped: %#v", rows)
	}
}

func TestInapplicableNullWindowsDoNotFailOrBackoffCollection(t *testing.T) {
	for _, tc := range []struct {
		name, provider, body string
		windows              int
	}{
		{"claude", "claude", `{"five_hour":{"utilization":25},"seven_day":{"utilization":10},"seven_day_oauth_apps":null,"seven_day_opus":null,"seven_day_sonnet":null,"seven_day_cowork":null,"iguana_necktie":null}`, 2},
		{"codex-review-null", "codex", `{"rate_limit":{"primary_window":{"used_percent":25,"limit_window_seconds":18000},"secondary_window":null},"code_review_rate_limit":null}`, 1},
		{"codex-primary-null", "codex", `{"rate_limit":{"primary_window":null,"secondary_window":{"used_percent":10,"limit_window_seconds":604800}},"code_review_rate_limit":{"primary_window":null,"secondary_window":null}}`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openTestStore(t)
			now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
			var probes atomic.Int32
			native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v8/management/credentials" {
					_ = json.NewEncoder(w).Encode(map[string]any{"files": []map[string]any{{"provider": tc.provider, "auth_index": "idx", "disabled": false}}})
					return
				}
				var req probeRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
					return
				}
				if req.URL == claudeProfileURL {
					_ = json.NewEncoder(w).Encode(probeResponse{StatusCode: 200, Body: `{"account":{"uuid":"account-uuid"},"organization":{"uuid":"org-uuid","rate_limit_tier":"default_claude_max_5x"}}`})
					return
				}
				probes.Add(1)
				_ = json.NewEncoder(w).Encode(probeResponse{StatusCode: 200, Body: tc.body})
			}))
			defer native.Close()
			c, err := NewCollector(s, native.URL, "secret", Options{Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err = c.Poll(context.Background()); err != nil {
					t.Fatalf("valid quota with null slots failed: %v", err)
				}
				now = now.Add(PollInterval)
			}
			if probes.Load() != 2 || len(c.retries) != 0 {
				t.Fatalf("normal null slots triggered backoff: probes=%d retries=%d", probes.Load(), len(c.retries))
			}
			status := c.Status()
			if status.LastError != nil || status.ConsecutiveFailures != 0 || status.LastSuccessAt == nil || status.Stale {
				t.Fatalf("healthy quota marked failed: %#v", status)
			}
			page, err := s.Query(Query{From: now.Add(-time.Hour), To: now, Limit: 20})
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Samples) != tc.windows*2 {
				t.Fatalf("inapplicable slots persisted: %#v", page.Samples)
			}
			for _, row := range page.Samples {
				if row.State != "ok" || row.UsedPercent == nil {
					t.Fatalf("unexpected gap: %#v", row)
				}
			}
		})
	}
}

func TestPreviouslyValidNullWindowPersistsMissingGap(t *testing.T) {
	for _, provider := range []string{"claude", "codex"} {
		t.Run(provider, func(t *testing.T) {
			s := openTestStore(t)
			now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
			var missing atomic.Bool
			native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v8/management/credentials" {
					_ = json.NewEncoder(w).Encode(map[string]any{"files": []map[string]any{{"provider": provider, "auth_index": "idx", "disabled": false}}})
					return
				}
				var req probeRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
					return
				}
				if req.URL == claudeProfileURL {
					_ = json.NewEncoder(w).Encode(probeResponse{StatusCode: 200, Body: `{"account":{"uuid":"account-uuid"},"organization":{"uuid":"org-uuid","rate_limit_tier":"default_claude_max_5x"}}`})
					return
				}
				body := `{"five_hour":{"utilization":25},"seven_day":{"utilization":10}}`
				if provider == "codex" {
					body = `{"rate_limit":{"primary_window":{"used_percent":25,"limit_window_seconds":18000},"secondary_window":{"used_percent":10,"limit_window_seconds":604800}}}`
				}
				if missing.Load() {
					body = `{"five_hour":null,"seven_day":{"utilization":11}}`
					if provider == "codex" {
						body = `{"rate_limit":{"primary_window":null,"secondary_window":{"used_percent":11,"limit_window_seconds":604800}}}`
					}
				}
				_ = json.NewEncoder(w).Encode(probeResponse{StatusCode: 200, Body: body})
			}))
			defer native.Close()
			c, err := NewCollector(s, native.URL, "secret", Options{Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Poll(context.Background()); err != nil {
				t.Fatal(err)
			}
			missing.Store(true)
			now = now.Add(PollInterval)
			if err = c.Poll(context.Background()); err == nil {
				t.Fatal("previously valid null window silently disappeared")
			}
			page, err := s.Query(Query{From: now.Add(-time.Second), To: now.Add(time.Second), Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, row := range page.Samples {
				if row.LimitID == "five-hour" && row.State == "unknown" && row.UsedPercent == nil && row.Reason != nil && *row.Reason == "missing_window" {
					found = true
				}
			}
			if !found {
				t.Fatalf("missing durable gap: %#v", page.Samples)
			}
		})
	}
}

func TestNoUsableQuotaWindowsRemainUnknown(t *testing.T) {
	for _, provider := range []string{"claude", "codex"} {
		for _, body := range []string{`null`, `{}`, `{"five_hour":null,"seven_day":null,"rate_limit":{"primary_window":null,"secondary_window":null},"code_review_rate_limit":null}`, `{"five_hour":false,"rate_limit":{"primary_window":false}}`, `{"five_hour":`} {
			rows := parseUsage(credential{Provider: provider}, []byte(body), time.Unix(1794844800, 0))
			if len(rows) == 0 {
				t.Fatalf("empty observations for %s %s", provider, body)
			}
			for _, row := range rows {
				if row.State != "unknown" || row.UsedPercent != nil {
					t.Fatalf("unusable payload reported availability: %#v", row)
				}
			}
		}
	}
}

func TestHTTPQueryHardening(t *testing.T) {
	h := NewHandler(openTestStore(t), "secret", func() Status { return Status{} })
	base := Prefix + "?from=2026-11-16T00:00:00Z&to=2026-11-16T01:00:00Z"
	for _, tc := range []struct {
		method, url string
		status      int
	}{{"GET", base, 200}, {"GET", base + "&provider=gemini", 400}, {"GET", base + "&limit=2001", 400}, {"GET", base + "&cursor=invalid", 400}, {"GET", base + "&limit=1&limit=2", 400}, {"GET", base + "&unknown=1", 400}, {"GET", base + "&provider=", 400}, {"POST", Prefix, 405}, {"GET", Prefix + "/unrecognized", 404}} {
		req := httptest.NewRequest(tc.method, tc.url, nil)
		req.Header.Set("Authorization", "Bearer secret")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != tc.status {
			t.Errorf("%s %s=%d want%d", tc.method, tc.url, w.Code, tc.status)
		}
	}
}

func TestDeduplicatedClaudeAliasKeepsMappingDuringProfileFailure(t *testing.T) {
	s := openTestStore(t)
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	var selected atomic.Value
	selected.Store("")
	var failProfile atomic.Bool
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v8/management/credentials" {
			files := []map[string]any{{"provider": "claude", "auth_index": "aliasA", "disabled": false}, {"provider": "claude", "auth_index": "aliasB", "disabled": false}}
			if alias := selected.Load().(string); alias != "" {
				files = []map[string]any{{"provider": "claude", "auth_index": alias, "disabled": false}}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"files": files})
			return
		}
		var request probeRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		if request.URL == claudeProfileURL {
			if failProfile.Load() {
				_ = json.NewEncoder(w).Encode(probeResponse{StatusCode: 503})
				return
			}
			_ = json.NewEncoder(w).Encode(probeResponse{StatusCode: 200, Body: `{"account":{"uuid":"account-uuid"},"organization":{"uuid":"org-uuid","rate_limit_tier":"default_claude_max_5x"}}`})
			return
		}
		_ = json.NewEncoder(w).Encode(probeResponse{StatusCode: 200, Body: `{"five_hour":{"utilization":25,"resets_at":"2026-11-16T15:00:00Z"}}`})
	}))
	defer native.Close()
	c, err := NewCollector(s, native.URL, "secret", Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	page, err := s.Query(Query{From: now.Add(-time.Minute), To: now.Add(time.Minute), Limit: 10})
	if err != nil || len(page.Samples) != 1 || page.Samples[0].IdentityQuality != "provider" {
		t.Fatalf("first poll %#v, %v", page.Samples, err)
	}
	original := page.Samples[0].AccountID
	loser := "aliasA"
	if page.Samples[0].AuthIndex == loser {
		loser = "aliasB"
	}
	selected.Store(loser)
	failProfile.Store(true)
	now = now.Add(PollInterval)
	if err = c.Poll(context.Background()); err == nil {
		t.Fatal("profile failure not surfaced")
	}
	page, err = s.Query(Query{From: now.Add(-time.Second), To: now.Add(time.Second), Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	foundGap := false
	for _, row := range page.Samples {
		if row.AccountID != original || row.IdentityQuality != "provider" || row.AuthIndex != loser {
			t.Fatalf("deduplicated alias lost mapping: %#v", row)
		}
		if row.LimitID == "unknown" && row.State == "unknown" {
			foundGap = true
		}
	}
	if !foundGap {
		t.Fatalf("missing original-account gap: %#v", page.Samples)
	}
}

func TestAliasMappingsAndSamplesCommitAtomically(t *testing.T) {
	s := openTestStore(t)
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	sample := unknownRow(credential{Provider: "claude", AuthIndex: "winner"}, now, "fixture")
	sample.AccountID = s.ID("claude", "provider", "synthetic-account")
	sample.IdentityQuality = "provider"
	alias := sample
	alias.AuthIndex = "loser"
	alias.ObservedAt = now.Add(2 * time.Minute)
	if err := s.appendObservations([]Sample{sample}, []Sample{sample, alias}, now); err == nil {
		t.Fatal("invalid alias mapping accepted")
	}
	page, err := s.Query(Query{From: now.Add(-time.Minute), To: now.Add(time.Minute), Limit: 10})
	if err != nil || len(page.Samples) != 0 {
		t.Fatalf("sample committed without mappings: %#v %v", page, err)
	}
	for _, index := range []string{"winner", "loser"} {
		_, quality, errIdentity := s.accountID(credential{Provider: "claude", AuthIndex: index}, now)
		if errIdentity != nil || quality != "credential" {
			t.Fatalf("mapping committed on failed transaction: %s %s %v", index, quality, errIdentity)
		}
	}
	alias.ObservedAt = now
	if err = s.appendObservations([]Sample{sample}, []Sample{sample, alias}, now); err != nil {
		t.Fatal(err)
	}
	for _, index := range []string{"winner", "loser"} {
		id, quality, errIdentity := s.accountID(credential{Provider: "claude", AuthIndex: index}, now)
		if errIdentity != nil || quality != "provider" || id != sample.AccountID {
			t.Fatalf("alias mapping missing: %s %s %s %v", index, id, quality, errIdentity)
		}
	}
}

func TestMappingCapacityKeepsCurrentAliases(t *testing.T) {
	s := openTestStore(t)
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	old := make([]Sample, 4096)
	for i := range old {
		index := fmt.Sprintf("zold%04d", i)
		old[i] = unknownRow(credential{Provider: "claude", AuthIndex: index}, now.Add(-time.Hour), "fixture")
		old[i].AccountID = s.ID("provider", index)
		old[i].IdentityQuality = "provider"
	}
	if err := s.appendObservations(nil, old, now); err != nil {
		t.Fatal(err)
	}
	current := unknownRow(credential{Provider: "claude", AuthIndex: "a-current"}, now, "fixture")
	current.AccountID = s.ID("provider", "current")
	current.IdentityQuality = "provider"
	if err := s.Append([]Sample{current}, now); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(nil, now); err != nil {
		t.Fatal(err)
	}
	id, quality, err := s.accountID(credential{Provider: "claude", AuthIndex: current.AuthIndex}, now)
	if err != nil || id != current.AccountID || quality != "provider" {
		t.Fatalf("capacity evicted current alias: %s %s %v", id, quality, err)
	}
}

func TestSampleCapIncludesCurrentTransactionWrites(t *testing.T) {
	s := openTestStore(t)
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("samples"))
		for i := range MaxSamples {
			if errPut := bucket.Put(sampleKey(now, uint64(i)), []byte(`{}`)); errPut != nil {
				return errPut
			}
		}
		return bucket.SetSequence(MaxSamples)
	}); err != nil {
		t.Fatal(err)
	}
	current := unknownRow(credential{Provider: "codex", AuthIndex: "current"}, now, "fixture")
	if err := s.Append([]Sample{current}, now); err != nil {
		t.Fatal(err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("samples"))
		if count := bucket.Stats().KeyN; count != MaxSamples {
			t.Errorf("sample count %d exceeds exact cap %d", count, MaxSamples)
		}
		if bucket.Get(sampleKey(now, 0)) != nil {
			t.Error("oldest observation not evicted")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRetentionRemovesAdjacentExpiredRowsAndMappings(t *testing.T) {
	s := openTestStore(t)
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	old := now.Add(-Retention - time.Hour)
	rows := make([]Sample, 5)
	for i := range rows {
		rows[i] = unknownRow(credential{Provider: "claude", AuthIndex: fmt.Sprintf("idx%d", i)}, old, "fixture")
		rows[i].AccountID = s.ID("fixture", rows[i].AuthIndex)
		rows[i].IdentityQuality = "provider"
	}
	if err := s.Append(rows, old); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(nil, now); err != nil {
		t.Fatal(err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		for _, bucket := range []string{"samples", "accounts"} {
			if count := tx.Bucket([]byte(bucket)).Stats().KeyN; count != 0 {
				t.Errorf("retention skipped adjacent %s: %d remain", bucket, count)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
