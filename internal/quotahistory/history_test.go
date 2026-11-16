package quotahistory

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MoeForge/moego/json"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if errClose := s.Close(); errClose != nil {
			t.Error(errClose)
		}
	})
	return s
}

func TestStoreRetentionPaginationIdentityAndPermissions(t *testing.T) {
	s := openTestStore(t)
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	a := Sample{ObservedAt: now, Provider: "claude", AccountID: s.ID("claude", "abc"), AuthIndex: "idx", IdentityQuality: "provider", LimitID: "five-hour", State: "unknown", Source: "management-api"}
	old := a
	old.ObservedAt = now.Add(-31 * 24 * time.Hour)
	if err := s.Append([]Sample{old, a, a}, now); err != nil {
		t.Fatal(err)
	}
	q := Query{From: now.Add(-time.Hour), To: now.Add(time.Hour), Limit: 1}
	p, err := s.Query(q)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Samples) != 1 || p.NextCursor == nil {
		t.Fatalf("page %#v", p)
	}
	q.Cursor = *p.NextCursor
	p, err = s.Query(q)
	if err != nil || len(p.Samples) != 1 || p.NextCursor != nil {
		t.Fatalf("page2 %#v %v", p, err)
	}
	q.Provider = "codex"
	if _, err = s.Query(q); err == nil {
		t.Fatal("cursor reused with changed filter")
	}
	if s.ID("claude", "abc") != a.AccountID || s.ID("codex", "abc") == a.AccountID {
		t.Fatal("unstable identity")
	}
	for path, mode := range map[string]os.FileMode{s.dir: 0700, filepath.Join(s.dir, "history.db"): 0600} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != mode {
			t.Fatalf("permissions %s %v %v", path, info, err)
		}
	}
}

func TestProviderFixturesPreserveUnknownAndBoundaries(t *testing.T) {
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	c := credential{Provider: "claude", AuthIndex: "idx"}
	rows := parseUsage(c, []byte(`{"five_hour":{"utilization":23.5,"resets_at":"2026-11-16T15:00:00Z"},"seven_day":{"utilization":null},"seven_day_sonnet":{"utilization":101}}`), now)
	if len(rows) != 3 || rows[0].UsedPercent == nil || *rows[0].UsedPercent != 23.5 || *rows[0].PeriodSeconds != 18000 {
		t.Fatalf("rows %#v", rows)
	}
	if rows[1].UsedPercent != nil || rows[1].State != "unknown" || rows[2].UsedPercent != nil {
		t.Fatal("missing/invalid becomes availability")
	}
	c.Provider = "codex"
	rows = parseUsage(c, []byte(`{"rate_limit":{"primary_window":{"used_percent":12,"limit_window_seconds":18000,"reset_at":1794844800},"secondary_window":{"used_percent":"22","limit_window_seconds":2592000,"reset_after_seconds":3600}},"code_review_rate_limit":{"primary_window":{"used_percent":null}}}`), now)
	if len(rows) != 3 || rows[1].LimitID != "monthly" || rows[1].ResetAt == nil || !rows[1].ResetAt.Equal(now.Add(time.Hour)) || rows[2].State != "unknown" {
		t.Fatalf("codex %#v", rows)
	}
	if got := parseUsage(c, []byte(`{"unrecognized":true}`), now); len(got) != 1 || got[0].State != "unknown" {
		t.Fatalf("unsupported %#v", got)
	}
	if got := parseUsage(c, []byte(`null`), now); len(got) != 1 || got[0].State != "unknown" {
		t.Fatal("null payload")
	}
}

func TestHTTPAuthValidationAndCapabilities(t *testing.T) {
	s := openTestStore(t)
	h := NewHandler(s, "secret", func() Status { return Status{Stale: true} })
	for _, tc := range []struct {
		url, key string
		status   int
	}{
		{Prefix + "/capabilities", "", 401}, {Prefix + "/capabilities", "Bearer wrong", 401},
		{Prefix + "/capabilities", "Bearer secret", 200}, {Prefix, "Bearer secret", 400},
		{Prefix + "?from=2026-01-01T00:00:00Z&to=2026-04-01T00:00:00Z", "Bearer secret", 400},
		{Prefix + "?from=2026-01-01T00:00:00Z&to=2026-01-02T00:00:00Z&limit=0", "Bearer secret", 400},
	} {
		r := httptest.NewRequest(http.MethodGet, tc.url, nil)
		r.Header.Set("Authorization", tc.key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%s: %d body %s", tc.url, w.Code, w.Body.String())
		}
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("cacheable")
		}
	}
}

func TestCollectorTrustedRequestsDeduplicatesAndRedacts(t *testing.T) {
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	var calls atomic.Int32
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Error("missing management authentication")
		}
		if r.URL.Path == "/v8/management/credentials" {
			_, _ = w.Write([]byte(`{"files":[{"provider":"codex","auth_index":"a","disabled":false,"id_token":{"chatgpt_account_id":"same-account"}},{"provider":"codex","auth_index":"alias","disabled":false,"id_token":{"chatgpt_account_id":"same-account"}},{"provider":"claude","auth_index":"off","disabled":true},{"provider":"gemini","auth_index":"other"}]}`))
			return
		}
		if r.URL.Path != "/v8/management/requests/api-call" || r.Method != "POST" {
			t.Error("unexpected path")
		}
		var req probeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		if req.URL != "https://chatgpt.com/backend-api/wham/usage" || req.Header["Authorization"] != "Bearer $TOKEN$" || req.Header["Chatgpt-Account-Id"] != "same-account" {
			t.Errorf("wrong probe %#v", req)
		}
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(probeResponse{StatusCode: 200, Body: `{"plan_type":"pro","rate_limit":{"primary_window":{"used_percent":42,"limit_window_seconds":18000,"reset_at":1794844800}}}`})
	}))
	defer native.Close()
	s := openTestStore(t)
	c, err := NewCollector(s, native.URL, "secret", Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("workspace members merged, calls %d", calls.Load())
	}
	p, err := s.Query(Query{From: now.Add(-time.Minute), To: now.Add(time.Minute), Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Samples) != 2 || p.Samples[0].IdentityQuality != "credential" || p.Samples[0].AccountID == p.Samples[1].AccountID {
		t.Fatalf("page %#v", p)
	}
	raw, _ := json.Marshal(p)
	for _, secret := range []string{"same-account", "secret", "Bearer", "@"} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("leaked %s", secret)
		}
	}
	if c.Status().LastSuccessAt == nil || c.Status().Stale {
		t.Fatal("status not updated")
	}
}

func TestHealthReadinessContainsNoPrivateData(t *testing.T) {
	h := NewHandler(openTestStore(t), "secret", func() Status { return Status{} })
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w.Code != 200 || strings.TrimSpace(w.Body.String()) != `{"status":"ok"}` {
		t.Fatalf("health %d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/healthz", nil))
	if w.Code != 405 {
		t.Fatal("health accepts mutation method")
	}
}

func TestLoopbackValidation(t *testing.T) {
	for _, origin := range []string{"http://example.com:8317", "http://localhost:8317", "https://127.0.0.1:8317", "http://127.0.0.1:8317/path", "http://user:pass@127.0.0.1:8317", "http://127.0.0.1", "http://127.0.0.1:8317?"} {
		if _, err := validateOrigin(origin); err == nil {
			t.Fatalf("accepted %s", origin)
		}
	}
	for _, addr := range []string{"0.0.0.0:8318", "localhost:8318", ":8318", "192.168.1.2:8318"} {
		if err := ValidateListen(addr); err == nil {
			t.Fatalf("accepted %s", addr)
		}
	}
	if err := ValidateListen("[::1]:8318"); err != nil {
		t.Fatal(err)
	}
}
