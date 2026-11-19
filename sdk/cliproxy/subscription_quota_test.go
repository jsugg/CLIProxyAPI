package cliproxy

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestSubscriptionMonitorRecoversWithoutDashboard(t *testing.T) {
	m := coreauth.NewManager(nil, nil, nil)
	_, err := m.Register(context.Background(), &coreauth.Auth{ID: t.Name(), Provider: "claude", Status: coreauth.StatusActive, Metadata: map[string]any{"access_token": "fixture"}})
	if err != nil {
		t.Fatal(err)
	}
	retry := 72 * time.Hour
	m.MarkResult(context.Background(), coreauth.Result{AuthID: t.Name(), Provider: "claude", Model: "claude-opus-5-5", CredentialScope: true, RetryAfter: &retry, Error: &coreauth.Error{HTTPStatus: 429}})
	calls := 0
	monitor := newSubscriptionQuotaMonitor(m, func(context.Context, *coreauth.Auth) ([]byte, error) {
		calls++
		return []byte(fmt.Sprintf(`{"five_hour":null,"seven_day":{"utilization":50,"resets_at":%q}}`, time.Now().Add(72*time.Hour).Format(time.RFC3339))), nil
	})
	now := time.Now()
	monitor.check(context.Background(), now)
	if calls != 0 {
		t.Fatal("new rejection was probed immediately")
	}
	monitor.check(context.Background(), now.Add(2*time.Minute))
	a, _ := m.GetByID(t.Name())
	if calls != 1 || a.Quota.Exceeded || a.Unavailable {
		t.Fatalf("calls=%d unavailable=%v quota=%+v", calls, a.Unavailable, a.Quota)
	}
	monitor.check(context.Background(), now.Add(10*time.Minute))
	if calls != 1 {
		t.Fatal("healthy credential kept polling")
	}
}

func TestSubscriptionMonitorBoundsRetries(t *testing.T) {
	m := coreauth.NewManager(nil, nil, nil)
	_, _ = m.Register(context.Background(), &coreauth.Auth{ID: t.Name(), Provider: "claude", Status: coreauth.StatusActive, Metadata: map[string]any{"access_token": "fixture"}})
	retry := 72 * time.Hour
	m.MarkResult(context.Background(), coreauth.Result{AuthID: t.Name(), Provider: "claude", Model: "claude-opus-5-5", CredentialScope: true, RetryAfter: &retry, Error: &coreauth.Error{HTTPStatus: 429}})
	var calls atomic.Int32
	monitor := newSubscriptionQuotaMonitor(m, func(context.Context, *coreauth.Auth) ([]byte, error) { calls.Add(1); return nil, errors.New("offline") })
	now := time.Now().Add(2 * time.Minute)
	monitor.check(context.Background(), now)
	monitor.check(context.Background(), now.Add(time.Minute))
	if calls.Load() != 1 {
		t.Fatal("failure retry was not throttled")
	}
	monitor.check(context.Background(), now.Add(6*time.Minute))
	if calls.Load() != 2 {
		t.Fatal("failure was never retried")
	}
	a, _ := m.GetByID(t.Name())
	if !a.Quota.Exceeded {
		t.Fatal("failed probe cleared quota")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	monitor.check(ctx, now.Add(time.Hour))
	if calls.Load() != 2 {
		t.Fatal("cancelled monitor dispatched a read")
	}
}
