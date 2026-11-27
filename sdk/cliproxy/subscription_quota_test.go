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

func TestSubscriptionMonitorProbesCopilotOnceAtStartupAndRecovers(t *testing.T) {
	m := coreauth.NewManager(nil, nil, nil)
	_, err := m.Register(context.Background(), &coreauth.Auth{
		ID:       "copilot-healthy",
		Provider: "copilot",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"refresh_token": "gho-healthy", "access_token": "tid-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.Register(context.Background(), &coreauth.Auth{
		ID:       "copilot-exhausted",
		Provider: "copilot",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"refresh_token": "gho-exhausted", "access_token": "tid-2"},
	})
	if err != nil {
		t.Fatal(err)
	}

	healthyCalls := 0
	exhaustedCalls := 0
	exhausted := true
	reset := time.Now().Add(72 * time.Hour).UTC().Format(time.RFC3339)

	monitor := newSubscriptionQuotaMonitor(m, func(_ context.Context, a *coreauth.Auth) ([]byte, error) {
		if a.ID == "copilot-healthy" {
			healthyCalls++
			return []byte(fmt.Sprintf(`{"quota_reset_date_utc":%q,"quota_snapshots":{"premium_interactions":{"unlimited":true,"overage_permitted":true,"has_quota":true,"percent_remaining":100.0}}}`, reset)), nil
		}
		exhaustedCalls++
		if exhausted {
			return []byte(fmt.Sprintf(`{"quota_reset_date_utc":%q,"quota_snapshots":{"premium_interactions":{"unlimited":false,"overage_permitted":false,"has_quota":false,"percent_remaining":0.0,"remaining":0,"entitlement":50000}}}`, reset)), nil
		}
		return []byte(fmt.Sprintf(`{"quota_reset_date_utc":%q,"quota_snapshots":{"premium_interactions":{"unlimited":false,"overage_permitted":false,"has_quota":true,"percent_remaining":100.0,"remaining":50000,"entitlement":50000}}}`, reset)), nil
	})

	now := time.Now()
	monitor.check(context.Background(), now)
	if healthyCalls != 1 || exhaustedCalls != 1 {
		t.Fatalf("startup probe calls healthy=%d exhausted=%d, want 1/1", healthyCalls, exhaustedCalls)
	}
	exhaustedAuth, _ := m.GetByID("copilot-exhausted")
	if !exhaustedAuth.Quota.Exceeded || exhaustedAuth.Quota.Reason != "credential_quota" {
		t.Fatalf("exhausted Copilot seat was not cooled at startup: %+v", exhaustedAuth.Quota)
	}
	healthyAuth, _ := m.GetByID("copilot-healthy")
	if healthyAuth.Quota.Exceeded || healthyAuth.Unavailable {
		t.Fatalf("healthy Copilot seat was wrongly cooled: %+v", healthyAuth)
	}

	// Token refresh on the healthy seat must not trigger another initial probe.
	refreshed := healthyAuth.Clone()
	refreshed.Metadata["access_token"] = "tid-1-refreshed"
	if _, errUpd := m.Update(context.Background(), refreshed); errUpd != nil {
		t.Fatal(errUpd)
	}
	exhausted = false
	monitor.check(context.Background(), now.Add(2*time.Minute))
	if healthyCalls != 1 {
		t.Fatalf("healthy Copilot seat was re-probed after access_token refresh: calls=%d", healthyCalls)
	}
	if exhaustedCalls != 2 {
		t.Fatalf("cooled Copilot seat was not rechecked for recovery: calls=%d", exhaustedCalls)
	}
	recoveredAuth, _ := m.GetByID("copilot-exhausted")
	if recoveredAuth.Quota.Exceeded || recoveredAuth.Unavailable {
		t.Fatalf("restored Copilot seat remained blocked: unavailable=%v quota=%+v", recoveredAuth.Unavailable, recoveredAuth.Quota)
	}

	// Replacing a failing Copilot credential clears stale initial-probe backoff immediately.
	failingCalls := 0
	failFirst := true
	_, _ = m.Register(context.Background(), &coreauth.Auth{
		ID:       "copilot-replace",
		Provider: "copilot",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"refresh_token": "gho-old"},
	})
	replaceMonitor := newSubscriptionQuotaMonitor(m, func(_ context.Context, a *coreauth.Auth) ([]byte, error) {
		if a.ID != "copilot-replace" {
			return []byte(fmt.Sprintf(`{"quota_reset_date_utc":%q,"quota_snapshots":{"premium_interactions":{"unlimited":true,"has_quota":true}}}`, reset)), nil
		}
		failingCalls++
		if failFirst {
			return nil, errors.New("offline")
		}
		return []byte(fmt.Sprintf(`{"quota_reset_date_utc":%q,"quota_snapshots":{"premium_interactions":{"unlimited":false,"overage_permitted":false,"has_quota":false,"percent_remaining":0.0,"remaining":0,"entitlement":50000}}}`, reset)), nil
	})
	replaceMonitor.check(context.Background(), now)
	if failingCalls != 1 {
		t.Fatalf("initial failure calls = %d, want 1", failingCalls)
	}
	failFirst = false
	cur, _ := m.GetByID("copilot-replace")
	replaced := cur.Clone()
	replaced.Metadata["refresh_token"] = "gho-new"
	if _, errUpd := m.Update(context.Background(), replaced); errUpd != nil {
		t.Fatal(errUpd)
	}
	replaceMonitor.check(context.Background(), now.Add(10*time.Second))
	if failingCalls != 2 {
		t.Fatalf("replacement Copilot credential inherited old backoff: calls=%d, want 2", failingCalls)
	}
	replacedAuth, _ := m.GetByID("copilot-replace")
	if !replacedAuth.Quota.Exceeded || replacedAuth.Quota.Reason != "credential_quota" {
		t.Fatalf("replacement exhausted Copilot credential was not cooled immediately: %+v", replacedAuth.Quota)
	}
}
