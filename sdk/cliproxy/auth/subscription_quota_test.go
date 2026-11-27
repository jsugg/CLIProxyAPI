package auth

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func subscriptionUsage(weekly, fable int) []byte {
	return []byte(fmt.Sprintf(`{"five_hour":null,"seven_day":{"utilization":%d,"resets_at":%q},"limits":[{"kind":"weekly_scoped","percent":%d,"scope":{"model":{"display_name":"Fable"}}}]}`, weekly, time.Now().Add(72*time.Hour).UTC().Format(time.RFC3339), fable))
}

func subscriptionFixture(t *testing.T) (*Manager, *Auth) {
	t.Helper()
	m := NewManager(nil, nil, nil)
	id := t.Name()
	registry.GetGlobalRegistry().RegisterClient(id, "claude", []*registry.ModelInfo{{ID: "claude-opus-5-5"}, {ID: "claude-fable-5-2"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
	_, err := m.Register(context.Background(), &Auth{ID: id, Provider: "claude", Status: StatusActive, Metadata: map[string]any{"access_token": "test-token"}})
	if err != nil {
		t.Fatal(err)
	}
	retry := 72 * time.Hour
	m.MarkResult(context.Background(), Result{AuthID: id, Provider: "claude", Model: "claude-opus-5-5", CredentialScope: true, RetryAfter: &retry, Error: &Error{HTTPStatus: 429, Message: "rate_limit_error"}})
	a, _ := m.GetByID(id)
	return m, a
}

func TestSubscriptionQuotaRestoration(t *testing.T) {
	for _, tc := range []struct {
		name          string
		weekly, fable int
		restored      bool
	}{
		{"Fable full half refunded", 50, 0, true},
		{"mixed usage retains Opus charge", 80, 0, true},
		{"all models still exhausted", 100, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, base := subscriptionFixture(t)
			changed, err := m.ReconcileSubscriptionQuota(context.Background(), base, subscriptionUsage(tc.weekly, tc.fable))
			if err != nil {
				t.Fatal(err)
			}
			if changed != tc.restored {
				t.Fatalf("recovered=%v, want %v", changed, tc.restored)
			}
			a, _ := m.GetByID(base.ID)
			for _, model := range []string{"claude-opus-5-5", "claude-fable-5-2"} {
				blocked, _, _ := isAuthBlockedForModel(a, model, time.Now())
				if blocked == tc.restored {
					t.Fatalf("%s blocked=%v", model, blocked)
				}
			}
			if tc.restored && registry.GetGlobalRegistry().IsModelQuotaExceededForClient(base.ID, "claude-opus-5-5") {
				t.Fatal("registry quota survived")
			}
		})
	}
}

func TestSubscriptionQuotaRejectsUnprovenRecovery(t *testing.T) {
	for _, raw := range []string{`{}`, `{"five_hour":null}`, `{"five_hour":null,"seven_day":null}`, `{"five_hour":null,"seven_day":{"utilization":-1}}`, `{"five_hour":{"utilization":100},"seven_day":{"utilization":0}}`, `{"five_hour":null,"seven_day":{"utilization":0,"resets_at":"2020-01-01T00:00:00Z"}}`} {
		t.Run(raw, func(t *testing.T) {
			m, a := subscriptionFixture(t)
			changed, err := m.ReconcileSubscriptionQuota(context.Background(), a, []byte(raw))
			if err != nil {
				t.Fatal(err)
			}
			if changed {
				t.Fatal("unproven quota cleared cooldown")
			}
		})
	}
}

func TestSubscriptionQuotaPreservesIndependentFailures(t *testing.T) {
	m, _ := subscriptionFixture(t)
	retry := time.Hour
	m.MarkResult(context.Background(), Result{AuthID: t.Name(), Provider: "claude", Model: "claude-fable-5-2", RetryAfter: &retry, Error: &Error{HTTPStatus: 503, Message: "capacity"}})
	a, _ := m.GetByID(t.Name())
	if changed, err := m.ReconcileSubscriptionQuota(context.Background(), a, subscriptionUsage(50, 0)); err != nil || !changed {
		t.Fatalf("recovery=%v %v", changed, err)
	}
	a, _ = m.GetByID(a.ID)
	if blocked, _, _ := isAuthBlockedForModel(a, "claude-opus-5-5", time.Now()); blocked {
		t.Fatal("Opus stayed blocked")
	}
	if blocked, _, _ := isAuthBlockedForModel(a, "claude-fable-5-2", time.Now()); !blocked {
		t.Fatal("independent 503 was cleared")
	}
	if a.Status != StatusError || a.LastError == nil || a.LastError.HTTPStatus != 503 {
		t.Fatalf("independent error hidden by recovery: status=%s error=%+v", a.Status, a.LastError)
	}
}

func TestSubscriptionQuotaRecoversDuringUnrelatedSuccess(t *testing.T) {
	m := NewManager(nil, nil, nil)
	_, _ = m.Register(context.Background(), &Auth{ID: t.Name(), Provider: "claude", Status: StatusActive, Metadata: map[string]any{"access_token": "fixture"}})
	retry := time.Hour
	m.MarkResult(context.Background(), Result{AuthID: t.Name(), Provider: "claude", Model: "claude-opus-5-5", RetryAfter: &retry, Error: &Error{HTTPStatus: 429}})
	base, _ := m.GetByID(t.Name())
	m.MarkResult(context.Background(), Result{AuthID: base.ID, Provider: "claude", Model: "claude-sonnet-5-5", Success: true})
	body := []byte(fmt.Sprintf(`{"five_hour":null,"seven_day":{"utilization":50,"resets_at":%q},"seven_day_opus":{"utilization":0,"resets_at":%q}}`, time.Now().Add(time.Hour).Format(time.RFC3339), time.Now().Add(time.Hour).Format(time.RFC3339)))
	if recovered, err := m.ReconcileSubscriptionQuota(context.Background(), base, body); err != nil || !recovered {
		t.Fatalf("unrelated traffic prevented recovery: %v %v", recovered, err)
	}
	a, _ := m.GetByID(base.ID)
	if blocked, _, _ := isAuthBlockedForModel(a, "claude-opus-5-5", time.Now()); blocked {
		t.Fatal("Opus still blocked")
	}
}

func TestSubscriptionQuotaIgnoresDollarBalances(t *testing.T) {
	for _, tc := range []struct {
		name, balance, limits string
		want                  bool
	}{
		{"spent credits with healthy Fable", `"limit_dollars":100,"used_dollars":100,"utilization":100`, `,"limits":[{"kind":"weekly_scoped","percent":64,"scope":{"model":{"display_name":"Fable"}}}]`, true},
		{"remaining credits cannot prove Fable recovery", `"remaining_dollars":95,"utilization":5`, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := NewManager(nil, nil, nil)
			_, _ = m.Register(context.Background(), &Auth{ID: t.Name(), Provider: "claude", Status: StatusActive, Metadata: map[string]any{"access_token": "fixture"}})
			retry := time.Hour
			m.MarkResult(context.Background(), Result{AuthID: t.Name(), Provider: "claude", Model: "claude-fable-5-2", RetryAfter: &retry, Error: &Error{HTTPStatus: 429}})
			base, _ := m.GetByID(t.Name())
			body := []byte(fmt.Sprintf(`{"five_hour":null,"seven_day":{"utilization":50,"resets_at":%q},"iguana_necktie":{%s,"resets_at":%q}%s}`, time.Now().Add(time.Hour).Format(time.RFC3339), tc.balance, time.Now().Add(time.Hour).Format(time.RFC3339), tc.limits))
			if recovered, err := m.ReconcileSubscriptionQuota(context.Background(), base, body); err != nil || recovered != tc.want {
				t.Fatalf("recovery=%v want=%v err=%v", recovered, tc.want, err)
			}
		})
	}
}

func TestSubscriptionQuotaVersionedFableDoesNotBlockOpus(t *testing.T) {
	m, base := subscriptionFixture(t)
	body := []byte(fmt.Sprintf(`{"five_hour":null,"seven_day":{"utilization":50,"resets_at":%q},"limits":[{"kind":"weekly_scoped","percent":100,"is_active":true,"scope":{"model":{"display_name":"Fable 5.2"}}}]}`, time.Now().Add(time.Hour).Format(time.RFC3339)))
	if recovered, err := m.ReconcileSubscriptionQuota(context.Background(), base, body); err != nil || !recovered {
		t.Fatalf("Opus recovery=%v err=%v", recovered, err)
	}
	a, _ := m.GetByID(base.ID)
	if blocked, _, _ := isAuthBlockedForModel(a, "claude-opus-5-5", time.Now()); blocked {
		t.Fatal("Opus blocked by Fable")
	}
}

func TestSubscriptionQuotaZeroUsageWithoutReset(t *testing.T) {
	m, base := subscriptionFixture(t)
	body := []byte(`{"five_hour":{"utilization":0,"resets_at":null},"seven_day":{"utilization":0,"resets_at":null},"iguana_necktie":{"utilization":0,"resets_at":null}}`)
	if recovered, err := m.ReconcileSubscriptionQuota(context.Background(), base, body); err != nil || !recovered {
		t.Fatalf("zero-usage recovery=%v err=%v", recovered, err)
	}
}

func TestSubscriptionQuotaRejectsRacingResponse(t *testing.T) {
	for _, kind := range []string{"new rejection", "disabled", "replaced", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			m, base := subscriptionFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch kind {
			case "new rejection":
				retry := 96 * time.Hour
				m.MarkResult(ctx, Result{AuthID: base.ID, Provider: "claude", Model: "claude-opus-5-5", CredentialScope: true, RetryAfter: &retry, Error: &Error{HTTPStatus: 429}})
			case "disabled":
				a := base.Clone()
				a.Disabled = true
				if _, err := m.Update(ctx, a); err != nil {
					t.Fatal(err)
				}
			case "replaced":
				a := base.Clone()
				a.Metadata["access_token"] = "replacement"
				if _, err := m.Update(ctx, a); err != nil {
					t.Fatal(err)
				}
			case "cancelled":
				cancel()
			}
			changed, _ := m.ReconcileSubscriptionQuota(ctx, base, subscriptionUsage(0, 0))
			if changed {
				t.Fatal("stale response recovered credential")
			}
		})
	}
}

func TestSubscriptionQuotaModelLimitSurvivesSharedRecovery(t *testing.T) {
	m, _ := subscriptionFixture(t)
	retry := 24 * time.Hour
	m.MarkResult(context.Background(), Result{AuthID: t.Name(), Provider: "claude", Model: "claude-fable-5-2", RetryAfter: &retry, Error: &Error{HTTPStatus: 429, Message: "Fable limit"}})
	a, _ := m.GetByID(t.Name())
	if changed, err := m.ReconcileSubscriptionQuota(context.Background(), a, subscriptionUsage(50, 100)); err != nil || !changed {
		t.Fatalf("recovery=%v %v", changed, err)
	}
	a, _ = m.GetByID(a.ID)
	if blocked, _, _ := isAuthBlockedForModel(a, "claude-fable-5-2", time.Now()); !blocked {
		t.Fatal("exhausted Fable became available")
	}
	if blocked, _, _ := isAuthBlockedForModel(a, "claude-opus-5-5", time.Now()); blocked {
		t.Fatal("healthy Opus stayed blocked")
	}
}

func TestSubscriptionQuotaPreservesForceCooldown(t *testing.T) {
	m, base := subscriptionFixture(t)
	retry := time.Hour
	m.MarkResult(context.Background(), Result{AuthID: base.ID, Provider: "claude", Model: "claude-opus-5-5", CredentialScope: true, RetryAfter: &retry, Error: &Error{Code: ErrorCodeForceCooldown, HTTPStatus: 429}})
	base, _ = m.GetByID(base.ID)
	if recovered, _ := m.ReconcileSubscriptionQuota(context.Background(), base, subscriptionUsage(0, 0)); recovered {
		t.Fatal("operator force cooldown cleared")
	}
}

func TestCodexSubscriptionQuotaRecovery(t *testing.T) {
	for _, used := range []int{50, 100} {
		t.Run(fmt.Sprint(used), func(t *testing.T) {
			m := NewManager(nil, nil, nil)
			_, _ = m.Register(context.Background(), &Auth{ID: t.Name(), Provider: "codex", Status: StatusActive, Metadata: map[string]any{"access_token": "fixture"}})
			retry := time.Hour
			m.MarkResult(context.Background(), Result{AuthID: t.Name(), Provider: "codex", Model: "gpt-6.1-sol", RetryAfter: &retry, Error: &Error{HTTPStatus: 429, Message: "usage_limit_reached"}})
			base, _ := m.GetByID(t.Name())
			body := []byte(fmt.Sprintf(`{"rate_limit":{"allowed":true,"limit_reached":false,"primary_window":{"used_percent":%d,"reset_at":%d},"secondary_window":null}}`, used, time.Now().Add(time.Hour).Unix()))
			recovered, err := m.ReconcileSubscriptionQuota(context.Background(), base, body)
			if err != nil || recovered != (used < 100) {
				t.Fatalf("recovered=%v err=%v", recovered, err)
			}
		})
	}
}

func TestSubscriptionModelQuotaNeedsMatchingObservation(t *testing.T) {
	m := NewManager(nil, nil, nil)
	_, _ = m.Register(context.Background(), &Auth{ID: t.Name(), Provider: "claude", Status: StatusActive, Metadata: map[string]any{"access_token": "fixture"}})
	retry := time.Hour
	m.MarkResult(context.Background(), Result{AuthID: t.Name(), Provider: "claude", Model: "claude-fable-5-2", RetryAfter: &retry, Error: &Error{HTTPStatus: 429, Message: "Fable limit"}})
	base, _ := m.GetByID(t.Name())
	body := []byte(fmt.Sprintf(`{"five_hour":null,"seven_day":{"utilization":50,"resets_at":%q}}`, time.Now().Add(time.Hour).Format(time.RFC3339)))
	if recovered, _ := m.ReconcileSubscriptionQuota(context.Background(), base, body); recovered {
		t.Fatal("missing Fable observation cleared model limit")
	}
}

func TestSubscriptionQuotaRecoveryPersists(t *testing.T) {
	m, base := subscriptionFixture(t)
	store := &recordingCooldownStateStore{}
	if !m.SwapCooldownStateStore(context.Background(), store, false) {
		t.Fatal("store setup")
	}
	m.PersistCooldownStates(context.Background())
	if len(store.savedRecords()) == 0 {
		t.Fatal("no initial cooldown persisted")
	}
	if recovered, err := m.ReconcileSubscriptionQuota(context.Background(), base, subscriptionUsage(50, 0)); err != nil || !recovered {
		t.Fatalf("recovery=%v %v", recovered, err)
	}
	if len(store.savedRecords()) != 0 {
		t.Fatal("recovered cooldown would return on restart")
	}
}

func TestSubscriptionQuotaDoesNotRecoverSIWCPartition(t *testing.T) {
	m := NewManager(nil, nil, nil)
	_, _ = m.Register(context.Background(), &Auth{ID: t.Name(), Provider: "codex", Status: StatusActive, Metadata: map[string]any{"access_token": "fixture", "use_siwc": true}})
	retry := time.Hour
	m.MarkResult(context.Background(), Result{AuthID: t.Name(), Provider: "codex", Model: "gpt-6.1-sol", RetryAfter: &retry, Error: &Error{HTTPStatus: 429}})
	base, _ := m.GetByID(t.Name())
	body := []byte(fmt.Sprintf(`{"rate_limit":{"allowed":true,"limit_reached":false,"primary_window":{"used_percent":0,"reset_at":%d},"secondary_window":null}}`, time.Now().Add(time.Hour).Unix()))
	if recovered, _ := m.ReconcileSubscriptionQuota(context.Background(), base, body); recovered {
		t.Fatal("outer subscription read cleared another authentication partition")
	}
}

func TestSubscriptionQuotaRevisionSurvivesAuthUpdate(t *testing.T) {
	m, base := subscriptionFixture(t)
	incoming := base.Clone()
	incoming.subscriptionQuotaRevision = 0
	if _, err := m.Update(context.Background(), incoming); err != nil {
		t.Fatal(err)
	}
	current, _ := m.GetByID(base.ID)
	if current.subscriptionQuotaRevision != base.subscriptionQuotaRevision {
		t.Fatal("credential update discarded the quota rejection fence")
	}
}

func TestCopilot402CredentialQuotaCoolsWholeAccountAndRecovers(t *testing.T) {
	m := NewManager(nil, nil, nil)
	id := t.Name()
	registry.GetGlobalRegistry().RegisterClient(id, "copilot", []*registry.ModelInfo{
		{ID: "copilot/gemini-4-neon"},
		{ID: "copilot/claude-opus-5.5"},
	})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
	_, err := m.Register(context.Background(), &Auth{
		ID:       id,
		Provider: "copilot",
		Status:   StatusActive,
		Metadata: map[string]any{"refresh_token": "gho-fixture", "access_token": "tid-fixture"},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &recordingCooldownStateStore{}
	if !m.SwapCooldownStateStore(context.Background(), store, false) {
		t.Fatal("store setup")
	}

	retry := 84 * time.Hour
	before := time.Now()
	m.MarkResult(context.Background(), Result{
		AuthID:          id,
		Provider:        "copilot",
		Model:           "copilot/gemini-4-neon",
		CredentialScope: true,
		RetryAfter:      &retry,
		Error: &Error{
			Code:       ErrorCodeCredentialQuota,
			HTTPStatus: http.StatusPaymentRequired,
			Message:    `{"error":{"message":"You have exceeded your monthly quota","code":"quota_exceeded"}}`,
		},
	})

	cooled, _ := m.GetByID(id)
	if !cooled.Quota.Exceeded || cooled.Quota.Reason != "credential_quota" {
		t.Fatalf("expected credential_quota, got %+v", cooled.Quota)
	}
	if cooled.Quota.NextRecoverAt.Before(before.Add(83 * time.Hour)) {
		t.Fatalf("NextRecoverAt = %v, want ~84h from %v (must not fall back to 30m)", cooled.Quota.NextRecoverAt, before)
	}
	for _, model := range []string{"copilot/gemini-4-neon", "copilot/claude-opus-5.5", "copilot/gpt-6.1-sol"} {
		blocked, _, next := isAuthBlockedForModel(cooled, model, before.Add(time.Hour))
		if !blocked || next.Before(before.Add(83*time.Hour)) {
			t.Fatalf("model %s after 1h: blocked=%v next=%v", model, blocked, next)
		}
	}
	views := CooldownSnapshotForAuth(cooled, before)
	if len(views) == 0 || views[0].Scope != "credential" || views[0].Reason != "credential_quota" {
		t.Fatalf("cooldown view = %+v, want credential/credential_quota", views)
	}
	if !SubscriptionQuotaNeedsRefresh(cooled, before.Add(time.Minute)) {
		t.Fatal("expected cooled Copilot 402 credential_quota to need refresh")
	}

	// Exhausted snapshot must keep the cooldown intact.
	exhaustedBody := []byte(`{"quota_reset_date_utc":"2026-12-01T00:00:00.000Z","quota_snapshots":{"premium_interactions":{"unlimited":false,"overage_permitted":false,"has_quota":false,"percent_remaining":0.0,"remaining":0,"entitlement":50000}}}`)
	if recovered, errRec := m.ReconcileSubscriptionQuota(context.Background(), cooled, exhaustedBody); errRec != nil || recovered {
		t.Fatalf("exhausted snapshot cleared cooldown: recovered=%v err=%v", recovered, errRec)
	}

	// Restored quota clears the credential_quota cooldown across all models.
	restoredBody := []byte(`{"quota_reset_date_utc":"2027-01-01T00:00:00.000Z","quota_snapshots":{"premium_interactions":{"unlimited":false,"overage_permitted":false,"has_quota":true,"percent_remaining":100.0,"remaining":50000,"entitlement":50000}}}`)
	if recovered, errRec := m.ReconcileSubscriptionQuota(context.Background(), cooled, restoredBody); errRec != nil || !recovered {
		t.Fatalf("restored snapshot failed to recover: recovered=%v err=%v", recovered, errRec)
	}
	after, _ := m.GetByID(id)
	for _, model := range []string{"copilot/gemini-4-neon", "copilot/claude-opus-5.5"} {
		if blocked, _, _ := isAuthBlockedForModel(after, model, time.Now()); blocked {
			t.Fatalf("model %s remained blocked after quota restoration", model)
		}
	}
	if len(store.savedRecords()) != 0 {
		t.Fatalf("cooldown store still holds records after recovery: %+v", store.savedRecords())
	}
}

func TestCopilotProactiveExhaustedQuotaCoolsBeforeTraffic(t *testing.T) {
	m := NewManager(nil, nil, nil)
	id := t.Name()
	registry.GetGlobalRegistry().RegisterClient(id, "copilot", []*registry.ModelInfo{
		{ID: "copilot/gemini-4-neon"},
		{ID: "copilot/claude-opus-5.5"},
	})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
	_, err := m.Register(context.Background(), &Auth{
		ID:       id,
		Provider: "copilot",
		Status:   StatusActive,
		Metadata: map[string]any{"refresh_token": "gho-fixture"},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &recordingCooldownStateStore{}
	if !m.SwapCooldownStateStore(context.Background(), store, false) {
		t.Fatal("store setup")
	}

	reset := time.Now().Add(72 * time.Hour).UTC().Truncate(time.Second)
	exhaustedBody := []byte(fmt.Sprintf(`{"quota_reset_date_utc":%q,"quota_snapshots":{"chat":{"unlimited":true,"percent_remaining":100.0,"has_quota":true},"premium_interactions":{"unlimited":false,"overage_permitted":false,"has_quota":false,"percent_remaining":0.0,"quota_remaining":0.0,"remaining":0,"entitlement":50000}}}`, reset.Format(time.RFC3339)))

	base, _ := m.GetByID(id)
	if !SubscriptionQuotaNeedsInitialProbe(base, time.Now()) {
		t.Fatal("expected uncooled Copilot credential to request initial probe")
	}
	// Concurrent inference access_token rotation must not discard the in-flight probe.
	rotated := base.Clone()
	rotated.Metadata["access_token"] = "tid-rotated"
	if _, errUpd := m.Update(context.Background(), rotated); errUpd != nil {
		t.Fatal(errUpd)
	}
	recovered, errRec := m.ReconcileSubscriptionQuota(context.Background(), base, exhaustedBody)
	if errRec != nil || recovered {
		t.Fatalf("ReconcileSubscriptionQuota = (%v, %v), want (false, nil)", recovered, errRec)
	}
	cooled, _ := m.GetByID(id)
	if !cooled.Quota.Exceeded || cooled.Quota.Reason != "credential_quota" || !cooled.Quota.NextRecoverAt.Equal(reset) {
		t.Fatalf("unexpected proactive cooldown state: %+v (want reset=%v)", cooled.Quota, reset)
	}
	if SubscriptionQuotaNeedsInitialProbe(cooled, time.Now()) {
		t.Fatal("already cooled Copilot credential must not trigger another initial probe")
	}
	for _, model := range []string{"copilot/gemini-4-neon", "copilot/claude-opus-5.5"} {
		if blocked, _, _ := isAuthBlockedForModel(cooled, model, time.Now()); !blocked {
			t.Fatalf("model %s was not blocked after proactive exhaustion", model)
		}
	}
	if len(store.savedRecords()) == 0 {
		t.Fatal("proactive Copilot cooldown was not persisted")
	}
}

func TestCopilotFailoverPreserves503OverFallback402QuotaError(t *testing.T) {
	prevDisable := quotaCooldownDisabled.Load()
	quotaCooldownDisabled.Store(false)
	t.Cleanup(func() { quotaCooldownDisabled.Store(prevDisable) })

	m := NewManager(nil, &RoundRobinSelector{}, nil)
	reg := registry.GetGlobalRegistry()
	model := "copilot/gemini-4-neon"
	idHealthy := t.Name() + "-healthy"
	idExhausted := t.Name() + "-exhausted"
	reg.RegisterClient(idHealthy, "copilot", []*registry.ModelInfo{
		{ID: model, SourceModelID: model},
		{ID: "copilot/claude-opus-5.5", SourceModelID: "copilot/claude-opus-5.5"},
	})
	reg.RegisterClient(idExhausted, "copilot", []*registry.ModelInfo{
		{ID: model, SourceModelID: model},
		{ID: "copilot/claude-opus-5.5", SourceModelID: "copilot/claude-opus-5.5"},
	})
	t.Cleanup(func() {
		reg.UnregisterClient(idHealthy)
		reg.UnregisterClient(idExhausted)
	})
	exp := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	_, _ = m.Register(context.Background(), &Auth{
		ID:         idHealthy,
		Provider:   "copilot",
		Status:     StatusActive,
		Attributes: map[string]string{"priority": "100"},
		Metadata: map[string]any{
			"access_token": "tok-healthy",
			"base_url":     "https://api.business.githubcopilot.com",
			"expired":      exp,
		},
	})
	_, _ = m.Register(context.Background(), &Auth{
		ID:         idExhausted,
		Provider:   "copilot",
		Status:     StatusActive,
		Attributes: map[string]string{"priority": "50"},
		Metadata: map[string]any{
			"access_token": "tok-exhausted",
			"base_url":     "https://api.enterprise.githubcopilot.com",
			"expired":      exp,
		},
	})

	retry84h := 84 * time.Hour
	m.RegisterExecutor(&customStreamMockExecutor{
		identifier: "copilot",
		streamFn: func(ctx context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
			cliproxyexecutor.MarkUpstreamAttempt(ctx)
			if auth.ID == idHealthy {
				return nil, customStatusError{
					code: http.StatusServiceUnavailable,
					msg:  `{"error":{"message":"model overloaded","code":"model_overloaded"}}`,
				}
			}
			return nil, copilotCredQuotaStatusError{
				code:       http.StatusPaymentRequired,
				msg:        `{"error":{"message":"You have exceeded your monthly quota","code":"quota_exceeded"}}`,
				retryAfter: &retry84h,
			}
		},
	})

	_, errStream := m.ExecuteStream(context.Background(), []string{"copilot"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{Stream: true})
	if errStream == nil {
		t.Fatal("expected error when healthy seat is 503 and fallback is 402")
	}
	if got := statusCodeFromError(errStream); got != http.StatusServiceUnavailable {
		t.Fatalf("status = %d (%v), want 503 so SDK retries instead of aborting on 402", got, errStream)
	}
	exhaustedSnap, _ := m.GetByID(idExhausted)
	if !exhaustedSnap.Quota.Exceeded || exhaustedSnap.Quota.Reason != "credential_quota" || exhaustedSnap.Quota.NextRecoverAt.Before(time.Now().Add(83*time.Hour)) {
		t.Fatalf("fallback exhausted seat was not cooled until monthly reset: %+v", exhaustedSnap.Quota)
	}
	for _, sibling := range []string{model, "copilot/claude-opus-5.5"} {
		if blocked, _, _ := isAuthBlockedForModel(exhaustedSnap, sibling, time.Now().Add(time.Hour)); !blocked {
			t.Fatalf("fallback exhausted seat became unblocked for %s after 1h", sibling)
		}
	}
}

type copilotCredQuotaStatusError struct {
	code       int
	msg        string
	retryAfter *time.Duration
}

func (e copilotCredQuotaStatusError) Error() string              { return e.msg }
func (e copilotCredQuotaStatusError) StatusCode() int            { return e.code }
func (e copilotCredQuotaStatusError) RetryAfter() *time.Duration { return e.retryAfter }
func (e copilotCredQuotaStatusError) IsCredentialScoped() bool   { return true }
func (e copilotCredQuotaStatusError) ErrorCode() string          { return ErrorCodeCredentialQuota }
