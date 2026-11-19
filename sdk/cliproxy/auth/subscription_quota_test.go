package auth

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
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

func TestSubscriptionQuotaRevisionSurvivesAuthUpdate(t *testing.T){
	m,base:=subscriptionFixture(t)
	incoming:=base.Clone();incoming.subscriptionQuotaRevision=0
	if _,err:=m.Update(context.Background(),incoming);err!=nil{t.Fatal(err)}
	current,_:=m.GetByID(base.ID)
	if current.subscriptionQuotaRevision!=base.subscriptionQuotaRevision{t.Fatal("credential update discarded the quota rejection fence")}
}
