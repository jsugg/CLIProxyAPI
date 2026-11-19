package auth

import (
	"context"
	"maps"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/MoeForge/moego/json"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
)

type subscriptionUsageWindow struct {
	Utilization      *float64 `json:"utilization"`
	ResetsAt         string   `json:"resets_at"`
	LimitDollars     *float64 `json:"limit_dollars"`
	UsedDollars      *float64 `json:"used_dollars"`
	RemainingDollars *float64 `json:"remaining_dollars"`
}

type subscriptionAvailability struct {
	shared bool
	models map[string]bool
	claude bool
}

func validSubscriptionPercent(n *float64) bool {
	return n != nil && !math.IsNaN(*n) && !math.IsInf(*n, 0) && *n >= 0 && *n <= 100
}

func subscriptionAvailabilityFromBody(provider string, body []byte, now time.Time) subscriptionAvailability {
	result := subscriptionAvailability{models: map[string]bool{}}
	if provider == "claude" {
		result.claude = true
		var payload struct {
			FiveHour json.RawMessage          `json:"five_hour"`
			SevenDay *subscriptionUsageWindow `json:"seven_day"`
			Opus     *subscriptionUsageWindow `json:"seven_day_opus"`
			Sonnet   *subscriptionUsageWindow `json:"seven_day_sonnet"`
			Fable    *subscriptionUsageWindow `json:"iguana_necktie"`
			Limits   []struct {
				Kind    string   `json:"kind"`
				Percent *float64 `json:"percent"`
				Active  bool     `json:"is_active"`
				Scope   struct {
					Model struct {
						Name string `json:"display_name"`
					} `json:"model"`
				} `json:"scope"`
			} `json:"limits"`
		}
		if json.Unmarshal(body, &payload) != nil || len(payload.FiveHour) == 0 || payload.SevenDay == nil {
			return result
		}
		healthy := func(w *subscriptionUsageWindow) bool {
			if w == nil || !validSubscriptionPercent(w.Utilization) || *w.Utilization >= 100 {
				return false
			}
			if *w.Utilization == 0 && w.ResetsAt == "" {
				return true
			}
			reset, err := time.Parse(time.RFC3339Nano, w.ResetsAt)
			return err == nil && reset.After(now)
		}
		if !healthy(payload.SevenDay) {
			return result
		}
		if string(payload.FiveHour) != "null" {
			var w subscriptionUsageWindow
			if json.Unmarshal(payload.FiveHour, &w) != nil || !healthy(&w) {
				return result
			}
		}
		result.shared = true
		for name, w := range map[string]*subscriptionUsageWindow{"opus": payload.Opus, "sonnet": payload.Sonnet, "fable": payload.Fable} {
			if name == "fable" && w != nil && (w.LimitDollars != nil || w.UsedDollars != nil || w.RemainingDollars != nil) {
				continue
			}
			if w != nil {
				result.models[name] = healthy(w)
			}
		}
		for _, limit := range payload.Limits {
			if !validSubscriptionPercent(limit.Percent) {
				result.shared = false
				return result
			}
			if limit.Kind == "weekly_all" && *limit.Percent >= 100 {
				result.shared = false
			}
			if limit.Kind == "weekly_scoped" {
				name := strings.ToLower(strings.TrimSpace(limit.Scope.Model.Name))
				if strings.HasPrefix(name, "fable ") {
					name = "fable"
				}
				if name == "fable" || name == "opus" || name == "sonnet" {
					prior, known := result.models[name]
					result.models[name] = (!known || prior) && *limit.Percent < 100
				} else if limit.Active && *limit.Percent >= 100 {
					result.shared = false
				}
			} else if limit.Kind != "weekly_all" && limit.Active && *limit.Percent >= 100 {
				result.shared = false
			}
		}
		return result
	}
	if provider == "codex" {
		type window struct {
			Used  *float64 `json:"used_percent"`
			Reset int64    `json:"reset_at"`
		}
		type rate struct {
			Allowed   *bool   `json:"allowed"`
			Reached   *bool   `json:"limit_reached"`
			Primary   *window `json:"primary_window"`
			Secondary *window `json:"secondary_window"`
		}
		var payload struct {
			Rate       *rate `json:"rate_limit"`
			Additional []struct {
				Rate *rate `json:"rate_limit"`
			} `json:"additional_rate_limits"`
		}
		if json.Unmarshal(body, &payload) != nil {
			return result
		}
		healthy := func(r *rate) bool {
			if r == nil || r.Allowed == nil || !*r.Allowed || r.Reached == nil || *r.Reached || r.Primary == nil && r.Secondary == nil {
				return false
			}
			for _, w := range []*window{r.Primary, r.Secondary} {
				if w != nil && (!validSubscriptionPercent(w.Used) || *w.Used >= 100 || !time.Unix(w.Reset, 0).After(now)) {
					return false
				}
			}
			return true
		}
		result.shared = healthy(payload.Rate)
		// Unknown resource-to-model mappings cannot prove that a model recovered.
		for _, extra := range payload.Additional {
			if !healthy(extra.Rate) {
				result.shared = false
			}
		}
	}
	return result
}

func (a subscriptionAvailability) allows(model string) bool {
	if !a.shared {
		return false
	}
	model = strings.ToLower(model)
	for family, allowed := range a.models {
		if strings.Contains(model, family) {
			return allowed
		}
	}
	return true
}

func (a subscriptionAvailability) allowsModelQuota(model string, q QuotaState) bool {
	if !a.allows(model) {
		return false
	}
	if !a.claude {
		return true
	}
	for _, family := range []string{"fable", "opus", "sonnet"} {
		if !strings.Contains(strings.ToLower(model), family) {
			continue
		}
		if allowed, known := a.models[family]; known {
			return allowed
		}
		// Shared subscription rejections can be cleared from shared usage. A
		// model-only rejection requires that model's own returned observation.
		for key, value := range q.cooldownSignals {
			if (strings.EqualFold(key, "Anthropic-Ratelimit-Unified-5h-Status") || strings.EqualFold(key, "Anthropic-Ratelimit-Unified-7d-Status")) && value == "rejected" {
				return true
			}
		}
		return false
	}
	return true
}

func subscriptionQuotaFailure(q QuotaState, err *Error) bool {
	if !q.Exceeded || q.Reason != "quota" && q.Reason != "credential_quota" || err != nil && (err.HTTPStatus != http.StatusTooManyRequests || isForceCooldownError(err)) {
		return false
	}
	if err != nil {
		text := strings.ToLower(err.Message)
		for _, s := range []string{"per minute", "concurrent", "capacity"} {
			if strings.Contains(text, s) {
				return false
			}
		}
	}
	return true
}

// SubscriptionQuotaNeedsRefresh excludes disabled credentials, API keys, and failures
// a usage read cannot repair. Healthy credentials incur no background requests.
func SubscriptionQuotaNeedsRefresh(a *Auth, now time.Time) bool {
	if a == nil || a.Disabled || a.Status == StatusDisabled || a.RequiresSignIn || hasUnauthorizedAuthFailure(a) || (a.Provider != "claude" && a.Provider != "codex") || authMetadataString(a, "access_token") == "" || strings.TrimSpace(a.Attributes["api_key"]) != "" {
		return false
	}
	if a.credQuotaForceUntil.After(now) || effectiveAuthForceUntil(a).After(now) || a.Attributes["base_url"] != "" {
		return false
	}
	// ChatGPT subscription usage does not describe the separate SIWC grant.
	if a.Provider == "codex" {
		if useSIWC, _ := parseBoolAny(a.Metadata["use_siwc"]); useSIWC {
			return false
		}
	}
	if subscriptionCredentialFailure(a) && isAuthActiveCooldown(a, now) {
		return true
	}
	for _, s := range a.ModelStates {
		if s != nil && subscriptionQuotaFailure(s.Quota, s.LastError) && isModelStateActiveCooldown(s, now) {
			return true
		}
	}
	return false
}

func subscriptionCredentialFailure(a *Auth) bool {
	// Model results also replace LastError; the credential quota owns its own layer.
	if a.Quota.Reason == "credential_quota" {
		return subscriptionQuotaFailure(a.Quota, nil)
	}
	return subscriptionQuotaFailure(a.Quota, a.LastError)
}

func sameSubscriptionQuotaCooldown(a, b QuotaState) bool {
	return a.Exceeded == b.Exceeded && a.Reason == b.Reason && a.NextRecoverAt.Equal(b.NextRecoverAt) && a.BackoffLevel == b.BackoffLevel && maps.Equal(a.cooldownSignals, b.cooldownSignals)
}

func subscriptionCooldownSnapshotCurrent(a, base *Auth) bool {
	if a.subscriptionQuotaRevision != base.subscriptionQuotaRevision || !sameSubscriptionQuotaCooldown(a.Quota, base.Quota) || !a.credentialRetryAfter.Equal(base.credentialRetryAfter) {
		return false
	}
	for model, state := range a.ModelStates {
		before := base.ModelStates[model]
		if state == nil || !state.Quota.Exceeded {
			continue
		}
		if before == nil || !sameSubscriptionQuotaCooldown(state.Quota, before.Quota) || !state.UpdatedAt.Equal(before.UpdatedAt) {
			return false
		}
	}
	return true
}

// ReconcileSubscriptionQuota consumes only a provider usage GET performed by the
// host. The pre-request snapshot fences delayed responses and replacement accounts.
// It never redeems a reset, renews credentials, or marks inference as verified.
func (m *Manager) ReconcileSubscriptionQuota(ctx context.Context, base *Auth, body []byte) (bool, error) {
	if m == nil || base == nil || m.HomeEnabled() {
		return false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	now := time.Now()
	available := subscriptionAvailabilityFromBody(base.Provider, body, now)
	if !available.shared {
		return false, nil
	}
	release := m.lockAuthMutation(base.ID)
	defer release()
	m.mu.Lock()
	a := m.auths[base.ID]
	if a == nil || m.HomeEnabled() || a.RegistrationEpoch != base.RegistrationEpoch || a.CredentialVersion != base.CredentialVersion || CredentialsChanged(a, base) || !subscriptionCooldownSnapshotCurrent(a, base) || !SubscriptionQuotaNeedsRefresh(a, now) || ctx.Err() != nil {
		m.mu.Unlock()
		return false, ctx.Err()
	}
	changed := false
	if subscriptionCredentialFailure(a) && (a.Quota.Reason == "credential_quota" || len(a.ModelStates) == 0) {
		applyCooldownFields(&a.Quota, QuotaState{})
		a.credQuotaForceUntil = time.Time{}
		a.Unavailable = false
		a.NextRetryAfter = time.Time{}
		for model, s := range a.ModelStates {
			if s == nil || s.Status == StatusDisabled || !(s.credentialQuotaPropagated || s.Quota.Reason == "credential_quota") {
				continue
			}
			if shadow := s.credentialQuotaShadow; shadow != nil && isModelStateActiveCooldown(shadow, now) {
				*s = *shadow.cloneShallow()
			} else if available.allows(model) {
				resetModelState(s, now)
			} else {
				s.credentialQuotaPropagated = false
				s.credentialQuotaShadow = nil
				s.Quota.Reason = "quota"
			}
		}
		changed = true
	}
	for model, s := range a.ModelStates {
		if s != nil && s.Status != StatusDisabled && !s.credentialQuotaPropagated && subscriptionQuotaFailure(s.Quota, s.LastError) && !effectiveModelForceUntil(s).After(now) && available.allowsModelQuota(model, s.Quota) {
			resetModelState(s, now)
			changed = true
		}
	}
	if !changed {
		m.mu.Unlock()
		return false, nil
	}
	updateAggregatedAvailability(a, now)
	if !hasModelError(a, now) && !effectiveAuthForceUntil(a).After(now) && !a.credentialRetryAfter.After(now) {
		a.Status = StatusActive
		a.StatusMessage = ""
		a.LastError = nil
	}
	a.Generation++
	a.UpdatedAt = now
	snapshot := a.Clone()
	err := m.persistLocked(ctx, a)
	m.mu.Unlock()
	release()
	models, epoch := registry.GetGlobalRegistry().GetModelsAndEpochForClient(base.ID)
	projections := make([]registry.ClientModelProjection, 0, len(models))
	for _, model := range models {
		if model != nil {
			projections = append(projections, m.clientModelProjectionForAuth(snapshot, model.ID, now))
		}
	}
	registry.GetGlobalRegistry().ApplyClientModelProjections(base.ID, epoch, snapshot.Generation, projections)
	if m.scheduler != nil {
		m.scheduler.upsertAuth(snapshot)
	}
	m.persistCooldownStates(context.Background())
	return true, err
}
