package cliproxy

import (
	"context"
	"crypto/sha256"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

type subscriptionQuotaCheck struct {
	next     time.Time
	failures int
}

type copilotInitialProbeIdentity struct {
	epoch     uint64
	tokenHash [32]byte
}

type subscriptionQuotaMonitor struct {
	manager       *coreauth.Manager
	fetch         func(context.Context, *coreauth.Auth) ([]byte, error)
	checks        map[string]subscriptionQuotaCheck
	initialChecks map[string]subscriptionQuotaCheck
	probed        map[string]copilotInitialProbeIdentity
}

func newSubscriptionQuotaMonitor(m *coreauth.Manager, fetch func(context.Context, *coreauth.Auth) ([]byte, error)) *subscriptionQuotaMonitor {
	return &subscriptionQuotaMonitor{
		manager:       m,
		fetch:         fetch,
		checks:        make(map[string]subscriptionQuotaCheck),
		initialChecks: make(map[string]subscriptionQuotaCheck),
		probed:        make(map[string]copilotInitialProbeIdentity),
	}
}

func copilotProbeIdentityFor(a *coreauth.Auth) copilotInitialProbeIdentity {
	raw, _ := a.Metadata["refresh_token"].(string)
	return copilotInitialProbeIdentity{
		epoch:     a.RegistrationEpoch,
		tokenHash: sha256.Sum256([]byte(strings.TrimSpace(raw))),
	}
}

func (s *Service) monitorSubscriptionQuota(ctx context.Context) {
	monitor := newSubscriptionQuotaMonitor(s.coreManager, func(ctx context.Context, a *coreauth.Auth) ([]byte, error) {
		return helps.FetchSubscriptionQuota(ctx, s.currentConfig(), a)
	})
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	monitor.check(ctx, time.Now())
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			monitor.check(ctx, now)
		}
	}
}

// Cooled OAuth accounts are rechecked at most once per five minutes. Uncooled
// Copilot seats are also probed once at startup or after credential replacement
// so exhausted seats are cooled before routing sends inference traffic to them.
func (m *subscriptionQuotaMonitor) check(ctx context.Context, now time.Time) {
	if ctx.Err() != nil || m.manager.HomeEnabled() {
		return
	}
	auths := m.manager.List()
	sort.Slice(auths, func(i, j int) bool { return auths[i].ID < auths[j].ID })
	live := make(map[string]bool)
	liveInitial := make(map[string]bool)
	present := make(map[string]bool, len(auths))
	type probeJob struct {
		auth     *coreauth.Auth
		initial  bool
		identity copilotInitialProbeIdentity
	}
	var due []probeJob
	for _, a := range auths {
		if a == nil {
			continue
		}
		present[a.ID] = true
		if a.Attributes["base_url"] != "" {
			continue
		}
		if coreauth.SubscriptionQuotaNeedsRefresh(a, now) {
			live[a.ID] = true
			check, ok := m.checks[a.ID]
			if !ok {
				check.next = a.UpdatedAt.Add(time.Minute)
				m.checks[a.ID] = check
			}
			if !check.next.After(now) && len(due) < 64 {
				due = append(due, probeJob{auth: a})
			}
			continue
		}
		if coreauth.SubscriptionQuotaNeedsInitialProbe(a, now) {
			identity := copilotProbeIdentityFor(a)
			if prev, ok := m.probed[a.ID]; ok && prev == identity {
				continue
			}
			liveInitial[a.ID] = true
			check, ok := m.initialChecks[a.ID]
			if !ok {
				check.next = now
				m.initialChecks[a.ID] = check
			}
			if !check.next.After(now) && len(due) < 64 {
				due = append(due, probeJob{auth: a, initial: true, identity: identity})
			}
		}
	}
	for id := range m.checks {
		if !live[id] {
			delete(m.checks, id)
		}
	}
	for id := range m.initialChecks {
		if !liveInitial[id] {
			delete(m.initialChecks, id)
		}
	}
	for id := range m.probed {
		if !present[id] {
			delete(m.probed, id)
		}
	}
	type outcome struct {
		id        string
		initial   bool
		identity  copilotInitialProbeIdentity
		err       error
		recovered bool
	}
	results := make(chan outcome, len(due))
	jobs := make(chan probeJob)
	var workers sync.WaitGroup
	for i := 0; i < 2; i++ {
		workers.Go(func() {
			for job := range jobs {
				if ctx.Err() != nil {
					continue
				}
				body, err := m.fetch(ctx, job.auth)
				recovered := false
				if err == nil {
					recovered, err = m.manager.ReconcileSubscriptionQuota(ctx, job.auth, body)
				}
				results <- outcome{job.auth.ID, job.initial, job.identity, err, recovered}
			}
		})
	}
	for _, job := range due {
		select {
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			return
		case jobs <- job:
		}
	}
	close(jobs)
	workers.Wait()
	close(results)
	for result := range results {
		if result.initial {
			if result.err == nil {
				m.probed[result.id] = result.identity
				delete(m.initialChecks, result.id)
				continue
			}
			check := m.initialChecks[result.id]
			delay := 5 * time.Minute
			check.failures++
			if check.failures > 1 {
				delay *= time.Duration(min(check.failures, 6))
			}
			var upstream *helps.SubscriptionQuotaError
			if errors.As(result.err, &upstream) && upstream.RetryAfter > delay {
				delay = upstream.RetryAfter
			}
			check.next = now.Add(delay)
			m.initialChecks[result.id] = check
			continue
		}
		check := m.checks[result.id]
		delay := 5 * time.Minute
		if result.err != nil {
			check.failures++
			if check.failures > 1 {
				delay *= time.Duration(min(check.failures, 6))
			}
			var upstream *helps.SubscriptionQuotaError
			if errors.As(result.err, &upstream) && upstream.RetryAfter > delay {
				delay = upstream.RetryAfter
			}
		} else {
			check.failures = 0
		}
		check.next = now.Add(delay)
		m.checks[result.id] = check
		if result.recovered {
			log.WithField("provider", "subscription").Info("quota restored; routing cooldown reconciled")
		}
	}
}
