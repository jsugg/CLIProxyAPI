package cliproxy

import (
	"context"
	"errors"
	"sort"
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
type subscriptionQuotaMonitor struct {
	manager *coreauth.Manager
	fetch   func(context.Context, *coreauth.Auth) ([]byte, error)
	checks  map[string]subscriptionQuotaCheck
}

func newSubscriptionQuotaMonitor(m *coreauth.Manager, fetch func(context.Context, *coreauth.Auth) ([]byte, error)) *subscriptionQuotaMonitor {
	return &subscriptionQuotaMonitor{manager: m, fetch: fetch, checks: make(map[string]subscriptionQuotaCheck)}
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

// Only cooled OAuth accounts are read, at most once per five minutes. Neither a
// page visit nor the optional history collector is needed for recovery.
func (m *subscriptionQuotaMonitor) check(ctx context.Context, now time.Time) {
	if ctx.Err() != nil || m.manager.HomeEnabled() {
		return
	}
	auths := m.manager.List()
	sort.Slice(auths, func(i, j int) bool { return auths[i].ID < auths[j].ID })
	live := make(map[string]bool)
	var due []*coreauth.Auth
	for _, a := range auths {
		if !coreauth.SubscriptionQuotaNeedsRefresh(a, now) || a.Attributes["base_url"] != "" {
			continue
		}
		live[a.ID] = true
		check, ok := m.checks[a.ID]
		if !ok {
			check.next = a.UpdatedAt.Add(time.Minute)
			m.checks[a.ID] = check
		}
		if !check.next.After(now) && len(due) < 64 {
			due = append(due, a)
		}
	}
	for id := range m.checks {
		if !live[id] {
			delete(m.checks, id)
		}
	}
	type outcome struct {
		id        string
		err       error
		recovered bool
	}
	results := make(chan outcome, len(due))
	jobs := make(chan *coreauth.Auth)
	var workers sync.WaitGroup
	for i := 0; i < 2; i++ {
		workers.Go(func() {
			for a := range jobs {
				if ctx.Err() != nil {
					continue
				}
				body, err := m.fetch(ctx, a)
				recovered := false
				if err == nil {
					recovered, err = m.manager.ReconcileSubscriptionQuota(ctx, a, body)
				}
				results <- outcome{a.ID, err, recovered}
			}
		})
	}
	for _, a := range due {
		select {
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			return
		case jobs <- a:
		}
	}
	close(jobs)
	workers.Wait()
	close(results)
	for result := range results {
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
