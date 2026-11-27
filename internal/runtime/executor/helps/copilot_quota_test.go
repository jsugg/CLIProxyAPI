package helps

import (
	"net/http"
	"testing"
	"time"
)

func TestParseCopilotQuotaSnapshotHeader(t *testing.T) {
	exhausted := ParseCopilotQuotaSnapshotHeader("crUsed=50000&ent=50000&ov=0.0&ovPerm=false&rem=0.0&rst=2026-12-01T00%3A00%3A00Z&totRem=0.0")
	if !exhausted.Present || !exhausted.Exhausted() {
		t.Fatalf("expected exhausted snapshot, got %+v", exhausted)
	}
	wantReset := time.Date(2026, time.December, 1, 0, 0, 0, 0, time.UTC)
	if !exhausted.ResetAt.Equal(wantReset) {
		t.Fatalf("ResetAt = %v, want %v", exhausted.ResetAt, wantReset)
	}

	unlimited := ParseCopilotQuotaSnapshotHeader("crUsed=596452&ent=-1&ov=0.0&ovPerm=true&rem=100.0&rst=2026-12-01T00%3A00%3A00Z&totRem=-1")
	if !unlimited.Present || unlimited.Exhausted() || !unlimited.Unlimited {
		t.Fatalf("expected healthy unlimited snapshot, got %+v", unlimited)
	}

	overageAllowed := ParseCopilotQuotaSnapshotHeader("crUsed=50000&ent=50000&ov=10.0&ovPerm=true&rem=0.0&rst=2026-12-01T00%3A00%3A00Z&totRem=0.0")
	if overageAllowed.Exhausted() {
		t.Fatalf("overage-permitted snapshot must not be exhausted: %+v", overageAllowed)
	}

	roundedZeroWithCreditsLeft := ParseCopilotQuotaSnapshotHeader("crUsed=49999&ent=50000&ov=0.0&ovPerm=false&rem=0.0&rst=2026-12-01T00%3A00%3A00Z&totRem=1.0")
	if roundedZeroWithCreditsLeft.Exhausted() {
		t.Fatalf("snapshot with totRem=1 must not be exhausted: %+v", roundedZeroWithCreditsLeft)
	}
}

func TestParseCopilotQuotaError(t *testing.T) {
	now := time.Date(2026, time.November, 27, 15, 8, 10, 0, time.UTC)
	reset := time.Date(2026, time.December, 1, 0, 0, 0, 0, time.UTC)
	wantDelay := reset.Sub(now)

	t.Run("402 monthly quota with RFC1123 UTC Retry-After", func(t *testing.T) {
		headers := http.Header{
			"Retry-After":                           {"Tue, 01 Dec 2026 00:00:00 UTC"},
			"X-Quota-Snapshot-Premium_interactions": {"crUsed=50000&ent=50000&ov=0.0&ovPerm=false&rem=0.0&rst=2026-12-01T00%3A00%3A00Z&totRem=0.0"},
		}
		body := []byte(`{"error":{"message":"You have exceeded your monthly quota","code":"quota_exceeded"}}`)
		credQuota, delay := ParseCopilotQuotaError(http.StatusPaymentRequired, headers, body, now)
		if !credQuota {
			t.Fatal("expected credentialQuota=true")
		}
		if delay == nil || *delay != wantDelay {
			t.Fatalf("retryAfter = %v, want %v", delay, wantDelay)
		}
	})

	t.Run("402 falls back to X-Quota-Snapshot rst when Retry-After missing", func(t *testing.T) {
		headers := http.Header{
			"X-Quota-Snapshot-Premium_interactions": {"crUsed=50000&ent=50000&ov=0.0&ovPerm=false&rem=0.0&rst=2026-12-01T00%3A00%3A00Z&totRem=0.0"},
		}
		body := []byte(`{"error":{"message":"You have exceeded your monthly quota","code":"quota_exceeded"}}`)
		credQuota, delay := ParseCopilotQuotaError(http.StatusPaymentRequired, headers, body, now)
		if !credQuota || delay == nil || *delay != wantDelay {
			t.Fatalf("credQuota=%v delay=%v, want true/%v", credQuota, delay, wantDelay)
		}
	})

	t.Run("402 past reset timestamp returns nil delay for backoff", func(t *testing.T) {
		afterReset := reset.Add(2 * time.Minute)
		headers := http.Header{
			"Retry-After":                           {"Tue, 01 Dec 2026 00:00:00 UTC"},
			"X-Quota-Snapshot-Premium_interactions": {"crUsed=50000&ent=50000&ov=0.0&ovPerm=false&rem=0.0&rst=2026-12-01T00%3A00%3A00Z&totRem=0.0"},
		}
		body := []byte(`{"error":{"message":"You have exceeded your monthly quota","code":"quota_exceeded"}}`)
		credQuota, delay := ParseCopilotQuotaError(http.StatusPaymentRequired, headers, body, afterReset)
		if !credQuota || delay != nil {
			t.Fatalf("credQuota=%v delay=%v, want true/nil", credQuota, delay)
		}
	})

	t.Run("non-quota 402 stays unescalated", func(t *testing.T) {
		credQuota, delay := ParseCopilotQuotaError(http.StatusPaymentRequired, http.Header{}, []byte(`{"error":{"message":"payment method invalid"}}`), now)
		if credQuota || delay != nil {
			t.Fatalf("credQuota=%v delay=%v, want false/nil", credQuota, delay)
		}
	})

	t.Run("429 parses RFC1123 UTC Retry-After without false credential escalation", func(t *testing.T) {
		headers := http.Header{
			"Retry-After": {"Fri, 27 Nov 2026 15:09:10 UTC"},
		}
		credQuota, delay := ParseCopilotQuotaError(http.StatusTooManyRequests, headers, []byte(`{"error":{"code":"rate_limit_exceeded"}}`), now)
		if credQuota {
			t.Fatal("ordinary 429 must not escalate without quota exhaustion")
		}
		if delay == nil || *delay != time.Minute {
			t.Fatalf("retryAfter = %v, want 1m", delay)
		}
	})
}
