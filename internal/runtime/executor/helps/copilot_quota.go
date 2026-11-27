package helps

import (
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/MoeForge/moego/ojson"
)

const copilotQuotaResetMaxDelay = 45 * 24 * time.Hour

// CopilotQuotaSnapshot represents a parsed X-Quota-Snapshot-* header value.
type CopilotQuotaSnapshot struct {
	Present             bool
	Unlimited           bool
	OveragePermitted    bool
	HasRemainingPercent bool
	RemainingPercent    float64
	HasTotalRemaining   bool
	TotalRemaining      float64
	HasEntitlement      bool
	Entitlement         float64
	ResetAt             time.Time
}

// Exhausted reports whether the snapshot proves this bucket has no remaining
// capacity and cannot spill into overage.
func (s CopilotQuotaSnapshot) Exhausted() bool {
	if !s.Present || s.Unlimited || s.OveragePermitted {
		return false
	}
	if s.HasTotalRemaining && s.TotalRemaining < 0 {
		return false
	}
	if s.HasEntitlement && s.Entitlement < 0 {
		return false
	}
	if s.HasTotalRemaining && s.TotalRemaining <= 0 {
		return !s.HasRemainingPercent || s.RemainingPercent <= 0
	}
	if s.HasRemainingPercent && s.RemainingPercent <= 0 {
		return !s.HasTotalRemaining || s.TotalRemaining <= 0
	}
	return false
}

// ParseCopilotQuotaSnapshotHeader parses a GitHub Copilot X-Quota-Snapshot-*
// query-encoded header value.
func ParseCopilotQuotaSnapshotHeader(raw string) CopilotQuotaSnapshot {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return CopilotQuotaSnapshot{}
	}
	values, errParse := url.ParseQuery(raw)
	if errParse != nil || len(values) == 0 {
		return CopilotQuotaSnapshot{}
	}
	snap := CopilotQuotaSnapshot{Present: true}
	if ovPerm := strings.TrimSpace(values.Get("ovPerm")); ovPerm != "" {
		if parsed, errBool := strconv.ParseBool(ovPerm); errBool == nil {
			snap.OveragePermitted = parsed
		}
	}
	if ent, ok := parseFiniteSnapshotFloat(values.Get("ent")); ok {
		snap.HasEntitlement = true
		snap.Entitlement = ent
		if ent < 0 {
			snap.Unlimited = true
		}
	}
	if totRem, ok := parseFiniteSnapshotFloat(values.Get("totRem")); ok {
		snap.HasTotalRemaining = true
		snap.TotalRemaining = totRem
		if totRem < 0 {
			snap.Unlimited = true
		}
	}
	if rem, ok := parseFiniteSnapshotFloat(values.Get("rem")); ok {
		snap.HasRemainingPercent = true
		snap.RemainingPercent = rem
	}
	if rst := strings.TrimSpace(values.Get("rst")); rst != "" {
		if t, errTime := time.Parse(time.RFC3339Nano, rst); errTime == nil {
			snap.ResetAt = t.UTC()
		} else if t, errTime = time.Parse(time.RFC3339, rst); errTime == nil {
			snap.ResetAt = t.UTC()
		}
	}
	return snap
}

func parseFiniteSnapshotFloat(raw string) (float64, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	v, errParse := strconv.ParseFloat(raw, 64)
	if errParse != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, false
	}
	return v, true
}

// ParseHTTPTimeWithUTC parses standard HTTP dates plus RFC1123 timestamps that
// use UTC instead of GMT (as emitted by GitHub Copilot's Retry-After header).
func ParseHTTPTimeWithUTC(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	if t, errParse := http.ParseTime(raw); errParse == nil {
		return t.UTC(), true
	}
	if t, errParse := time.Parse(time.RFC1123, raw); errParse == nil {
		return t.UTC(), true
	}
	return time.Time{}, false
}

func parseCopilotRetryAfterHeader(headers http.Header, now time.Time, allowZero bool) *time.Duration {
	raw := strings.TrimSpace(getHeaderCaseInsensitive(headers, "Retry-After"))
	if raw == "" {
		return nil
	}
	if seconds, errParse := strconv.ParseInt(raw, 10, 64); errParse == nil && seconds >= 0 {
		if seconds == 0 {
			if !allowZero {
				return nil
			}
			zero := time.Duration(0)
			return &zero
		}
		if seconds > int64(copilotQuotaResetMaxDelay/time.Second) {
			return nil
		}
		delay := time.Duration(seconds) * time.Second
		return &delay
	}
	if deadline, ok := ParseHTTPTimeWithUTC(raw); ok {
		delay := deadline.Sub(now)
		if delay <= 0 {
			if allowZero {
				zero := time.Duration(0)
				return &zero
			}
			return nil
		}
		if delay > copilotQuotaResetMaxDelay {
			return nil
		}
		return &delay
	}
	return nil
}

func copilotSnapshotResetDelay(snap CopilotQuotaSnapshot, now time.Time) *time.Duration {
	if snap.ResetAt.IsZero() {
		return nil
	}
	delay := snap.ResetAt.Sub(now)
	if delay <= 0 || delay > copilotQuotaResetMaxDelay {
		return nil
	}
	return &delay
}

func copilotErrorBodyIndicatesQuotaExhausted(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	code := strings.TrimSpace(ojson.GetBytesNoCopy(body, "error.code").String())
	if code == "" {
		code = strings.TrimSpace(ojson.GetBytesNoCopy(body, "code").String())
	}
	if strings.EqualFold(code, "quota_exceeded") {
		return true
	}
	msg := strings.TrimSpace(ojson.GetBytesNoCopy(body, "error.message").String())
	if msg == "" {
		msg = strings.TrimSpace(ojson.GetBytesNoCopy(body, "message").String())
	}
	lower := strings.ToLower(msg)
	if strings.Contains(lower, "monthly quota") {
		return true
	}
	return strings.Contains(lower, "quota") &&
		(strings.Contains(lower, "exceeded") || strings.Contains(lower, "exhausted"))
}

// ParseCopilotQuotaError classifies GitHub Copilot quota rejections (402/429)
// and extracts the reset deadline from Retry-After (including RFC1123 UTC dates)
// or X-Quota-Snapshot-* headers.
func ParseCopilotQuotaError(status int, headers http.Header, body []byte, now time.Time) (credentialQuota bool, retryAfter *time.Duration) {
	if status != http.StatusPaymentRequired && status != http.StatusTooManyRequests {
		return false, nil
	}
	premiumSnap := ParseCopilotQuotaSnapshotHeader(getHeaderCaseInsensitive(headers, "X-Quota-Snapshot-Premium_interactions"))
	chatSnap := ParseCopilotQuotaSnapshotHeader(getHeaderCaseInsensitive(headers, "X-Quota-Snapshot-Chat"))
	bodyQuota := copilotErrorBodyIndicatesQuotaExhausted(body)
	snapshotExhausted := premiumSnap.Exhausted() || chatSnap.Exhausted()

	if status == http.StatusPaymentRequired {
		if !bodyQuota && !snapshotExhausted {
			return false, nil
		}
		delay := parseCopilotRetryAfterHeader(headers, now, false)
		if delay == nil && (premiumSnap.Exhausted() || bodyQuota) {
			delay = copilotSnapshotResetDelay(premiumSnap, now)
		}
		if delay == nil && chatSnap.Exhausted() {
			delay = copilotSnapshotResetDelay(chatSnap, now)
		}
		return true, delay
	}

	// HTTP 429: preserve Retry-After (including UTC HTTP dates) and escalate to
	// credential quota only when quota exhaustion is explicitly reported.
	isQuota := bodyQuota || snapshotExhausted
	delay := parseCopilotRetryAfterHeader(headers, now, !isQuota)
	if isQuota && delay == nil {
		if premiumSnap.Exhausted() || bodyQuota {
			delay = copilotSnapshotResetDelay(premiumSnap, now)
		}
		if delay == nil && chatSnap.Exhausted() {
			delay = copilotSnapshotResetDelay(chatSnap, now)
		}
	}
	return isQuota, delay
}
