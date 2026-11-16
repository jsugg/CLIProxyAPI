package quotahistory

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/MoeForge/moego/json"
)

type credential struct {
	Provider  string
	AuthIndex string
	Account   string
	Identity  string
	Plan      string
}

type object map[string]json.RawMessage

func decodeObject(raw []byte) object {
	var value object
	if json.Unmarshal(raw, &value) != nil {
		return nil
	}
	return value
}

func (o object) first(keys ...string) json.RawMessage {
	for _, key := range keys {
		if raw := o[key]; len(raw) > 0 && string(raw) != "null" {
			return raw
		}
	}
	return nil
}

func textValue(raw []byte) string {
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return ""
	}
	return strings.TrimSpace(s)
}

func numberValue(raw []byte) *float64 {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var n float64
	if err := json.Unmarshal(raw, &n); err != nil {
		s := textValue(raw)
		if s == "" {
			return nil
		}
		var errParse error
		n, errParse = strconv.ParseFloat(s, 64)
		if errParse != nil {
			return nil
		}
	}
	if math.IsNaN(n) || math.IsInf(n, 0) {
		return nil
	}
	return &n
}

func positiveSeconds(raw []byte) *int64 {
	n := numberValue(raw)
	if n == nil || *n <= 0 || *n > 366*24*60*60 || math.Trunc(*n) != *n {
		return nil
	}
	seconds := int64(*n)
	return &seconds
}

func resetTime(raw []byte) *time.Time {
	if s := textValue(raw); s != "" {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil && t.Year() >= 2020 && t.Year() <= 2200 {
			utc := t.UTC()
			return &utc
		}
	}
	n := numberValue(raw)
	if n == nil {
		return nil
	}
	seconds := *n
	if seconds > 1e12 {
		seconds /= 1000
	}
	if seconds < 1577836800 || seconds > 7258118400 {
		return nil
	}
	t := time.Unix(int64(seconds), 0).UTC()
	return &t
}

func unknownRow(c credential, now time.Time, reason string) Sample {
	return Sample{ObservedAt: now.UTC(), Provider: c.Provider, AuthIndex: c.AuthIndex, IdentityQuality: "credential", LimitID: "unknown", State: "unknown", Source: "management-api", Reason: &reason}
}

func windowRow(c credential, now time.Time, id string, window object, period *int64, percentKeys ...string) Sample {
	row := unknownRow(c, now, "missing_usage")
	row.LimitID = id
	row.PeriodSeconds = period
	row.ResetAt = resetTime(window.first("resets_at", "reset_at", "resetAt"))
	if row.ResetAt == nil {
		if offset := numberValue(window.first("reset_after_seconds", "resetAfterSeconds")); offset != nil && *offset >= 0 && *offset <= 366*24*60*60 {
			at := now.Add(time.Duration(*offset * float64(time.Second))).UTC()
			row.ResetAt = &at
		}
	}
	if n := numberValue(window.first(percentKeys...)); n != nil && *n >= 0 && *n <= 100 {
		row.UsedPercent = n
		row.State = "ok"
		row.Reason = nil
	}
	return row
}

func parseUsage(c credential, raw []byte, now time.Time) []Sample {
	payload := decodeObject(raw)
	rows := make([]Sample, 0, 8)
	if c.Provider == "claude" {
		keys := []struct{ key, id string }{{"five_hour", "five-hour"}, {"seven_day", "seven-day"}, {"seven_day_oauth_apps", "seven-day-oauth-apps"}, {"seven_day_opus", "seven-day-opus"}, {"seven_day_sonnet", "seven-day-sonnet"}, {"seven_day_cowork", "seven-day-cowork"}, {"iguana_necktie", "seven-day-fable"}}
		var fable object
		var limits []object
		if json.Unmarshal(payload["limits"], &limits) == nil && len(limits) <= 32 {
			for _, limit := range limits {
				scope := decodeObject(limit["scope"])
				model := decodeObject(scope["model"])
				name := strings.ToLower(textValue(model["display_name"]))
				if textValue(limit["kind"]) == "weekly_scoped" && (name == "fable" || name == "fable 5") && numberValue(limit["percent"]) != nil {
					if fable == nil || string(limit["is_active"]) == "true" {
						fable = limit
					}
					if string(limit["is_active"]) == "true" {
						break
					}
				}
			}
		}
		for _, entry := range keys {
			rawWindow := payload.first(entry.key)
			if len(rawWindow) == 0 {
				continue
			}
			window := decodeObject(rawWindow)
			seconds := int64(604800)
			if entry.key == "five_hour" {
				seconds = 18000
			}
			id := entry.id
			period := &seconds
			if entry.key == "iguana_necktie" {
				dollar := numberValue(window["limit_dollars"]) != nil || numberValue(window["used_dollars"]) != nil || numberValue(window["remaining_dollars"]) != nil
				if dollar {
					id = "cloud-session-credits"
					period = nil
				} else if fable != nil {
					continue
				}
			}
			rows = append(rows, windowRow(c, now, id, window, period, "utilization"))
		}
		if fable != nil {
			seconds := int64(604800)
			rows = append(rows, windowRow(c, now, "seven-day-fable", fable, &seconds, "percent"))
		}
	} else if c.Provider == "codex" {
		add := func(info object, prefix string) {
			for i, keys := range [][]string{{"primary_window", "primaryWindow"}, {"secondary_window", "secondaryWindow"}} {
				rawWindow := info.first(keys...)
				if len(rawWindow) == 0 {
					continue
				}
				window := decodeObject(rawWindow)
				period := positiveSeconds(window.first("limit_window_seconds", "limitWindowSeconds"))
				id := "five-hour"
				if i == 1 {
					id = "weekly"
				}
				if period != nil {
					switch {
					case *period == 18000:
						id = "five-hour"
					case *period == 604800:
						id = "weekly"
					case *period >= 28*24*60*60 && *period <= 31*24*60*60:
						id = "monthly"
					default:
						id = "window-" + strconv.FormatInt(*period, 10)
					}
				}
				// Unknown durations keep legacy row labels, but never acquire an inferred period.
				rows = append(rows, windowRow(c, now, prefix+id, window, period, "used_percent", "usedPercent"))
			}
		}
		add(decodeObject(payload.first("rate_limit", "rateLimit")), "")
		add(decodeObject(payload.first("code_review_rate_limit", "codeReviewRateLimit")), "code-review-")
		var additional []object
		if rawAdditional := payload.first("additional_rate_limits", "additionalRateLimits"); len(rawAdditional) > 0 {
			if json.Unmarshal(rawAdditional, &additional) != nil || len(additional) > 16 {
				return []Sample{unknownRow(c, now, "unsupported_payload")}
			}
			for _, limit := range additional {
				name := textValue(limit.first("limit_name", "limitName", "metered_feature", "meteredFeature"))
				if name == "" || len(name) > 256 {
					rows = append(rows, unknownRow(c, now, "unsupported_limit"))
					continue
				}
				sum := sha256.Sum256([]byte(name))
				add(decodeObject(limit.first("rate_limit", "rateLimit")), "additional-"+hex.EncodeToString(sum[:8])+"-")
			}
		}
	}
	if len(rows) == 0 {
		return []Sample{unknownRow(c, now, "unsupported_payload")}
	}
	return rows
}
