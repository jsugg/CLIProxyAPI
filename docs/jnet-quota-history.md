# Quota history companion

This optional binary collects observations, not inference or billing data. It does
not replace or modify the inference server. No history exists before first launch.

## HTTP contract, schema version 1

Mount only `/v8/management/quota-history` and its children on the same dashboard
origin. Every request requires the existing management `Authorization: Bearer`
key. The binary reads that key from a private file, never a command-line key.
No CORS or alternate login is provided. Responses use `Cache-Control: no-store`.
The sole exception is `GET /healthz`, which returns only `{"status":"ok"}`
without authentication. It means the listener is ready, not that upstream
collection succeeded. Never mount `/healthz` through Serve.

`GET /v8/management/quota-history/capabilities` returns:

```json
{"schema_version":1,"poll_interval_seconds":300,"retention_days":30,"providers":["claude","codex"],"max_query_days":30,"max_page_size":2000,"max_accounts":128,"max_samples":500000,"status":{"started_at":"2026-11-16T00:00:00Z","last_poll_at":null,"last_success_at":null,"last_sample_at":null,"consecutive_failures":0,"last_error":null,"stale":true}}
```

`GET /v8/management/quota-history?provider=claude&from=RFC3339&to=RFC3339&limit=1000`
returns observations in ascending timestamp order. `provider` is optional and
otherwise must be `claude` or `codex`. `from` and `to` are required, inclusive,
at most 30 days apart. `limit` defaults to 1000 and must be 1..2000. Follow
`next_cursor` as the `cursor` query parameter with the same filters and limit until null. Cursors expire
with retention; concurrent deletion never promises a snapshot.

```json
{"schema_version":1,"samples":[{"observed_at":"2026-11-16T00:00:00Z","provider":"claude","account_id":"opaque-stable-id","auth_index":"native-index","identity_quality":"provider","limit_id":"five-hour","series_id":"opaque-window-id","period_seconds":18000,"reset_at":"2026-11-16T04:00:00Z","used_percent":23.5,"state":"ok","source":"management-api","reason":null}],"next_cursor":null}
```

`period_seconds`, `reset_at`, and `used_percent` may be null. `state` is `ok` or
`unknown`. `reason` is null or a fixed diagnostic code, never an upstream body.
Failed probes emit an `unknown` limit row, not a zero or 100% reading. Unsupported
or malformed windows remain unknown. Percentages outside 0..100 are unknown.
Explicitly null, inapplicable window slots are skipped; an entirely unusable
payload remains unknown. Null slots do not imply zero usage or available quota.
If a recently valid window disappears from a partial payload, the collector emits
its limit as `unknown` / `missing_window` for 15 minutes after its last valid
reading. A longer absence already exceeds the maximum safe forecast gap.
`identity_quality` is `provider` when verified non-secret per-user and organization
identifiers are available, otherwise `credential`. Claude profile UUIDs supply
that evidence. The native Codex listing supplies a workspace/account ID but not
the user's ID, so Codex currently keeps credential identity. It never merges
different team members just because their workspace ID matches. Credential
identity cannot deduplicate aliases with different native auth indices.
Do not sum accounts with credential identity without showing that caveat.

`account_id` is a keyed opaque identifier. `series_id` includes account, limit,
known plan/tier, period and reset boundary. Never estimate rates across series
changes, unknown observations, decreasing usage, or missing periods/resets.
Percent per hour measures percentage points, not tokens or dollars. Forecasts
are client estimates assuming the recent rate continues; no server forecast is
provided. A reset change or dropped observations invalidates that estimate.

Status timestamps are null until observed. `last_success_at` requires a complete
poll with no probe failures. Staleness means no successful poll for 15 minutes.
An empty successful poll does not imply any quota is available.
Enumeration failures persist `unknown` / `enumeration_failed` account-wide gaps
for up to 128 most-recent last-known identities. Their enabled roster is unknown.
Before any identities exist, only status can report enumeration failure.

## Collection and security

The companion binds a literal loopback address only. The configured native origin
must also be a literal loopback HTTP origin with an explicit port. The companion
never follows redirects from that local management origin or forwards its
administrator key through a redirect. Its outer management requests and responses
are bounded by time and size.
It lists `/v8/management/credentials`, probes enabled Claude/Codex accounts using
`POST /v8/management/requests/api-call`, native `authIndex`, and `$TOKEN$`.
Only fixed Claude usage/profile and Codex usage URLs are submitted as initial
provider targets. No credential
download, OAuth token parsing, subscription, redemption or inference calls occur.

The native APICall implementation controls the subsequent provider request. It
currently follows provider redirects and reads the provider response using
unbounded `io.ReadAll` before returning the management response. The companion's
4 MiB outer response limit does not bound memory consumed inside the native
process, and its outer cancellation/time limits do not establish a provider-side
redirect policy or a hard resource bound there. This companion does not change
that shared native implementation. Deploying collection therefore accepts this
existing native API dependency; the scoped companion tests do not qualify it as
redirect-safe or memory-bounded.

Collection starts immediately, then every 300 seconds with at most two concurrent
accounts. A completed cycle waits 300 seconds before the next cycle. Each outer
management operation has a 35-second bound, each account 65 seconds, the whole cycle 180
seconds. Failures back off per credential from five minutes to one hour, with up
to 10% bounded jitter. Retry-After is honored within that one-hour ceiling.
Enumeration reads at most eight pages of 128 records and rejects more than 128
distinct enabled supported native auth indices. No emails,
tokens, request headers, raw response bodies or raw provider IDs enter the DB.
Only a validated native auth index and the documented observation fields persist.
All probed aliases retain their credential-to-opaque-account mappings in the same
transaction as the deduplicated observations. A later profile failure therefore
records its gap against the original account, even if that alias's first sample
was removed by deduplication.

State directory permissions are 0700 and the bbolt file is 0600. Retention is
30 days with at most 500,000 observations and 4096 private index mappings.
Mapping capacity eviction removes the oldest observed mappings first, preserving
newly observed aliases rather than choosing by their native index ordering.
A 512 MiB bbolt file ceiling stops new writes
rather than allow unbounded growth. Back up the closed DB to preserve opaque
identity continuity. Losing its identity secret creates new IDs.

## Run

```sh
go build -o quota-history ./cmd/quota-history
./quota-history --listen 127.0.0.1:8318 --management-origin http://127.0.0.1:8317 \
  --key-file /absolute/private/management-key --state-dir /absolute/private/history
```

Use a process supervisor. Key rotation requires restarting only this companion.
Never restart inference to install history. Logs contain fixed diagnostic codes
and counts only. The companion has no inference API or credential mutation route.

## Independent qualification

```sh
go test -race ./internal/quotahistory ./cmd/quota-history
go vet ./internal/quotahistory ./cmd/quota-history
go build -o quota-history ./cmd/quota-history
```

The companion imports neither the inference runtime nor pluginhost. Its scoped
CI gate qualifies only this optional binary. It does not qualify a different
inference engine or waive the repository's full server build/test requirements.
