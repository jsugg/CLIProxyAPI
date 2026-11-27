# Subscription quota recovery

Claude, Codex, and GitHub Copilot OAuth accounts with active quota cooldowns are
rechecked even when the dashboard is closed. The service reads the provider's
usage endpoint; it never redeems a reset or sends an inference request. Healthy
accounts, API keys, custom upstreams, disabled accounts, and accounts requiring
sign-in are excluded. Active Copilot credentials are probed once at startup or
credential replacement so seats that are already out of monthly quota are cooled
before receiving traffic. Home continues to own its own routing state.

The first read is eligible one minute after the last state update, checked every
30 seconds. Subsequent reads are at least five minutes apart. Two workers limit
concurrency. Failures increase the interval up to 30 minutes, and an upstream
Retry-After can delay it further. Shutdown cancels the reads.

Successful quota reads release only quota blocks for which the returned usage
proves capacity is available. Unknown, malformed, stale, or exhausted windows
preserve the cooldown. A snapshot from before another rejection, credential
replacement, or disable operation cannot clear the newer state. Independent
model failures, authentication failures, and forced cooldowns remain intact.
Routing, model registry projections, and persisted cooldowns update together.

The v8 dashboard's authenticated usage reads also reconcile cooldowns. Callers
can request an explicit read with:

```http
POST /v8/management/routing/cooldown/refresh
Content-Type: application/json

{"auth_index":"<credential index>"}
```

The response contains `status`, `auth_index`, and `recovered`. A successful read
with `recovered: false` does not imply that all limits have been restored.
This differs from `/routing/cooldown/reset`, which is the existing unconditional
administrative clear.

After redeeming an exact Anthropic grant, the dashboard uses this authoritative
read instead of unconditionally clearing all routing blocks. Fable grants refill
Fable usage and return its consumed share to the all-model weekly allowance.
For example, if Fable has consumed its entire 50% allocation, that refund can
make 50% of the weekly all-model allowance available to Opus. Usage by other
models remains charged. No local percentage subtraction predicts the refund;
the provider's post-reset usage decides routing eligibility.

Regression tests use synthetic provider responses and simulated claims. They do
not redeem real subscription resets.

## Review notes and limits

Recovery fences quota rejections separately from ordinary request completions.
Successful traffic on another model does not invalidate the usage read. Token
reloads preserve the process-owned rejection revision. Dollar-denominated
`iguana_necktie` balances cannot prove Fable capacity; a scoped Fable observation
is required in that case. Versioned names such as `Fable 5.2` remain model-scoped.
Zero usage can recover without a reset timestamp; missing utilization remains
unknown and does not authorize recovery.

In Home mode, the refresh endpoint acknowledges `managed_by: "home"` with
`recovered: false`. It does not alter Home-owned routing state or turn an already
confirmed provider reset into a dashboard failure.

A stalled upstream read can still occupy a background worker until cancellation.
The repository's `AGENTS.md` prohibits adding a timeout after an upstream
connection is established. This patch preserves that rule; it does not introduce
a new network timeout exception. Shutdown cancels outstanding reads.
