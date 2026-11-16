# jnet deployment

This fork is maintained at https://github.com/jsugg/CLIProxyAPI.
The `jnet` branch starts at upstream v8.0.74. Keep runtime changes small and
merge upstream deliberately, after tests. The Go module path remains upstream
to preserve internal imports and SDK compatibility.

One CLIProxyAPI instance can hold both Claude Code and Codex OAuth credentials
in the same private auth directory. Its existing management center lists both
providers behind one administrator key. No custom dashboard or proxy-to-proxy
management layer is required.

The deployment and migration live in https://github.com/jsugg/jnet:

```sh
jnet proxy stop codex-1
jnet proxy stop claude-1
# Wait for shutdown and credential locks to be released, up to 30 seconds.
jnet proxy merge main codex-1 claude-1
jnet proxy start main
jnet proxy dashboard main
```

The first source determines the listener port and inference key. One management
key is retained from the first dashboard-enabled source, or generated if none
exists. Original pools become client aliases, so existing `jnet proxy run`
commands continue to select their native clients. All aliases start and stop
the same service. Only the unified hub appears in unfiltered status output.

Keep the listener on loopback and expose one HTTPS port using Tailscale Serve,
never Funnel. Remove only the retired pool's Serve port. Do not reset other
routes. Provider tokens remain on the Mac. Administrator and inference keys
remain distinct; agents never receive the administrator key.

The hub uses the original model names, without cross-provider aliases or
automatic provider/model fallback. Round-robin routing and session affinity
still operate within each model's eligible accounts. Claude subscriptions are
used through the unmodified Claude Code client.

Native CLI homes and active T3 sessions are not reconfigured. The pilot remains
opt-in, subject to provider terms. The installed binary stays pinned to the
verified upstream v8.0.74 release until a separately verified fork release is
adopted. Creating this fork alone does not change the installed executable.

Before release, run `go test ./...` and `go build -o test-output ./cmd/server`.
The fork adds a push/PR test gate rather than upstream's build-only PR check.
Release creation also waits for the full Go tests and a server build to pass.
Credentials, live configuration and local test logs must never be committed.
