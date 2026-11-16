package quotahistory

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MoeForge/moego/json"
)

const (
	claudeUsageURL   = "https://api.anthropic.com/api/oauth/usage"
	claudeProfileURL = "https://api.anthropic.com/api/oauth/profile"
	codexUsageURL    = "https://chatgpt.com/backend-api/wham/usage"
	maxResponseBytes = 4 << 20
)

// Status exposes fixed diagnostics, never upstream errors or credentials.
type Status struct {
	StartedAt           time.Time  `json:"started_at"`
	LastPollAt          *time.Time `json:"last_poll_at"`
	LastSuccessAt       *time.Time `json:"last_success_at"`
	LastSampleAt        *time.Time `json:"last_sample_at"`
	ConsecutiveFailures int        `json:"consecutive_failures"`
	LastError           *string    `json:"last_error"`
	Stale               bool       `json:"stale"`
}

// Options permits deterministic clocks, scheduling, jitter and synthetic transports.
type Options struct {
	Now       func() time.Time
	Transport http.RoundTripper
	Jitter    func(time.Duration) time.Duration
	Wait      func(context.Context, time.Duration) error
}

type retryState struct {
	Failures int
	Next     time.Time
}

// Collector schedules only fixed, read-only management quota probes.
type Collector struct {
	store       *Store
	origin, key string
	client      *http.Client
	opts        Options
	pollMu      sync.Mutex
	mu          sync.Mutex
	status      Status
	retries     map[string]retryState
}

func validateOrigin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || u.Port() == "" {
		return "", errors.New("management origin must be a literal loopback HTTP origin with a port")
	}
	ip := net.ParseIP(u.Hostname())
	port, errPort := strconv.Atoi(u.Port())
	if ip == nil || !ip.IsLoopback() || errPort != nil || port < 1 || port > 65535 {
		return "", errors.New("management origin must be loopback")
	}
	return u.String(), nil
}

// ValidateListen rejects wildcard, DNS and non-loopback bind addresses.
func ValidateListen(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return errors.New("listen address must be literal loopback host:port")
	}
	ip := net.ParseIP(host)
	n, errPort := strconv.Atoi(port)
	if ip == nil || !ip.IsLoopback() || errPort != nil || n < 1 || n > 65535 {
		return errors.New("listen address must be literal loopback host:port")
	}
	return nil
}

func waitContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// NewCollector validates the native origin and disables proxies and redirects.
func NewCollector(store *Store, origin, key string, opts Options) (*Collector, error) {
	origin, err := validateOrigin(origin)
	if err != nil {
		return nil, err
	}
	if store == nil || !validKey(key) {
		return nil, errors.New("store and valid management key required")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Wait == nil {
		opts.Wait = waitContext
	}
	if opts.Jitter == nil {
		opts.Jitter = func(max time.Duration) time.Duration { return time.Duration(rand.Int64N(int64(max) + 1)) }
	}
	transport := opts.Transport
	if transport == nil {
		transport = &http.Transport{MaxConnsPerHost: 2, MaxIdleConns: 4, IdleConnTimeout: 60 * time.Second}
	}
	return &Collector{store: store, origin: origin, key: key, opts: opts, client: &http.Client{Transport: transport, Timeout: 35 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, status: Status{StartedAt: opts.Now().UTC(), Stale: true}, retries: make(map[string]retryState)}, nil
}

type operationError struct {
	Code       string
	RetryAfter time.Duration
}

func (e *operationError) Error() string { return e.Code }

func (c *Collector) call(ctx context.Context, path string, payload any) ([]byte, error) {
	method := http.MethodGet
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, &operationError{Code: "encode_request"}
		}
		body = bytes.NewReader(raw)
		method = http.MethodPost
	}
	req, err := http.NewRequestWithContext(ctx, method, c.origin+path, body)
	if err != nil {
		return nil, &operationError{Code: "management_request"}
	}
	req.Header.Set("Authorization", "Bearer "+c.key)
	req.Header.Set("Content-Type", "application/json")
	res, err := c.client.Do(req)
	if err != nil {
		return nil, &operationError{Code: "management_transport"}
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		return nil, &operationError{Code: "management_http_" + strconv.Itoa(res.StatusCode), RetryAfter: retryAfter(res.Header.Get("Retry-After"), c.opts.Now())}
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, maxResponseBytes+1))
	if err != nil {
		return nil, &operationError{Code: "management_read"}
	}
	if len(raw) > maxResponseBytes {
		return nil, &operationError{Code: "management_response_limit"}
	}
	return raw, nil
}

func safeIdentifier(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

func (c *Collector) credentials(ctx context.Context) ([]credential, error) {
	list := make([]credential, 0, MaxAccounts)
	seen := make(map[string]bool)
	for page := 1; page <= 8; page++ {
		raw, err := c.call(ctx, "/v8/management/credentials?page="+strconv.Itoa(page)+"&page_size=128", nil)
		if err != nil {
			return nil, err
		}
		var response struct {
			Files   []object `json:"files"`
			HasMore bool     `json:"has_more"`
		}
		if json.Unmarshal(raw, &response) != nil || response.Files == nil || len(response.Files) > 128 {
			return nil, &operationError{Code: "credentials_payload"}
		}
		for _, file := range response.Files {
			provider := strings.ToLower(textValue(file.first("provider", "type")))
			if provider != "claude" && provider != "codex" {
				continue
			}
			disabled := string(file["disabled"])
			if disabled != "false" && disabled != "0" && disabled != "\"false\"" && disabled != "null" && disabled != "" {
				continue
			}
			idx := textValue(file.first("auth_index", "authIndex"))
			if !safeIdentifier(idx) {
				return nil, &operationError{Code: "credential_index_invalid"}
			}
			cred := credential{Provider: provider, AuthIndex: idx}
			if provider == "codex" {
				// v8 exposes already-sanitized claims, not the underlying OAuth token.
				claims := decodeObject(file["id_token"])
				account := textValue(file.first("chatgpt_account_id", "chatgptAccountId"))
				if account == "" {
					account = textValue(claims.first("chatgpt_account_id", "chatgptAccountId"))
				}
				if safeIdentifier(account) {
					cred.Account = account
				}
				cred.Plan = textValue(claims.first("plan_type", "planType"))
			}
			identity := provider + ":" + idx
			if seen[identity] {
				continue
			}
			seen[identity] = true
			list = append(list, cred)
			if len(list) > MaxAccounts {
				return nil, &operationError{Code: "credential_limit"}
			}
		}
		if !response.HasMore {
			return list, nil
		}
	}
	return nil, &operationError{Code: "enumeration_limit"}
}

type probeRequest struct {
	AuthIndex string            `json:"authIndex"`
	Method    string            `json:"method"`
	URL       string            `json:"url"`
	Header    map[string]string `json:"header"`
}
type probeResponse struct {
	StatusCode int                 `json:"status_code"`
	Header     map[string][]string `json:"header"`
	Body       string              `json:"body"`
}

func (c *Collector) probe(ctx context.Context, cred credential, target string) ([]byte, error) {
	headers := map[string]string{"Authorization": "Bearer $TOKEN$", "Content-Type": "application/json"}
	if cred.Provider == "claude" {
		headers["User-Agent"] = "claude-cli/2.1.280 (external, cli)"
		headers["anthropic-beta"] = "oauth-2025-04-20"
	} else {
		headers["User-Agent"] = "codex-tui/0.149.1 (Mac OS 26.5.2; arm64) iTerm.app/3.6.11 (codex-tui; 0.149.1)"
		if cred.Account != "" {
			headers["Chatgpt-Account-Id"] = cred.Account
		}
	}
	raw, err := c.call(ctx, "/v8/management/requests/api-call", probeRequest{AuthIndex: cred.AuthIndex, Method: "GET", URL: target, Header: headers})
	if err != nil {
		return nil, err
	}
	var res probeResponse
	if json.Unmarshal(raw, &res) != nil || res.StatusCode < 100 || res.StatusCode > 599 {
		return nil, &operationError{Code: "probe_payload"}
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		h := http.Header(res.Header)
		return nil, &operationError{Code: "upstream_http_" + strconv.Itoa(res.StatusCode), RetryAfter: retryAfter(h.Get("Retry-After"), c.opts.Now())}
	}
	return []byte(res.Body), nil
}

func retryAfter(raw string, now time.Time) time.Duration {
	var d time.Duration
	if seconds, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil {
		if seconds > 3600 {
			seconds = 3600
		}
		if seconds > 0 {
			d = time.Duration(seconds) * time.Second
		}
	} else if at, errDate := http.ParseTime(raw); errDate == nil {
		d = at.Sub(now)
	}
	if d < 0 {
		return 0
	}
	if d > time.Hour {
		return time.Hour
	}
	return d
}

func (c *Collector) retry(cred credential, err error, now time.Time) {
	key := cred.Provider + ":" + cred.AuthIndex
	if err == nil {
		delete(c.retries, key)
		return
	}
	state := c.retries[key]
	if state.Failures < 4 {
		state.Failures++
	}
	delay := PollInterval * time.Duration(1<<uint(state.Failures-1))
	if delay > time.Hour {
		delay = time.Hour
	}
	var operation *operationError
	if errors.As(err, &operation) && operation.RetryAfter > delay {
		delay = operation.RetryAfter
	}
	jitter := c.opts.Jitter(delay / 10)
	if jitter < 0 {
		jitter = 0
	}
	if jitter > delay/10 {
		jitter = delay / 10
	}
	delay += jitter
	if delay > time.Hour {
		delay = time.Hour
	}
	state.Next = now.Add(delay)
	c.retries[key] = state
}

func (c *Collector) collect(ctx context.Context, cred credential) ([]Sample, error) {
	ctx, cancel := context.WithTimeout(ctx, 65*time.Second)
	defer cancel()
	now := c.opts.Now().UTC()
	target := codexUsageURL
	var profileErr error
	if cred.Provider == "claude" {
		target = claudeUsageURL
		profile, err := c.probe(ctx, cred, claudeProfileURL)
		if err == nil {
			obj := decodeObject(profile)
			account := decodeObject(obj["account"])
			org := decodeObject(obj["organization"])
			accountID := textValue(account["uuid"])
			orgID := textValue(org["uuid"])
			if safeIdentifier(accountID) && safeIdentifier(orgID) {
				cred.Identity = accountID + ":" + orgID
			}
			cred.Plan = strings.Join([]string{textValue(org.first("rate_limit_tier", "rateLimitTier")), textValue(org["organization_type"]), textValue(org["subscription_status"]), string(account["has_claude_max"]), string(account["has_claude_pro"])}, "|")
			if cred.Identity == "" {
				profileErr = &operationError{Code: "profile_identity_unknown"}
			}
		} else {
			profileErr = err
		}
	}
	raw, err := c.probe(ctx, cred, target)
	rows := []Sample{unknownRow(cred, now, "probe_failed")}
	if err == nil {
		if cred.Provider == "codex" {
			payload := decodeObject(raw)
			if plan := textValue(payload.first("plan_type", "planType")); plan != "" {
				cred.Plan = plan
			}
		}
		rows = parseUsage(cred, raw, now)
	} else {
		var op *operationError
		if errors.As(err, &op) {
			rows[0].Reason = &op.Code
		}
	}
	accountID, quality, errIdentity := c.store.accountID(cred, now)
	if errIdentity != nil {
		return nil, &operationError{Code: "storage_identity"}
	}
	if len(cred.Plan) > 128 {
		cred.Plan = ""
	}
	for i := range rows {
		row := &rows[i]
		row.AccountID = accountID
		row.IdentityQuality = quality
		period := "unknown"
		if row.PeriodSeconds != nil {
			period = strconv.FormatInt(*row.PeriodSeconds, 10)
		}
		reset := "unknown"
		if row.ResetAt != nil {
			reset = row.ResetAt.Format(time.RFC3339Nano)
		}
		row.SeriesID = c.store.ID("series", accountID, row.LimitID, cred.Plan, period, reset)
		if row.State == "unknown" && err == nil {
			err = &operationError{Code: "usage_unknown"}
		}
	}
	if err == nil && profileErr != nil {
		gap := unknownRow(cred, now, "profile_identity_unknown")
		gap.AccountID = accountID
		gap.IdentityQuality = quality
		gap.SeriesID = c.store.ID("unknown", accountID)
		rows = append(rows, gap)
		err = profileErr
	}
	return rows, err
}

// Poll runs one bounded, non-overlapping collection cycle.
func (c *Collector) Poll(ctx context.Context) error {
	c.pollMu.Lock()
	defer c.pollMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 180*time.Second)
	defer cancel()
	now := c.opts.Now().UTC()
	c.mu.Lock()
	c.status.LastPollAt = &now
	c.mu.Unlock()
	credentials, err := c.credentials(ctx)
	rows := make([]Sample, 0, len(credentials)*8)
	if err != nil {
		var errGaps error
		rows, errGaps = c.store.unknownAccounts(now)
		if errGaps != nil {
			err = &operationError{Code: "storage_identity"}
		}
	}
	mappingRows := append([]Sample(nil), rows...)
	if err == nil {
		current := make(map[string]bool, len(credentials))
		for _, cred := range credentials {
			current[cred.Provider+":"+cred.AuthIndex] = true
		}
		for key := range c.retries {
			if !current[key] {
				delete(c.retries, key)
			}
		}
		type result struct {
			cred credential
			rows []Sample
			err  error
		}
		jobs := make(chan credential)
		results := make(chan result, len(credentials))
		var wg sync.WaitGroup
		ready := make([]credential, 0, len(credentials))
		for _, cred := range credentials {
			if retry := c.retries[cred.Provider+":"+cred.AuthIndex]; retry.Next.After(now) {
				row := unknownRow(cred, now, "backoff")
				id, quality, errIdentity := c.store.accountID(cred, now)
				if errIdentity != nil {
					results <- result{cred, nil, &operationError{Code: "storage_identity"}}
					continue
				}
				row.IdentityQuality = quality
				row.AccountID = id
				row.SeriesID = c.store.ID("unknown", id)
				results <- result{cred, []Sample{row}, &operationError{Code: "backoff"}}
				continue
			}
			ready = append(ready, cred)
		}
		for range 2 {
			wg.Go(func() {
				for cred := range jobs {
					data, errCollect := c.collect(ctx, cred)
					results <- result{cred, data, errCollect}
				}
			})
		}
		go func() {
			defer close(jobs)
			for _, cred := range ready {
				jobs <- cred
			}
		}()
		go func() { wg.Wait(); close(results) }()
		seen := make(map[string]int)
		for result := range results {
			// Preserve every alias mapping before collapsing its shared quota observation.
			mappingRows = append(mappingRows, result.rows...)
			if result.err != nil {
				if err == nil {
					err = result.err
				}
				if result.err.Error() != "backoff" {
					c.retry(result.cred, result.err, now)
				}
			} else {
				c.retry(result.cred, nil, now)
			}
			for _, row := range result.rows {
				key := row.AccountID + ":" + row.LimitID
				if index, exists := seen[key]; exists {
					previous := &rows[index]
					conflict := previous.State != row.State || previous.SeriesID != row.SeriesID || (previous.UsedPercent != nil && row.UsedPercent != nil && *previous.UsedPercent != *row.UsedPercent)
					if conflict {
						reason := "conflicting_alias"
						previous.UsedPercent = nil
						previous.State = "unknown"
						previous.Reason = &reason
					}
					continue
				}
				seen[key] = len(rows)
				rows = append(rows, row)
			}
		}
	}
	completed := c.opts.Now().UTC()
	missing, errMissing := c.store.missingWindows(rows, completed)
	if errMissing != nil {
		err = &operationError{Code: "storage_read"}
	} else if len(missing) > 0 {
		rows = append(rows, missing...)
		if err == nil {
			err = &operationError{Code: "missing_window"}
		}
	}
	if errAppend := c.store.appendObservations(rows, mappingRows, completed); errAppend != nil {
		err = &operationError{Code: "storage_write"}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(rows) > 0 && (err == nil || err.Error() != "storage_write") {
		last := rows[0].ObservedAt
		for _, row := range rows {
			if row.ObservedAt.After(last) {
				last = row.ObservedAt
			}
		}
		c.status.LastSampleAt = &last
	}
	if err != nil {
		c.status.ConsecutiveFailures++
		reason := "collection_failed"
		var op *operationError
		if errors.As(err, &op) {
			reason = op.Code
		}
		c.status.LastError = &reason
	} else {
		c.status.LastSuccessAt = &completed
		c.status.ConsecutiveFailures = 0
		c.status.LastError = nil
	}
	return err
}

// Status returns a race-safe snapshot with clock-derived staleness.
func (c *Collector) Status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	status := c.status
	status.Stale = status.LastSuccessAt == nil || c.opts.Now().Sub(*status.LastSuccessAt) > 3*PollInterval
	return status
}

// Run polls immediately, then waits 300 seconds after each completed cycle.
func (c *Collector) Run(ctx context.Context, onPoll func(Status)) {
	defer c.client.CloseIdleConnections()
	for ctx.Err() == nil {
		_ = c.Poll(ctx)
		if onPoll != nil {
			onPoll(c.Status())
		}
		if c.opts.Wait(ctx, PollInterval) != nil {
			return
		}
	}
}
