package quotahistory

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/MoeForge/moego/json"
)

// Prefix is the only route subtree owned by this companion.
const Prefix = "/v8/management/quota-history"

func validKey(key string) bool {
	if len(key) < 1 || len(key) > 4096 {
		return false
	}
	for _, r := range key {
		if r < 33 || r > 126 {
			return false
		}
	}
	return true
}

// ReadKey reads the existing key from a private regular file without following links.
func ReadKey(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", errors.New("key file must be absolute")
	}
	if err := privatePath(path, false); err != nil {
		return "", errors.New("key file must be private, regular and not a symlink")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", errors.New("cannot open key file")
	}
	defer func() { _ = file.Close() }()
	raw, err := io.ReadAll(io.LimitReader(file, 4098))
	if err != nil || len(raw) > 4097 {
		return "", errors.New("cannot read bounded key file")
	}
	key := strings.TrimSuffix(strings.TrimSuffix(string(raw), "\n"), "\r")
	if !validKey(key) {
		return "", errors.New("invalid management key file")
	}
	return key, nil
}

type handler struct {
	store  *Store
	digest [32]byte
	status func() Status
}

// NewHandler authenticates all history reads with the existing administrator key.
func NewHandler(store *Store, key string, status func() Status) http.Handler {
	return &handler{store: store, digest: sha256.Sum256([]byte("Bearer " + key)), status: status}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	raw, err := json.Marshal(value)
	if err != nil {
		http.Error(w, "response encoding failed", 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.URL.Path == "/healthz" {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			writeJSON(w, 405, map[string]string{"error": "method_not_allowed"})
			return
		}
		writeJSON(w, 200, map[string]string{"status": "ok"})
		return
	}
	digest := sha256.Sum256([]byte(r.Header.Get("Authorization")))
	if subtle.ConstantTimeCompare(digest[:], h.digest[:]) != 1 {
		writeJSON(w, 401, map[string]string{"error": "unauthorized"})
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeJSON(w, 405, map[string]string{"error": "method_not_allowed"})
		return
	}
	if len(r.RequestURI) > 4096 {
		writeJSON(w, 400, map[string]string{"error": "request_too_large"})
		return
	}
	if r.URL.Path == Prefix+"/capabilities" {
		writeJSON(w, 200, struct {
			SchemaVersion       int      `json:"schema_version"`
			PollIntervalSeconds int      `json:"poll_interval_seconds"`
			RetentionDays       int      `json:"retention_days"`
			Providers           []string `json:"providers"`
			MaxQueryDays        int      `json:"max_query_days"`
			MaxPageSize         int      `json:"max_page_size"`
			MaxAccounts         int      `json:"max_accounts"`
			MaxSamples          int      `json:"max_samples"`
			Status              Status   `json:"status"`
		}{1, 300, 30, []string{"claude", "codex"}, 30, MaxPageSize, MaxAccounts, MaxSamples, h.status()})
		return
	}
	if r.URL.Path != Prefix {
		writeJSON(w, 404, map[string]string{"error": "not_found"})
		return
	}
	params, err := urlQuery(r)
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid_query"})
		return
	}
	from, errFrom := time.Parse(time.RFC3339Nano, params["from"])
	to, errTo := time.Parse(time.RFC3339Nano, params["to"])
	limit := 1000
	var errLimit error
	if raw, exists := params["limit"]; exists {
		limit, errLimit = strconv.Atoi(raw)
	}
	q := Query{From: from, To: to, Provider: params["provider"], Limit: limit, Cursor: params["cursor"]}
	if errFrom != nil || errTo != nil || errLimit != nil || q.validate() != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid_query"})
		return
	}
	page, err := h.store.Query(q)
	if err != nil {
		if strings.Contains(err.Error(), "cursor") {
			writeJSON(w, 400, map[string]string{"error": "invalid_cursor"})
		} else {
			writeJSON(w, 500, map[string]string{"error": "storage_read"})
		}
		return
	}
	writeJSON(w, 200, page)
}

func urlQuery(r *http.Request) (map[string]string, error) {
	values, err := r.URL.Query(), r.ParseForm()
	if err != nil {
		return nil, err
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		switch key {
		case "from", "to", "provider", "limit", "cursor":
		default:
			return nil, errors.New("unknown query parameter")
		}
		if len(value) != 1 || value[0] == "" {
			return nil, errors.New("duplicate or empty query parameter")
		}
		result[key] = value[0]
	}
	return result, nil
}
