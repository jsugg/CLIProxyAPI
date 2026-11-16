// Package quotahistory collects bounded, privacy-preserving quota observations.
package quotahistory

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/MoeForge/moego/json"
	bolt "go.etcd.io/bbolt"
)

const (
	PollInterval       = 300 * time.Second
	Retention          = 30 * 24 * time.Hour
	MaxAccounts        = 128
	MaxSamples         = 500000
	MaxPageSize        = 2000
	maxDBSize          = 512 << 20
	maxAccountMappings = 4096
)

// Sample contains only allowlisted normalized observation data.
type Sample struct {
	ObservedAt      time.Time  `json:"observed_at"`
	Provider        string     `json:"provider"`
	AccountID       string     `json:"account_id"`
	AuthIndex       string     `json:"auth_index"`
	IdentityQuality string     `json:"identity_quality"`
	LimitID         string     `json:"limit_id"`
	SeriesID        string     `json:"series_id"`
	PeriodSeconds   *int64     `json:"period_seconds"`
	ResetAt         *time.Time `json:"reset_at"`
	UsedPercent     *float64   `json:"used_percent"`
	State           string     `json:"state"`
	Source          string     `json:"source"`
	Reason          *string    `json:"reason"`
}

// Query bounds a single page. Cursors are bound to every filter and the limit.
type Query struct {
	From, To time.Time
	Provider string
	Limit    int
	Cursor   string
}

// Page is the versioned management response.
type Page struct {
	SchemaVersion int      `json:"schema_version"`
	Samples       []Sample `json:"samples"`
	NextCursor    *string  `json:"next_cursor"`
}

// Store owns a private, size-limited bbolt database.
type Store struct {
	db     *bolt.DB
	secret []byte
	dir    string
}

// Open opens or creates stateDir, rejecting symlinks and permissive existing files.
func Open(stateDir string) (*Store, error) {
	if !filepath.IsAbs(stateDir) {
		return nil, errors.New("state directory must be absolute")
	}
	if err := os.MkdirAll(stateDir, 0700); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}
	if err := privatePath(stateDir, true); err != nil {
		return nil, err
	}
	path := filepath.Join(stateDir, "history.db")
	if _, err := os.Lstat(path); err == nil {
		if errPrivate := privatePath(path, false); errPrivate != nil {
			return nil, errPrivate
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second, MaxSize: maxDBSize})
	if err != nil {
		return nil, fmt.Errorf("open history database: %w", err)
	}
	s := &Store{db: db, dir: stateDir}
	err = db.Update(func(tx *bolt.Tx) error {
		if _, errBucket := tx.CreateBucketIfNotExists([]byte("samples")); errBucket != nil {
			return errBucket
		}
		if _, errBucket := tx.CreateBucketIfNotExists([]byte("accounts")); errBucket != nil {
			return errBucket
		}
		meta, errBucket := tx.CreateBucketIfNotExists([]byte("meta"))
		if errBucket != nil {
			return errBucket
		}
		key := meta.Get([]byte("identity-key"))
		if key == nil {
			key = make([]byte, 32)
			if _, errRand := rand.Read(key); errRand != nil {
				return errRand
			}
			if errPut := meta.Put([]byte("identity-key"), key); errPut != nil {
				return errPut
			}
		}
		if len(key) != 32 {
			return errors.New("invalid identity key")
		}
		s.secret = bytes.Clone(key)
		return nil
	})
	if err != nil {
		errClose := db.Close()
		return nil, errors.Join(err, errClose)
	}
	return s, nil
}

func privatePath(path string, dir bool) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	want := os.FileMode(0600)
	if dir {
		want = 0700
	}
	if info.Mode()&os.ModeSymlink != 0 || info.IsDir() != dir || (!dir && !info.Mode().IsRegular()) || info.Mode().Perm() != want {
		return errors.New("state path must be private, regular and not a symlink")
	}
	return nil
}

// Close releases the exclusive database lock.
func (s *Store) Close() error { return s.db.Close() }

// ID derives an opaque stable ID without storing its source identity.
func (s *Store) ID(parts ...string) string {
	mac := hmac.New(sha256.New, s.secret)
	for _, part := range parts {
		_, _ = fmt.Fprintf(mac, "%d:%s", len(part), part)
	}
	return hex.EncodeToString(mac.Sum(nil)[:16])
}

type accountMapping struct {
	AccountID  string    `json:"account_id"`
	Quality    string    `json:"identity_quality"`
	ObservedAt time.Time `json:"observed_at"`
}

func (s *Store) accountID(cred credential, now time.Time) (string, string, error) {
	if cred.Identity != "" {
		return s.ID(cred.Provider, "provider", cred.Identity), "provider", nil
	}
	id, quality := s.ID(cred.Provider, "credential", cred.AuthIndex), "credential"
	err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket([]byte("accounts")).Get([]byte(cred.Provider + ":" + cred.AuthIndex))
		if raw == nil {
			return nil
		}
		var mapping accountMapping
		if json.Unmarshal(raw, &mapping) != nil {
			return errors.New("invalid stored account mapping")
		}
		if !mapping.ObservedAt.Before(now.Add(-Retention)) {
			id, quality = mapping.AccountID, mapping.Quality
		}
		return nil
	})
	return id, quality, err
}

func (s *Store) unknownAccounts(now time.Time) ([]Sample, error) {
	type known struct {
		key     string
		mapping accountMapping
	}
	accounts := make([]known, 0, 128)
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("accounts")).ForEach(func(k, v []byte) error {
			var mapping accountMapping
			if json.Unmarshal(v, &mapping) != nil {
				return errors.New("invalid stored account mapping")
			}
			if !mapping.ObservedAt.Before(now.Add(-Retention)) {
				accounts = append(accounts, known{string(k), mapping})
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(accounts, func(i, j int) bool {
		if accounts[i].mapping.ObservedAt.Equal(accounts[j].mapping.ObservedAt) {
			return accounts[i].key < accounts[j].key
		}
		return accounts[i].mapping.ObservedAt.After(accounts[j].mapping.ObservedAt)
	})
	rows := make([]Sample, 0, MaxAccounts)
	seen := make(map[string]bool)
	for _, account := range accounts {
		mapping := account.mapping
		if seen[mapping.AccountID] {
			continue
		}
		seen[mapping.AccountID] = true
		parts := strings.SplitN(account.key, ":", 2)
		if len(parts) != 2 {
			return nil, errors.New("invalid stored account mapping")
		}
		row := unknownRow(credential{Provider: parts[0], AuthIndex: parts[1]}, now, "enumeration_failed")
		row.AccountID = mapping.AccountID
		row.IdentityQuality = mapping.Quality
		row.SeriesID = s.ID("unknown", mapping.AccountID)
		rows = append(rows, row)
		if len(rows) == MaxAccounts {
			break
		}
	}
	return rows, nil
}

func (s *Store) missingWindows(rows []Sample, now time.Time) ([]Sample, error) {
	active := make(map[string]Sample)
	present := make(map[string]bool)
	accountGaps := make(map[string]bool)
	for _, row := range rows {
		active[row.AccountID] = row
		present[row.AccountID+":"+row.LimitID] = true
		if row.LimitID == "unknown" {
			accountGaps[row.AccountID] = true
		}
	}
	missing := make(map[string]Sample)
	err := s.db.View(func(tx *bolt.Tx) error {
		cur := tx.Bucket([]byte("samples")).Cursor()
		for k, v := cur.Seek(sampleKey(now.Add(-3*PollInterval), 0)); k != nil; k, v = cur.Next() {
			var prior Sample
			if json.Unmarshal(v, &prior) != nil {
				return errors.New("invalid stored observation")
			}
			latest, exists := active[prior.AccountID]
			key := prior.AccountID + ":" + prior.LimitID
			if !exists || present[key] || accountGaps[prior.AccountID] || prior.LimitID == "unknown" || prior.State != "ok" {
				continue
			}
			row := unknownRow(credential{Provider: latest.Provider, AuthIndex: latest.AuthIndex}, now, "missing_window")
			row.AccountID = latest.AccountID
			row.IdentityQuality = latest.IdentityQuality
			row.LimitID = prior.LimitID
			row.SeriesID = s.ID("missing", row.AccountID, row.LimitID)
			missing[key] = row
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	result := make([]Sample, 0, len(missing))
	for _, row := range missing {
		result = append(result, row)
	}
	return result, nil
}

func sampleKey(at time.Time, seq uint64) []byte {
	key := make([]byte, 16)
	binary.BigEndian.PutUint64(key, uint64(at.UnixNano()))
	binary.BigEndian.PutUint64(key[8:], seq)
	return key
}

// Append atomically adds observations and evicts old/excess rows.
func (s *Store) Append(samples []Sample, now time.Time) error {
	return s.appendObservations(samples, samples, now)
}

func (s *Store) appendObservations(samples, mappingSamples []Sample, now time.Time) error {
	if len(samples) > MaxAccounts*64 || len(mappingSamples) > MaxAccounts*64 {
		return errors.New("sample batch exceeds limit")
	}
	cutoff := now.Add(-Retention)
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("samples"))
		count := b.Stats().KeyN
		cur := b.Cursor()
		accounts := tx.Bucket([]byte("accounts"))
		for k, _ := cur.First(); k != nil && bytes.Compare(k, sampleKey(cutoff, 0)) < 0; k, _ = cur.Next() {
			if err := cur.Delete(); err != nil {
				return err
			}
			count--
		}
		for _, sample := range samples {
			if sample.ObservedAt.Before(cutoff) {
				continue
			}
			if sample.ObservedAt.UnixNano() <= 0 || sample.ObservedAt.After(now.Add(time.Minute)) {
				return errors.New("invalid observation timestamp")
			}
			raw, err := json.Marshal(sample)
			if err != nil {
				return err
			}
			if len(raw) > 2048 {
				return errors.New("sample exceeds size limit")
			}
			seq, errSeq := b.NextSequence()
			if errSeq != nil {
				return errSeq
			}
			if errPut := b.Put(sampleKey(sample.ObservedAt, seq), raw); errPut != nil {
				return errPut
			}
			count++
		}
		mappings := make(map[string]accountMapping, len(mappingSamples))
		for _, sample := range mappingSamples {
			if sample.ObservedAt.Before(cutoff) {
				continue
			}
			if sample.ObservedAt.UnixNano() <= 0 || sample.ObservedAt.After(now.Add(time.Minute)) {
				return errors.New("invalid mapping timestamp")
			}
			mappings[sample.Provider+":"+sample.AuthIndex] = accountMapping{sample.AccountID, sample.IdentityQuality, sample.ObservedAt}
		}
		for key, account := range mappings {
			mapping, errMapping := json.Marshal(account)
			if errMapping != nil {
				return errMapping
			}
			if errPut := accounts.Put([]byte(key), mapping); errPut != nil {
				return errPut
			}
		}
		excess := count - MaxSamples
		for k, _ := cur.First(); k != nil && excess > 0; k, _ = cur.Next() {
			if err := cur.Delete(); err != nil {
				return err
			}
			excess--
		}
		ac := accounts.Cursor()
		type retainedMapping struct {
			key        []byte
			observedAt time.Time
		}
		retained := make([]retainedMapping, 0, maxAccountMappings)
		for k, v := ac.First(); k != nil; k, v = ac.Next() {
			var mapping accountMapping
			if json.Unmarshal(v, &mapping) != nil {
				return errors.New("invalid stored account mapping")
			}
			if mapping.ObservedAt.Before(cutoff) {
				if err := ac.Delete(); err != nil {
					return err
				}
				continue
			}
			retained = append(retained, retainedMapping{bytes.Clone(k), mapping.ObservedAt})
		}
		if excess = len(retained) - maxAccountMappings; excess > 0 {
			sort.Slice(retained, func(i, j int) bool {
				if retained[i].observedAt.Equal(retained[j].observedAt) {
					return bytes.Compare(retained[i].key, retained[j].key) < 0
				}
				return retained[i].observedAt.Before(retained[j].observedAt)
			})
			for _, mapping := range retained[:excess] {
				if err := accounts.Delete(mapping.key); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (q Query) validate() error {
	if q.Provider != "" && q.Provider != "claude" && q.Provider != "codex" {
		return errors.New("invalid provider")
	}
	if q.From.IsZero() || q.To.IsZero() || q.From.UnixNano() <= 0 || q.To.Before(q.From) || q.To.Sub(q.From) > Retention || q.To.Year() > 2200 || q.From.Year() < 1971 {
		return errors.New("invalid time range")
	}
	if q.Limit < 1 || q.Limit > MaxPageSize || len(q.Cursor) > 200 {
		return errors.New("invalid pagination")
	}
	return nil
}

func (s *Store) cursorMAC(q Query, key string) string {
	return s.ID("cursor", q.From.UTC().Format(time.RFC3339Nano), q.To.UTC().Format(time.RFC3339Nano), q.Provider, strconv.Itoa(q.Limit), key)
}

// Query retrieves at most MaxPageSize rows, in ascending observed-time order.
func (s *Store) Query(q Query) (Page, error) {
	page := Page{SchemaVersion: 1, Samples: []Sample{}}
	if err := q.validate(); err != nil {
		return page, err
	}
	var after []byte
	if q.Cursor != "" {
		parts := strings.Split(q.Cursor, ".")
		if len(parts) != 2 || len(parts[0]) != 32 || !hmac.Equal([]byte(parts[1]), []byte(s.cursorMAC(q, parts[0]))) {
			return page, errors.New("invalid cursor")
		}
		var err error
		after, err = hex.DecodeString(parts[0])
		if err != nil {
			return page, errors.New("invalid cursor")
		}
	}
	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("samples")).Cursor()
		start := sampleKey(q.From, 0)
		if len(after) > 0 && bytes.Compare(after, start) > 0 {
			start = after
		}
		end := sampleKey(q.To, ^uint64(0))
		var last []byte
		for k, v := c.Seek(start); k != nil && bytes.Compare(k, end) <= 0; k, v = c.Next() {
			if len(after) > 0 && bytes.Compare(k, after) <= 0 {
				continue
			}
			var row Sample
			if err := json.Unmarshal(v, &row); err != nil {
				return errors.New("invalid stored observation")
			}
			if q.Provider != "" && row.Provider != q.Provider {
				continue
			}
			if len(page.Samples) == q.Limit {
				key := hex.EncodeToString(last)
				cursor := key + "." + s.cursorMAC(q, key)
				page.NextCursor = &cursor
				break
			}
			page.Samples = append(page.Samples, row)
			last = bytes.Clone(k)
		}
		return nil
	})
	return page, err
}
