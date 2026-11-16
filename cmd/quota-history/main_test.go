package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunRejectsUnsafeConfiguration(t *testing.T) {
	for _, args := range [][]string{{}, {"--listen", "0.0.0.0:8318"}, {"--management-origin", "http://example.com:8317"}, {"--key", "secret"}} {
		if err := run(context.Background(), args); err == nil {
			t.Fatalf("accepted %#v", args)
		}
	}
}

func TestRunAlreadyCanceledDoesNotStart(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "key")
	if err := os.WriteFile(keyPath, []byte("synthetic-admin-key"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := run(ctx, []string{"--key-file", keyPath, "--state-dir", filepath.Join(dir, "state")})
	if err != nil && !strings.Contains(err.Error(), "canceled") {
		t.Fatal(err)
	}
}

func TestRunServesAndStopsWithContext(t *testing.T) {
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"files":[]}`)) }))
	defer native.Close()
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := reservation.Addr().String()
	if err = reservation.Close(); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "key")
	state := filepath.Join(dir, "state")
	if err = os.WriteFile(keyPath, []byte("synthetic-admin-key"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"--listen", addr, "--key-file", keyPath, "--state-dir", state, "--management-origin", native.URL})
	}()
	client := &http.Client{Timeout: time.Second}
	ready := false
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		req, errReq := http.NewRequest(http.MethodGet, "http://"+addr+"/v8/management/quota-history/capabilities", nil)
		if errReq != nil {
			t.Fatal(errReq)
		}
		req.Header.Set("Authorization", "Bearer synthetic-admin-key")
		res, errDo := client.Do(req)
		if errDo == nil {
			if res.StatusCode == 200 {
				ready = true
			}
			if errClose := res.Body.Close(); errClose != nil {
				t.Error(errClose)
			}
			if ready {
				break
			}
		}
		select {
		case errRun := <-done:
			t.Fatalf("startup failed: %v", errRun)
		case <-time.After(time.Millisecond):
		}
	}
	if !ready {
		t.Fatal("listener never ready")
	}
	cancel()
	select {
	case errRun := <-done:
		if errRun != nil {
			t.Fatal(errRun)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown stuck")
	}
	info, err := os.Stat(filepath.Join(state, "history.db"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("database %v %v", info, err)
	}
}
