// quota-history is an optional loopback-only management companion.
package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/quotahistory"
)

func run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("quota-history", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	listen := flags.String("listen", "127.0.0.1:8318", "literal loopback bind address")
	origin := flags.String("management-origin", "http://127.0.0.1:8317", "native loopback management origin")
	keyFile := flags.String("key-file", "", "absolute path to private existing administrator key file")
	stateDir := flags.String("state-dir", "", "absolute path to private history state directory")
	if err := flags.Parse(args); err != nil {
		return errors.New("invalid flags; use --listen, --management-origin, --key-file and --state-dir")
	}
	if flags.NArg() != 0 || *keyFile == "" || *stateDir == "" {
		return errors.New("absolute --key-file and --state-dir are required; raw keys are not accepted")
	}
	if err := quotahistory.ValidateListen(*listen); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	key, err := quotahistory.ReadKey(*keyFile)
	if err != nil {
		return err
	}
	store, err := quotahistory.Open(*stateDir)
	if err != nil {
		return errors.New("history storage initialization failed")
	}
	defer func() {
		if errClose := store.Close(); errClose != nil {
			log.WithField("event", "storage_close_failed").Error("quota history")
		}
	}()
	collector, err := quotahistory.NewCollector(store, *origin, key, quotahistory.Options{})
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return errors.New("history loopback listener unavailable")
	}
	server := &http.Server{Handler: quotahistory.NewHandler(store, key, collector.Status), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var workers sync.WaitGroup
	workers.Go(func() {
		collector.Run(runCtx, func(status quotahistory.Status) {
			entry := log.WithFields(log.Fields{"event": "quota_history_poll", "consecutive_failures": status.ConsecutiveFailures, "stale": status.Stale})
			if status.LastError != nil {
				entry.WithField("code", *status.LastError).Warn("quota history")
			} else {
				entry.Info("quota history")
			}
		})
	})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	log.WithFields(log.Fields{"event": "quota_history_ready", "listen": listener.Addr().String()}).Info("quota history")
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-done:
	}
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	errShutdown := server.Shutdown(shutdownCtx)
	if errShutdown != nil {
		if errClose := server.Close(); errClose != nil {
			log.WithField("event", "http_close_failed").Warn("quota history")
		}
	}
	workers.Wait()
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return errors.New("history HTTP server failed")
	}
	if errShutdown != nil {
		return errors.New("history HTTP shutdown failed")
	}
	return nil
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		log.WithField("event", "quota_history_start_failed").Error(err.Error())
		os.Exit(1)
	}
}
