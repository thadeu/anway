package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/thadeu/anway/internal/config"
	"github.com/thadeu/anway/internal/cron"
	"github.com/thadeu/anway/internal/manager"
	"github.com/thadeu/anway/internal/server"
	"github.com/thadeu/anway/internal/storage"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

// applySettings loads all TOML files. It changes nothing when a file is invalid.
func applySettings(dir string, jobs *manager.Manager, scheduler *cron.Scheduler) error {
	settings, err := cron.LoadSettings(dir)
	if err != nil {
		return err
	}

	jobs.SetThrottles(settings.Throttles())
	scheduler.SetEntries(settings.Entries)

	return nil
}

func reloadOnHangup(ctx context.Context, dir string, jobs *manager.Manager, scheduler *cron.Scheduler, log *slog.Logger) {
	hangup := make(chan os.Signal, 1)
	signal.Notify(hangup, syscall.SIGHUP)

	for {
		select {
		case <-ctx.Done():
			return
		case <-hangup:
			if err := applySettings(dir, jobs, scheduler); err != nil {
				log.Error("reload failed, the old settings stay active", "error", err)

				continue
			}

			log.Info("settings reloaded")
		}
	}
}

func listen(cfg config.Config) (net.Listener, error) {
	listener, err := net.Listen("tcp", cfg.BindAddr)
	if err != nil || cfg.TLSCertFile == "" {
		return listener, err
	}

	certificate, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		listener.Close()

		return nil, err
	}

	return tls.NewListener(listener, &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
	}), nil
}

// backupOnSignal writes a snapshot at each SIGUSR1.
func backupOnSignal(ctx context.Context, dir string, jobs *manager.Manager, log *slog.Logger) {
	request := make(chan os.Signal, 1)
	signal.Notify(request, syscall.SIGUSR1)

	for {
		select {
		case <-ctx.Done():
			return
		case <-request:
			path, err := writeBackup(dir, jobs)
			if err != nil {
				log.Error("backup failed", "error", err)

				continue
			}

			log.Info("backup finished", "path", path)
		}
	}
}

func writeBackup(dir string, jobs *manager.Manager) (string, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}

	name := "jobs-" + time.Now().UTC().Format("20060102T150405.000000000") + ".db"
	path := filepath.Join(dir, name)

	return path, jobs.Snapshot(path)
}

func run() error {
	cfg, err := config.Load(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}

	if err != nil {
		return err
	}

	if cfg.ShowVersion {
		fmt.Println(config.ProductName, config.Version)

		return nil
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))

	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return err
	}

	store, err := storage.OpenBolt(cfg.DataDir)
	if err != nil {
		return err
	}
	defer store.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	listener, err := listen(cfg)
	if err != nil {
		return err
	}

	jobs := manager.New(store)
	srv := server.New(jobs, server.Options{Password: cfg.Password, AllowFlush: cfg.AllowFlush}, log)
	loopCtx, stopLoop := context.WithCancel(context.Background())

	var loops sync.WaitGroup

	defer func() {
		stopLoop()
		loops.Wait()
	}()

	scheduler := cron.New(jobs, log)

	if err := applySettings(cfg.ConfDir, jobs, scheduler); err != nil {
		return err
	}

	loops.Go(func() { scheduler.Run(loopCtx) })
	loops.Go(func() { reloadOnHangup(loopCtx, cfg.ConfDir, jobs, scheduler, log) })
	loops.Go(func() { backupOnSignal(loopCtx, cfg.BackupDir, jobs, log) })

	loops.Go(func() {
		jobs.Run(loopCtx, func(err error) {
			log.Error("background loop failed", "error", err)
		})
	})

	serveErr := make(chan error, 1)

	go func() {
		serveErr <- srv.Serve(loopCtx, listener)
	}()

	log.Info("server started", "addr", listener.Addr().String(), "data_dir", cfg.DataDir)

	select {
	case <-ctx.Done():
	case err := <-serveErr:
		return err
	}

	log.Info("shutdown started")

	listener.Close()
	srv.Shutdown(cfg.ShutdownTimeout)

	return nil
}
