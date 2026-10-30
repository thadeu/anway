// Package config loads settings and holds the only reference to the product name.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"
)

const (
	ProductName = "jobserver"
	Version     = "0.1.0"
)

type Config struct {
	BindAddr        string
	DataDir         string
	ConfDir         string
	Password        string
	AllowFlush      bool
	TLSCertFile     string
	TLSKeyFile      string
	BackupDir       string
	ShutdownTimeout time.Duration
	LogLevel        slog.Level
}

func Load() (Config, error) {
	cfg := Config{
		BindAddr:        env("BIND_ADDR", "127.0.0.1:7419"),
		DataDir:         env("DATA_DIR", "./data"),
		ConfDir:         env("CONF_DIR", "./conf.d"),
		Password:        os.Getenv("SERVER_PASSWORD"),
		AllowFlush:      os.Getenv("ALLOW_FLUSH") == "true",
		TLSCertFile:     os.Getenv("TLS_CERT_FILE"),
		TLSKeyFile:      os.Getenv("TLS_KEY_FILE"),
		ShutdownTimeout: 30 * time.Second,
	}

	if raw := os.Getenv("SHUTDOWN_TIMEOUT"); raw != "" {
		timeout, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("invalid SHUTDOWN_TIMEOUT: %w", err)
		}

		cfg.ShutdownTimeout = timeout
	}

	if err := cfg.LogLevel.UnmarshalText([]byte(env("LOG_LEVEL", "info"))); err != nil {
		return Config{}, fmt.Errorf("invalid LOG_LEVEL: %w", err)
	}

	cfg.BackupDir = env("BACKUP_DIR", filepath.Join(cfg.DataDir, "backups"))

	return cfg, cfg.validate()
}

func (c Config) validate() error {
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return errors.New("TLS_CERT_FILE and TLS_KEY_FILE must be set together")
	}

	if c.Password == "" && !isLoopback(c.BindAddr) {
		return fmt.Errorf("SERVER_PASSWORD is required when BIND_ADDR is %s", c.BindAddr)
	}

	return nil
}

func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}

	if host == "localhost" {
		return true
	}

	ip := net.ParseIP(host)

	return ip != nil && ip.IsLoopback()
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}
