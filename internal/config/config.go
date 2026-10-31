// Package config loads settings and holds the only reference to the product name.
package config

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"
)

const ProductName = "anway"

// Version is replaced at release build time.
var Version = "dev"

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
	ShowVersion     bool
}

// Load reads env vars, then applies flags, which have priority.
func Load(args []string) (Config, error) {
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

	flags := flag.NewFlagSet(ProductName, flag.ContinueOnError)

	flags.StringVar(&cfg.BindAddr, "bind", cfg.BindAddr, "work protocol address")
	flags.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "storage directory")
	flags.StringVar(&cfg.ConfDir, "conf-dir", cfg.ConfDir, "YAML settings directory")
	flags.StringVar(&cfg.TLSCertFile, "tls-cert", cfg.TLSCertFile, "TLS certificate file")
	flags.StringVar(&cfg.TLSKeyFile, "tls-key", cfg.TLSKeyFile, "TLS private key file")
	flags.BoolVar(&cfg.ShowVersion, "version", false, "print the version")

	if err := flags.Parse(args); err != nil {
		return Config{}, err
	}

	if flags.NArg() > 0 {
		return Config{}, fmt.Errorf("unexpected argument: %s", flags.Arg(0))
	}

	if cfg.ShowVersion {
		return cfg, nil
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
