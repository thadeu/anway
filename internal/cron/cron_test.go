package cron

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thadeu/anway/internal/manager"
	"github.com/thadeu/anway/internal/storage"
)

const settingsFile = `
[throttles]
api = { concurrency = 2 }

[[cron]]
  schedule = "*/5 * * * *"
  [cron.job]
    type = "Cleanup"
    queue = "ops"
    retry = 3
    [cron.job.custom]
      expires_in = 60
`

func TestSchedulerPushesDueEntry(t *testing.T) {
	dir := t.TempDir()

	if err := os.WriteFile(filepath.Join(dir, "a.toml"), []byte(settingsFile), 0o600); err != nil {
		t.Fatal(err)
	}

	settings, err := LoadSettings(dir)
	if err != nil {
		t.Fatal(err)
	}

	if throttle := settings.Throttles()["api"]; throttle.Concurrency != 2 || throttle.Timeout != time.Minute {
		t.Fatalf("unexpected throttle: %+v", throttle)
	}

	store, err := storage.OpenBolt(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	jobs := manager.New(store)
	scheduler := New(jobs, slog.New(slog.NewTextHandler(io.Discard, nil)))

	scheduler.SetEntries(settings.Entries)
	scheduler.Tick(time.Now().UTC())
	scheduler.Tick(time.Now().UTC().Add(6 * time.Minute))
	scheduler.Tick(time.Now().UTC().Add(6 * time.Minute))

	stats, err := jobs.Stats()
	if err != nil {
		t.Fatal(err)
	}

	if stats.Queues["ops"] != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestInvalidSettingsFail(t *testing.T) {
	cases := []string{
		"[[cron]]\nschedule = \"bad\"\n[cron.job]\ntype = \"A\"\n",
		"[[cron]]\nschedule = \"* * * * *\"\n",
		"[throttles]\napi = { timeout = 5 }\n",
		"not toml =",
	}

	for _, content := range cases {
		dir := t.TempDir()

		if err := os.WriteFile(filepath.Join(dir, "a.toml"), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}

		if _, err := LoadSettings(dir); err == nil {
			t.Fatalf("settings must fail: %s", content)
		}
	}
}
