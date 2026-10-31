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
throttles:
  api: { concurrency: 2 }

cron:
  - schedule: "*/5 * * * *"
    job:
      type: Cleanup
      queue: ops
      retry: 3
      args: [1, "two"]
      custom:
        expires_in: 60
`

func TestSchedulerPushesDueEntry(t *testing.T) {
	dir := t.TempDir()

	if err := os.WriteFile(filepath.Join(dir, "a.yml"), []byte(settingsFile), 0o600); err != nil {
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
		"cron:\n  - schedule: bad\n    job:\n      type: A\n",
		"cron:\n  - schedule: \"* * * * *\"\n",
		"throttles:\n  api: { timeout: 5 }\n",
		"cron:\n  - shedule: \"* * * * *\"\n",
		"unknown: 1\n",
		"throttles: [",
	}

	for _, content := range cases {
		dir := t.TempDir()

		if err := os.WriteFile(filepath.Join(dir, "a.yml"), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}

		if _, err := LoadSettings(dir); err == nil {
			t.Fatalf("settings must fail: %s", content)
		}
	}
}

func TestLoadsBothExtensionsAndMerges(t *testing.T) {
	dir := t.TempDir()

	files := map[string]string{
		"a.yml":     "throttles:\n  api: { concurrency: 1 }\n",
		"b.yaml":    "throttles:\n  bulk: { worker: 2 }\n",
		"c.toml":    "not read = [",
		"empty.yml": "",
	}

	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	settings, err := LoadSettings(dir)
	if err != nil {
		t.Fatal(err)
	}

	if len(settings.Throttles()) != 2 {
		t.Fatalf("unexpected throttles: %+v", settings.Throttles())
	}
}

func TestCustomFieldsAndArgsReachTheJob(t *testing.T) {
	dir := t.TempDir()

	if err := os.WriteFile(filepath.Join(dir, "a.yml"), []byte(settingsFile), 0o600); err != nil {
		t.Fatal(err)
	}

	settings, err := LoadSettings(dir)
	if err != nil {
		t.Fatal(err)
	}

	built, err := settings.Entries[0].Job.build()
	if err != nil {
		t.Fatal(err)
	}

	if string(built.Args) != `[1,"two"]` || string(built.Custom) != `{"expires_in":60}` || *built.Retry != 3 {
		t.Fatalf("unexpected job: %+v", built)
	}
}
