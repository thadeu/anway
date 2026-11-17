package cron

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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

	built, err := settings.Entries[0].Job.build("cron-test")
	if err != nil {
		t.Fatal(err)
	}

	if string(built.Args) != `[1,"two"]` || string(built.Custom) != `{"expires_in":60}` || *built.Retry != 3 {
		t.Fatalf("unexpected job: %+v", built)
	}
}

func newScheduler(t *testing.T, store *storage.Bolt) (*Scheduler, *manager.Manager) {
	t.Helper()

	jobs := manager.New(store)

	return New(jobs, slog.New(slog.NewTextHandler(io.Discard, nil))), jobs
}

func openStore(t *testing.T, dir string) *storage.Bolt {
	t.Helper()

	store, err := storage.OpenBolt(dir)
	if err != nil {
		t.Fatal(err)
	}

	return store
}

func entry(spec, jobtype string) Entry {
	return Entry{Schedule: spec, Job: JobTemplate{Type: jobtype, Queue: "ops"}}
}

func sources(upcoming []Upcoming) []string {
	var list []string

	for _, current := range upcoming {
		list = append(list, current.Source+"/"+current.Tag+"/"+current.JobType)
	}

	return list
}

func TestScheduleReplacesTheEntriesOfATag(t *testing.T) {
	store := openStore(t, t.TempDir())
	defer store.Close()

	scheduler, _ := newScheduler(t, store)

	if err := scheduler.Schedule(ScheduleRequest{Tag: "shop", Entries: []Entry{entry("0 0 * * *", "a"), entry("*/5 * * * *", "b")}}); err != nil {
		t.Fatal(err)
	}

	if err := scheduler.Schedule(ScheduleRequest{Tag: "shop", Entries: []Entry{entry("0 0 * * *", "c")}}); err != nil {
		t.Fatal(err)
	}

	if got := sources(scheduler.Upcoming()); len(got) != 1 || got[0] != "client/shop/c" {
		t.Fatalf("a second SCHEDULE must replace the entries: %v", got)
	}

	if err := scheduler.Schedule(ScheduleRequest{Tag: "shop", Entries: []Entry{}}); err != nil {
		t.Fatal(err)
	}

	if got := scheduler.Upcoming(); len(got) != 0 {
		t.Fatalf("no entries must remove the tag: %v", got)
	}

	saved, err := store.Schedules()
	if err != nil {
		t.Fatal(err)
	}

	if len(saved) != 0 {
		t.Fatalf("the removed tag must not stay on disk: %+v", saved)
	}
}

func TestInvalidScheduleChangesNothing(t *testing.T) {
	store := openStore(t, t.TempDir())
	defer store.Close()

	scheduler, _ := newScheduler(t, store)

	if err := scheduler.Schedule(ScheduleRequest{Tag: "shop", Entries: []Entry{entry("0 0 * * *", "a")}}); err != nil {
		t.Fatal(err)
	}

	tooMany := make([]Entry, MaxClientEntries+1)

	for i := range tooMany {
		tooMany[i] = entry("0 0 * * *", "a")
	}

	cases := []ScheduleRequest{
		{Tag: "shop", Entries: []Entry{entry("0 0 * * *", "b"), entry("bad", "c")}},
		{Tag: "shop", Entries: []Entry{entry("0 0 * * *", "")}},
		{Tag: "shop", Entries: []Entry{{Schedule: "0 0 * * *", Job: JobTemplate{Type: "a", Queue: "bad queue"}}}},
		{Tag: "shop", Entries: []Entry{{Schedule: "0 0 * * *", Job: JobTemplate{Type: "a", ReserveFor: 5}}}},
		{Tag: "shop", Entries: tooMany},
		{Tag: "", Entries: []Entry{entry("0 0 * * *", "a")}},
		{Tag: "bad tag", Entries: []Entry{entry("0 0 * * *", "a")}},
	}

	for _, request := range cases {
		if err := scheduler.Schedule(request); err == nil {
			t.Fatalf("SCHEDULE must fail: %+v", request)
		}
	}

	if got := sources(scheduler.Upcoming()); len(got) != 1 || got[0] != "client/shop/a" {
		t.Fatalf("a refused SCHEDULE must change nothing: %v", got)
	}
}

func TestClientEntriesSurviveARestartAndAReload(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	scheduler, _ := newScheduler(t, store)

	if err := scheduler.Schedule(ScheduleRequest{Tag: "shop", Entries: []Entry{entry("0 0 * * *", "a")}}); err != nil {
		t.Fatal(err)
	}

	store.Close()

	store = openStore(t, dir)
	defer store.Close()

	scheduler, _ = newScheduler(t, store)

	if err := scheduler.LoadClient(); err != nil {
		t.Fatal(err)
	}

	scheduler.SetEntries([]Entry{prepared(t, "*/5 * * * *", "conf")})

	got := sources(scheduler.Upcoming())

	if len(got) != 2 || got[0] != "conf.d//conf" || got[1] != "client/shop/a" {
		t.Fatalf("conf.d and client entries must run together: %v", got)
	}

	scheduler.SetEntries(nil)

	if got := sources(scheduler.Upcoming()); len(got) != 1 || got[0] != "client/shop/a" {
		t.Fatalf("a conf.d reload must keep the client entries: %v", got)
	}

	if err := scheduler.Unschedule("shop"); err != nil {
		t.Fatal(err)
	}

	if err := scheduler.Unschedule("shop"); err != nil {
		t.Fatalf("a missing tag is not an error: %v", err)
	}

	if got := scheduler.Upcoming(); len(got) != 0 {
		t.Fatalf("UNSCHEDULE must remove the tag: %v", got)
	}
}

func TestSchedulerPushesClientEntries(t *testing.T) {
	store := openStore(t, t.TempDir())
	defer store.Close()

	scheduler, jobs := newScheduler(t, store)
	retry := 3

	request := ScheduleRequest{Tag: "shop", Entries: []Entry{{
		Schedule: "*/5 * * * *",
		Job:      JobTemplate{Type: "refill", Queue: "billing", Retry: &retry, Args: []any{1}},
	}}}

	if err := scheduler.Schedule(request); err != nil {
		t.Fatal(err)
	}

	scheduler.Tick(time.Now().UTC().Add(6 * time.Minute))

	stats, err := jobs.Stats()
	if err != nil {
		t.Fatal(err)
	}

	if stats.Queues["billing"] != 1 {
		t.Fatalf("the client entry must push its job: %+v", stats)
	}
}

func prepared(t *testing.T, spec, jobtype string) Entry {
	t.Helper()

	current := entry(spec, jobtype)

	if err := current.prepare(); err != nil {
		t.Fatal(err)
	}

	return current
}

func TestEntryIDsAreStableAndDistinct(t *testing.T) {
	store := openStore(t, t.TempDir())
	defer store.Close()

	scheduler, _ := newScheduler(t, store)
	entries := []Entry{entry("0 0 * * *", "a"), entry("0 0 * * *", "a"), entry("30 0 * * *", "a")}

	if err := scheduler.Schedule(ScheduleRequest{Tag: "shop", Entries: entries}); err != nil {
		t.Fatal(err)
	}

	scheduler.SetEntries([]Entry{prepared(t, "0 0 * * *", "a")})

	first := scheduler.Upcoming()
	ids := map[string]bool{}

	for _, current := range first {
		ids[current.ID] = true

		if len(current.ID) != 12 || len(current.NextRuns) != NextRuns || !current.NextRuns[0].Equal(current.Next) {
			t.Fatalf("unexpected entry: %+v", current)
		}
	}

	if len(ids) != 4 {
		t.Fatalf("each entry needs its own id: %v", ids)
	}

	if err := scheduler.Schedule(ScheduleRequest{Tag: "shop", Entries: entries}); err != nil {
		t.Fatal(err)
	}

	for i, current := range scheduler.Upcoming() {
		if current.ID != first[i].ID {
			t.Fatalf("a new SCHEDULE with the same entries must keep the ids: %s, %s", current.ID, first[i].ID)
		}
	}
}

// clientEntry schedules one entry for the tag shop and returns its id.
func clientEntry(t *testing.T, scheduler *Scheduler, spec string) string {
	t.Helper()

	if err := scheduler.Schedule(ScheduleRequest{Tag: "shop", Entries: []Entry{entry(spec, "refill")}}); err != nil {
		t.Fatal(err)
	}

	return scheduler.Upcoming()[0].ID
}

func TestPauseSkipsRunsAndSurvivesADeployAndARestart(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	scheduler, jobs := newScheduler(t, store)
	id := clientEntry(t, scheduler, "*/5 * * * *")

	if _, err := scheduler.Pause("ffffffffffff", "Ana"); !errors.Is(err, ErrUnknownEntry) {
		t.Fatalf("an unknown id must fail: %v", err)
	}

	pause, err := scheduler.Pause(id, "Ana")
	if err != nil {
		t.Fatal(err)
	}

	if again, _ := scheduler.Pause(id, "Bob"); again.PausedBy != "Ana" || again.PausedAt != pause.PausedAt {
		t.Fatalf("a second pause must keep the first: %+v", again)
	}

	scheduler.Tick(time.Now().UTC().Add(6 * time.Minute))

	if stats, _ := jobs.Stats(); stats.Queues["ops"] != 0 {
		t.Fatalf("a paused entry must not run: %+v", stats)
	}

	clientEntry(t, scheduler, "*/5 * * * *")

	store.Close()

	store = openStore(t, dir)
	defer store.Close()

	scheduler, jobs = newScheduler(t, store)

	if err := scheduler.LoadClient(); err != nil {
		t.Fatal(err)
	}

	upcoming := scheduler.Upcoming()

	if upcoming[0].Paused == nil || upcoming[0].Paused.PausedBy != "Ana" {
		t.Fatalf("a deploy and a restart must keep the pause: %+v", upcoming[0])
	}

	if err := scheduler.Resume(id); err != nil {
		t.Fatal(err)
	}

	scheduler.Tick(time.Now().UTC().Add(6 * time.Minute))

	if stats, _ := jobs.Stats(); stats.Queues["ops"] != 1 {
		t.Fatalf("a resumed entry must run again: %+v", stats)
	}
}

func TestAPauseOfAChangedEntryIsStale(t *testing.T) {
	store := openStore(t, t.TempDir())
	defer store.Close()

	scheduler, _ := newScheduler(t, store)
	id := clientEntry(t, scheduler, "*/5 * * * *")

	if _, err := scheduler.Pause(id, ""); err != nil {
		t.Fatal(err)
	}

	if stale := scheduler.StalePauses(); len(stale) != 0 {
		t.Fatalf("the pause of a current entry is not stale: %+v", stale)
	}

	changed := clientEntry(t, scheduler, "*/10 * * * *")

	stale := scheduler.StalePauses()

	if changed == id || len(stale) != 1 || stale[0].ID != id || stale[0].Schedule != "*/5 * * * *" {
		t.Fatalf("a pause whose entry changed must be stale: %+v", stale)
	}

	if scheduler.Upcoming()[0].Paused != nil {
		t.Fatal("the changed entry runs on its schedule")
	}

	if err := scheduler.Resume(id); err != nil {
		t.Fatal(err)
	}

	if stale := scheduler.StalePauses(); len(stale) != 0 {
		t.Fatalf("a resume must clear a stale pause: %+v", stale)
	}
}

func TestRunNowPushesAManualRun(t *testing.T) {
	store := openStore(t, t.TempDir())
	defer store.Close()

	scheduler, jobs := newScheduler(t, store)
	id := clientEntry(t, scheduler, "0 0 * * *")
	next := scheduler.Upcoming()[0].Next

	if _, err := scheduler.Pause(id, ""); err != nil {
		t.Fatal(err)
	}

	jid, err := scheduler.RunNow(id)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.HasPrefix(jid, "cron-manual-"+id+"-") {
		t.Fatalf("unexpected jid %q", jid)
	}

	if _, err := scheduler.RunNow("ffffffffffff"); !errors.Is(err, ErrUnknownEntry) {
		t.Fatalf("an unknown id must fail: %v", err)
	}

	runs, err := jobs.CronRuns()
	if err != nil {
		t.Fatal(err)
	}

	if len(runs[id]) != 1 || runs[id][0].JID != jid || !runs[id][0].Manual {
		t.Fatalf("the manual run must be in the history: %+v", runs)
	}

	if !scheduler.Upcoming()[0].Next.Equal(next) {
		t.Fatal("run now must not change the schedule")
	}

	clientEntry(t, scheduler, "0 1 * * *")

	if runs, _ := jobs.CronRuns(); len(runs) != 0 {
		t.Fatalf("the runs of a removed entry must go: %+v", runs)
	}
}
