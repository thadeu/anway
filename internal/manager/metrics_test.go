package manager

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/thadeu/anway/internal/job"
	"github.com/thadeu/anway/internal/storage"
)

func openStore(t *testing.T, dir string) *storage.Bolt {
	t.Helper()

	store, err := storage.OpenBolt(dir)
	if err != nil {
		t.Fatal(err)
	}

	return store
}

func processOne(t *testing.T, m *Manager, jid string) {
	t.Helper()

	if err := m.Push(&job.Job{JID: jid, Type: "x", Args: json.RawMessage(`[]`), Queue: "mail"}); err != nil {
		t.Fatal(err)
	}

	if _, err := m.Fetch(t.Context(), []string{"mail"}, "w"); err != nil {
		t.Fatal(err)
	}

	if err := m.Ack(jid); err != nil {
		t.Fatal(err)
	}
}

func lastDone(t *testing.T, m *Manager, rangeName string) uint64 {
	t.Helper()

	points, err := m.Series(rangeName)
	if err != nil {
		t.Fatal(err)
	}

	var total uint64

	for _, point := range points {
		total += point.Done
	}

	return total
}

func TestMetricsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	first := New(store)

	if err := first.RestoreMetrics(24 * time.Hour); err != nil {
		t.Fatal(err)
	}

	processOne(t, first, "a")
	processOne(t, first, "b")

	if err := first.SaveMetrics(); err != nil {
		t.Fatal(err)
	}

	store.Close()

	store = openStore(t, dir)
	defer store.Close()

	second := New(store)

	if err := second.RestoreMetrics(24 * time.Hour); err != nil {
		t.Fatal(err)
	}

	if done := lastDone(t, second, "15m"); done != 2 {
		t.Fatalf("restored done: %d", done)
	}

	processOne(t, second, "c")

	if err := second.SaveMetrics(); err != nil {
		t.Fatal(err)
	}

	saved, err := store.LoadMetrics(time.Now().Unix()/60-1, time.Now().Unix()/60)
	if err != nil {
		t.Fatal(err)
	}

	var total, mail uint64

	for _, minute := range saved {
		total += minute.Done
		mail += minute.Queues["mail"]
	}

	if total != 3 || mail != 3 {
		t.Fatalf("a save after a restore must keep old counts: %+v", saved)
	}
}

func TestLongRangesReadDiskAndRetentionPrunes(t *testing.T) {
	store := openStore(t, t.TempDir())
	defer store.Close()

	m := New(store)

	if err := m.RestoreMetrics(48 * time.Hour); err != nil {
		t.Fatal(err)
	}

	now := time.Now().Unix() / 60
	old := []storage.MetricMinute{{Minute: now - 3*1440, Done: 7}, {Minute: now - 1440 - 5, Done: 5}}

	if err := store.SaveMetrics(old); err != nil {
		t.Fatal(err)
	}

	if done := lastDone(t, m, "7d"); done != 12 {
		t.Fatalf("7d must read saved minutes: %d", done)
	}

	m.flushMu.Lock()
	m.lastFlush = 0
	m.flushMu.Unlock()

	if err := m.tickMetrics(time.Now()); err != nil {
		t.Fatal(err)
	}

	if done := lastDone(t, m, "7d"); done != 5 {
		t.Fatalf("retention must prune minutes older than 48h: %d", done)
	}
}

func TestSecondSeriesAndLiveRate(t *testing.T) {
	store := openStore(t, t.TempDir())
	defer store.Close()

	m := New(store)
	now := time.Now()

	m.metrics.add("mail", 10, 0, now.Add(-2*time.Second))
	m.metrics.add("mail", 5, 1, now)

	points, err := m.Series("60s")
	if err != nil {
		t.Fatal(err)
	}

	if len(points) != 60 || points[59].Done != 5 || points[57].Done != 10 {
		t.Fatalf("unexpected seconds: %+v", points[55:])
	}

	done, _ := m.metrics.liveRate(now)

	if done != 2 {
		t.Fatalf("live rate must average the last complete seconds: %v", done)
	}
}

func TestWindowReadsMemoryAndDiskWithAStep(t *testing.T) {
	store := openStore(t, t.TempDir())
	defer store.Close()

	m := New(store)
	now := time.Now()
	current := now.Unix() / 60

	if err := store.SaveMetrics([]storage.MetricMinute{{Minute: current - 2*1440, Done: 4}}); err != nil {
		t.Fatal(err)
	}

	processOne(t, m, "a")

	old, err := m.Window(time.Unix((current-2*1440-3)*60, 0), time.Unix((current-2*1440+3)*60, 0))
	if err != nil {
		t.Fatal(err)
	}

	total := uint64(0)

	for _, point := range old {
		total += point.Done
	}

	if len(old) != 7 || total != 4 {
		t.Fatalf("an old window must read saved minutes by minute: %d points, %d done", len(old), total)
	}

	day, err := m.Window(now.Add(-6*time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}

	if len(day) > windowPoints || day[1].Time.Sub(day[0].Time) != 5*time.Minute || day[len(day)-1].Done != 1 {
		t.Fatalf("a 6 hour window must use 5 minute steps and include the last minute: %d points", len(day))
	}
}

func TestThreadsKeepThePeakOfTheMinuteAndSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	first := New(store)

	if err := first.RestoreMetrics(24 * time.Hour); err != nil {
		t.Fatal(err)
	}

	first.Register(WorkerInfo{WID: "a", Concurrency: 5})
	first.Register(WorkerInfo{WID: "b", Concurrency: 10})

	now := time.Now()

	if err := first.Tick(now); err != nil {
		t.Fatal(err)
	}

	first.Beat("b", "", 20)

	if err := first.Tick(now); err != nil {
		t.Fatal(err)
	}

	first.Beat("b", "", 4)
	first.Disconnect("a")

	if err := first.Tick(now); err != nil {
		t.Fatal(err)
	}

	points, err := first.Series("15m")
	if err != nil {
		t.Fatal(err)
	}

	if threads := points[len(points)-1].Threads; threads != 25 {
		t.Fatalf("the minute must keep the peak of 5 + 20 threads: %d", threads)
	}

	if err := first.SaveMetrics(); err != nil {
		t.Fatal(err)
	}

	store.Close()

	store = openStore(t, dir)
	defer store.Close()

	second := New(store)

	if err := second.RestoreMetrics(24 * time.Hour); err != nil {
		t.Fatal(err)
	}

	points, err = second.Series("15m")
	if err != nil {
		t.Fatal(err)
	}

	if threads := points[len(points)-1].Threads; threads != 25 {
		t.Fatalf("the peak must survive a restart: %d", threads)
	}
}

func TestAStepShowsThePeakThreadsNotTheSum(t *testing.T) {
	m := newMetrics()
	now := time.Unix(1_800_000_000, 0)

	m.sampleThreads(7, now.Add(-time.Minute))
	m.sampleThreads(9, now)
	m.add("", 3, 0, now.Add(-time.Minute))
	m.add("", 4, 0, now)

	points := m.series(2, 2, now)

	if len(points) != 1 || points[0].Threads != 9 || points[0].Done != 7 {
		t.Fatalf("a step must add done jobs and keep the thread peak: %+v", points)
	}
}
