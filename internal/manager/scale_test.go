package manager

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/thadeu/anway/internal/job"
	"github.com/thadeu/anway/internal/storage"
)

func TestAutoscaleAddsThreadsWhenJobsWaitAndAllThreadsAreBusy(t *testing.T) {
	now := time.Now()
	worker := &WorkerInfo{Concurrency: 8, Autoscale: &Autoscale{Min: 4, Max: 10}}

	autoscale(worker, workerLoad{backlog: 5, busy: 8}, now)

	if worker.Wanted != 10 {
		t.Fatalf("8 busy threads with waiting jobs must grow by a quarter, up to max 10: got %d", worker.Wanted)
	}

	worker = &WorkerInfo{Concurrency: 8, Autoscale: &Autoscale{Min: 4, Max: 20}}

	autoscale(worker, workerLoad{backlog: 5, busy: 7}, now)

	if worker.Wanted != 0 {
		t.Fatalf("a free thread takes the waiting jobs, so the count must not change: got %d", worker.Wanted)
	}
}

func TestAutoscaleRemovesThreadsOnlyAfterAMinuteAtLowLoad(t *testing.T) {
	now := time.Now()
	worker := &WorkerInfo{Concurrency: 8, Autoscale: &Autoscale{Min: 4, Max: 20}}

	autoscale(worker, workerLoad{busy: 1}, now)
	autoscale(worker, workerLoad{busy: 1}, now.Add(30*time.Second))

	if worker.Wanted != 0 {
		t.Fatalf("low load for 30 seconds must not remove threads: got %d", worker.Wanted)
	}

	autoscale(worker, workerLoad{busy: 1}, now.Add(scaleDownAfter))

	if worker.Wanted != 6 {
		t.Fatalf("low load for a minute must remove a quarter: got %d", worker.Wanted)
	}

	worker = &WorkerInfo{Concurrency: 5, Autoscale: &Autoscale{Min: 4, Max: 20}}

	autoscale(worker, workerLoad{}, now)
	autoscale(worker, workerLoad{}, now.Add(scaleDownAfter))

	if worker.Wanted != 4 {
		t.Fatalf("threads must not go under min: got %d", worker.Wanted)
	}
}

func TestAutoscaleBringsThreadsIntoTheRange(t *testing.T) {
	worker := &WorkerInfo{Concurrency: 30, Autoscale: &Autoscale{Min: 4, Max: 20}}

	autoscale(worker, workerLoad{busy: 30}, time.Now())

	if worker.Wanted != 20 {
		t.Fatalf("a count above max must go to max: got %d", worker.Wanted)
	}
}

func TestBeatSendsTheWantedThreadsOnlyToAScalableWorker(t *testing.T) {
	m := New(openStore(t, t.TempDir()))

	m.Register(WorkerInfo{WID: "w", Concurrency: 5})

	if err := m.SetConcurrency("w", 8); !errors.Is(err, ErrNotScalable) {
		t.Fatalf("a worker that does not report its threads cannot scale: %v", err)
	}

	if _, wanted := m.Beat("w", "", 0); wanted != 0 {
		t.Fatalf("a Faktory worker must not get a thread count: %d", wanted)
	}

	m.Beat("w", "", 5)

	if err := m.SetConcurrency("w", 8); err != nil {
		t.Fatal(err)
	}

	if _, wanted := m.Beat("w", "", 5); wanted != 8 {
		t.Fatalf("the beat must carry the wanted count: %d", wanted)
	}

	if _, wanted := m.Beat("w", "", 8); wanted != 0 {
		t.Fatalf("after the worker runs 8 threads, the beat must not ask again: %d", wanted)
	}

	if err := m.SetConcurrency("w", 0); !errors.Is(err, ErrBadConcurrency) {
		t.Fatalf("0 threads must fail: %v", err)
	}

	if err := m.SetConcurrency("nobody", 3); !errors.Is(err, ErrUnknownWorker) {
		t.Fatalf("an unknown worker must fail: %v", err)
	}
}

func TestBeatAutoscalesFromTheQueuesOfTheWorker(t *testing.T) {
	m := New(openStore(t, t.TempDir()))

	m.Register(WorkerInfo{WID: "w", Concurrency: 1, Queues: []string{"mail"}})
	m.Beat("w", "", 1)

	if err := m.SetAutoscale("w", &Autoscale{Max: 3}); err != nil {
		t.Fatal(err)
	}

	for _, jid := range []string{"a", "b"} {
		if err := m.Push(&job.Job{JID: jid, Type: "x", Args: json.RawMessage(`[]`), Queue: "mail"}); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := m.Fetch(t.Context(), []string{"mail"}, "w"); err != nil {
		t.Fatal(err)
	}

	if _, wanted := m.Beat("w", "", 1); wanted != 2 {
		t.Fatalf("one busy thread and one waiting job must add a thread: %d", wanted)
	}

	if err := m.SetConcurrency("w", 5); err == nil {
		t.Fatal("with autoscale on, a count above max must fail")
	}

	if err := m.SetAutoscale("w", &Autoscale{Min: 4, Max: 3}); err == nil {
		t.Fatal("min above max must fail")
	}

	if err := m.SetAutoscale("w", nil); err != nil {
		t.Fatal(err)
	}

	if m.Workers()[0].Autoscale != nil {
		t.Fatal("nil must turn autoscale off")
	}
}

func TestHelloAutoscaleIsKeptOnlyFromTheFirstConnection(t *testing.T) {
	m := New(openStore(t, t.TempDir()))

	m.Register(WorkerInfo{WID: "w", Concurrency: 5, Autoscale: &Autoscale{Max: 20}})
	m.Beat("w", "", 5)

	if err := m.SetAutoscale("w", nil); err != nil {
		t.Fatal(err)
	}

	m.Register(WorkerInfo{WID: "w", Concurrency: 5, Autoscale: &Autoscale{Max: 20}})

	worker := m.Workers()[0]

	if worker.Autoscale != nil || worker.Connections != 2 {
		t.Fatalf("a second connection must not undo a change: %+v", worker)
	}

	m.Register(WorkerInfo{WID: "x", Concurrency: 5, Autoscale: &Autoscale{Max: 20}})

	for _, worker := range m.Workers() {
		if worker.WID == "x" && (worker.Autoscale == nil || worker.Autoscale.Min != 5 || worker.Autoscale.Max != 20) {
			t.Fatalf("HELLO autoscale must start with min at the start count: %+v", worker.Autoscale)
		}
	}
}

func TestALimitCapsTheThreadsAndTheAutoscaleMax(t *testing.T) {
	m := New(openStore(t, t.TempDir()))

	m.Register(WorkerInfo{WID: "w", Concurrency: 5, Limit: 10, Autoscale: &Autoscale{Max: 40}})
	m.Beat("w", "", 5)

	if scale := m.Workers()[0].Autoscale; scale == nil || scale.Max != 10 {
		t.Fatalf("a HELLO max above the limit must go down to the limit: %+v", scale)
	}

	if err := m.SetAutoscale("w", &Autoscale{Max: 12}); err == nil {
		t.Fatal("an autoscale max above the limit must fail")
	}

	if err := m.SetAutoscale("w", nil); err != nil {
		t.Fatal(err)
	}

	if err := m.SetConcurrency("w", 11); err == nil {
		t.Fatal("a count above the limit must fail")
	}

	if err := m.SetConcurrency("w", 10); err != nil {
		t.Fatal(err)
	}
}

// restart closes the store and opens a new manager on the same data, as a
// server restart does.
func restart(t *testing.T, dir string, store *storage.Bolt) (*Manager, *storage.Bolt) {
	t.Helper()

	store.Close()

	reopened := openStore(t, dir)

	t.Cleanup(func() { reopened.Close() })

	return New(reopened), reopened
}

func TestAConsoleSettingWinsOverTheConfigAfterARestart(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	m := New(store)
	hello := WorkerInfo{WID: "first", Hostname: "mac", Tag: "shop", Concurrency: 1, Autoscale: &Autoscale{Max: 50}}

	m.Register(hello)
	m.Beat("first", "", 1)

	if err := m.SetAutoscale("first", &Autoscale{Min: 1, Max: 1}); err != nil {
		t.Fatal(err)
	}

	m, store = restart(t, dir, store)
	hello.WID = "second"

	m.Register(hello)

	worker := m.Workers()[0]

	if worker.Autoscale == nil || *worker.Autoscale != (Autoscale{Min: 1, Max: 1}) || !worker.Saved {
		t.Fatalf("the saved 1-1 must win over max 50 from the config: %+v saved=%v", worker.Autoscale, worker.Saved)
	}

	m.Beat("second", "", 1)

	if err := m.Scale("second", ScaleChange{Reset: true}); err != nil {
		t.Fatal(err)
	}

	worker = m.Workers()[0]

	if worker.Autoscale == nil || *worker.Autoscale != (Autoscale{Min: 1, Max: 50}) || worker.Saved {
		t.Fatalf("a reset must bring back the config: %+v saved=%v", worker.Autoscale, worker.Saved)
	}

	m, _ = restart(t, dir, store)
	hello.WID = "third"

	m.Register(hello)

	if worker := m.Workers()[0]; worker.Saved || worker.Autoscale == nil || worker.Autoscale.Max != 50 {
		t.Fatalf("after a reset, a restart must use the config: %+v", worker)
	}
}

func TestASavedThreadCountComesBackWithinTheLimit(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	m := New(store)

	m.Register(WorkerInfo{WID: "a", Hostname: "mac", Tag: "shop", Concurrency: 5})
	m.Beat("a", "", 5)

	if err := m.Scale("a", ScaleChange{SetAutoscale: true, Concurrency: 30}); err != nil {
		t.Fatal(err)
	}

	m, store = restart(t, dir, store)

	m.Register(WorkerInfo{WID: "b", Hostname: "mac", Tag: "shop", Concurrency: 5, Limit: 20})
	m.Register(WorkerInfo{WID: "c", Hostname: "other", Tag: "shop", Concurrency: 5})
	m.Beat("b", "", 5)
	m.Beat("c", "", 5)

	for _, worker := range m.Workers() {
		switch worker.WID {
		case "b":
			if worker.Wanted != 20 || worker.Autoscale != nil || !worker.Saved {
				t.Fatalf("the saved 30 threads must come back, lowered to the limit 20: %+v", worker)
			}
		case "c":
			if worker.Wanted != 0 || worker.Saved {
				t.Fatalf("another worker name must not get the setting: %+v", worker)
			}
		}
	}

	if _, found, _ := store.LoadWorkerSetting(workerName("mac", "shop")); !found {
		t.Fatal("the setting must stay saved")
	}
}

func TestABurstDoublesTheThreadsAndAddsFifteenPercent(t *testing.T) {
	now := time.Now()
	worker := &WorkerInfo{Concurrency: 4, Autoscale: &Autoscale{Min: 1, Max: 30}}

	autoscale(worker, workerLoad{backlog: 2000, busy: 4}, now)

	if worker.Wanted != 9 {
		t.Fatalf("2,000 waiting jobs must take 4 threads to 4 + 4 + 15%% = 9: got %d", worker.Wanted)
	}

	worker = &WorkerInfo{Concurrency: 16, Autoscale: &Autoscale{Min: 1, Max: 30}}

	autoscale(worker, workerLoad{backlog: 2000, busy: 16}, now)

	if worker.Wanted != 30 {
		t.Fatalf("a burst must stop at max: got %d", worker.Wanted)
	}

	worker = &WorkerInfo{Concurrency: 8, Autoscale: &Autoscale{Min: 1, Max: 30}}

	autoscale(worker, workerLoad{backlog: 15, busy: 8}, now)

	if worker.Wanted != 10 {
		t.Fatalf("under 2 waiting jobs for each thread, the step stays a quarter: got %d", worker.Wanted)
	}
}

func TestABeatThatConfirmsAChangeDoesNotScaleAgain(t *testing.T) {
	m := New(openStore(t, t.TempDir()))

	m.Register(WorkerInfo{WID: "w", Concurrency: 4, Queues: []string{"mail"}})
	m.Beat("w", "", 4)

	if err := m.SetAutoscale("w", &Autoscale{Min: 1, Max: 30}); err != nil {
		t.Fatal(err)
	}

	for index := range 20 {
		if err := m.Push(&job.Job{JID: fmt.Sprint("j", index), Type: "x", Args: json.RawMessage(`[]`), Queue: "mail"}); err != nil {
			t.Fatal(err)
		}
	}

	for range 4 {
		if _, err := m.Fetch(t.Context(), []string{"mail"}, "w"); err != nil {
			t.Fatal(err)
		}
	}

	if _, wanted := m.Beat("w", "", 4); wanted != 9 {
		t.Fatalf("16 waiting jobs for 4 busy threads is a burst: %d", wanted)
	}

	if _, wanted := m.Beat("w", "", 9); wanted != 0 {
		t.Fatalf("the beat that reports 9 only confirms the change: %d", wanted)
	}
}
