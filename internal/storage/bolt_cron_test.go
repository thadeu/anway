package storage

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/thadeu/anway/internal/job"
)

const runEntry = "0123456789ab"

func cronJob(t *testing.T, retry int) *job.Job {
	t.Helper()

	jid, err := job.CronJID(runEntry, false)
	if err != nil {
		t.Fatal(err)
	}

	pushed := &job.Job{JID: jid, Type: "refill", Args: json.RawMessage(`[]`), Queue: "default", Retry: &retry}

	if err := pushed.Normalize(time.Now()); err != nil {
		t.Fatal(err)
	}

	return pushed
}

func runsOf(t *testing.T, store *Bolt) []CronRun {
	t.Helper()

	runs, err := store.CronRuns()
	if err != nil {
		t.Fatal(err)
	}

	return runs[runEntry]
}

func failTo(outcome Outcome) Decide {
	return func(j *job.Job) Disposition {
		j.Failure = &job.Failure{ErrType: "Timeout", Message: "no answer in 30s"}

		return Disposition{Outcome: outcome, At: time.Now().Add(time.Hour)}
	}
}

func TestCronRunFollowsItsJob(t *testing.T) {
	store, err := OpenBolt(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	done := cronJob(t, 3)
	now := time.Now()

	if err := store.Push(done, now); err != nil {
		t.Fatal(err)
	}

	if runs := runsOf(t, store); len(runs) != 1 || runs[0].Status != RunPushed || runs[0].JID != done.JID || runs[0].Manual {
		t.Fatalf("unexpected runs after push: %+v", runs)
	}

	if _, err := store.Fetch([]string{"default"}, "w1", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	if runs := runsOf(t, store); runs[0].Status != RunRunning || runs[0].StartedAt == "" {
		t.Fatalf("unexpected runs after fetch: %+v", runs)
	}

	if _, err := store.Ack(done.JID); err != nil {
		t.Fatal(err)
	}

	run := runsOf(t, store)[0]

	if run.Status != RunDone || run.FinishedAt == "" || run.DurationMS == nil || run.Error != "" {
		t.Fatalf("unexpected run after ack: %+v", run)
	}

	failed := cronJob(t, 0)

	if err := store.Push(failed, now); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Fetch([]string{"default"}, "w1", now); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Fail(failed.JID, now, failTo(Discard)); err != nil {
		t.Fatal(err)
	}

	runs := runsOf(t, store)

	if len(runs) != 2 || runs[0].JID != failed.JID || runs[0].Status != RunFailed || runs[0].Error != "Timeout: no answer in 30s" {
		t.Fatalf("the newest run comes first, with its error: %+v", runs)
	}
}

func TestCronRunGoesDeadAndBackToTheQueue(t *testing.T) {
	store, err := OpenBolt(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	pushed := cronJob(t, 1)
	now := time.Now()

	if err := store.Push(pushed, now); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Fetch([]string{"default"}, "w1", now); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Fail(pushed.JID, now, failTo(Dead)); err != nil {
		t.Fatal(err)
	}

	if run := runsOf(t, store)[0]; run.Status != RunDead || run.Error == "" {
		t.Fatalf("unexpected run: %+v", run)
	}

	match := func(j *job.Job, _ []byte) bool { return j.JID == pushed.JID }

	if _, err := store.Mutate(Mutation{Command: "requeue", Target: "dead", Match: match}, now); err != nil {
		t.Fatal(err)
	}

	if runs := runsOf(t, store); len(runs) != 1 || runs[0].Status != RunPushed {
		t.Fatalf("a requeued run is the same run, back in the queue: %+v", runs)
	}
}

func TestCronRunsKeepTheLastEight(t *testing.T) {
	store, err := OpenBolt(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	var last string

	for range MaxCronRuns + 3 {
		pushed := cronJob(t, 3)
		last = pushed.JID

		if err := store.Push(pushed, time.Now()); err != nil {
			t.Fatal(err)
		}
	}

	runs := runsOf(t, store)

	if len(runs) != MaxCronRuns || runs[0].JID != last {
		t.Fatalf("unexpected runs: %d, first %+v", len(runs), runs[0])
	}

	plain := &job.Job{JID: "web-1", Type: "refill", Args: json.RawMessage(`[]`), Queue: "default"}

	if err := store.Push(plain, time.Now()); err != nil {
		t.Fatal(err)
	}

	all, err := store.CronRuns()
	if err != nil {
		t.Fatal(err)
	}

	if len(all) != 1 {
		t.Fatalf("a job that is not a cron run has no runs: %+v", all)
	}

	if err := store.KeepCronRuns([]string{"ffffffffffff"}); err != nil {
		t.Fatal(err)
	}

	if runs := runsOf(t, store); len(runs) != 0 {
		t.Fatalf("the runs of a removed entry must go: %+v", runs)
	}
}

func TestCronPausesAndTagsSeen(t *testing.T) {
	store, err := OpenBolt(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	for i := range 2 {
		pause := CronPause{ID: fmt.Sprintf("00000000000%d", i), Source: "client", Tag: "shop", JobType: "refill", Schedule: "0 0 * * *", PausedAt: "2026-11-17T12:00:00Z", PausedBy: "Ana"}

		if err := store.SaveCronPause(pause); err != nil {
			t.Fatal(err)
		}
	}

	if err := store.DeleteCronPause("000000000000"); err != nil {
		t.Fatal(err)
	}

	pauses, err := store.CronPauses()
	if err != nil {
		t.Fatal(err)
	}

	if len(pauses) != 1 || pauses[0].ID != "000000000001" || pauses[0].PausedBy != "Ana" {
		t.Fatalf("unexpected pauses: %+v", pauses)
	}

	seen := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)

	if err := store.SaveTagSeen("shop", seen); err != nil {
		t.Fatal(err)
	}

	tags, err := store.TagsSeen()
	if err != nil {
		t.Fatal(err)
	}

	if !tags["shop"].Equal(seen) {
		t.Fatalf("unexpected tags: %+v", tags)
	}

	if err := store.Flush(); err != nil {
		t.Fatal(err)
	}

	if pauses, _ := store.CronPauses(); len(pauses) != 0 {
		t.Fatalf("FLUSH removes the pauses: %+v", pauses)
	}
}
