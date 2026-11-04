package manager

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/thadeu/anway/internal/job"
)

func bulkJob(jid, queue string) *job.Job {
	return &job.Job{JID: jid, Type: "x", Args: json.RawMessage(`[]`), Queue: queue}
}

func TestPushBulkPutsInEveryValidJobAndListsTheOthers(t *testing.T) {
	m := New(openStore(t, t.TempDir()))

	if err := m.Push(bulkJob("taken", "mail")); err != nil {
		t.Fatal(err)
	}

	unique := json.RawMessage(`{"unique_for":60}`)
	first := bulkJob("u1", "mail")
	first.Custom = unique
	second := bulkJob("u2", "mail")
	second.Custom = unique

	jobs := []*job.Job{
		bulkJob("a", "mail"),
		bulkJob("taken", "mail"),
		{JID: "no-type", Args: json.RawMessage(`[]`)},
		bulkJob("b", "mail"),
		bulkJob("b", "mail"),
		first,
		second,
	}

	failures, err := m.PushBulk(jobs)
	if err != nil {
		t.Fatal(err)
	}

	if len(failures) != 4 || !strings.Contains(failures["taken"], "already exists") || !strings.Contains(failures["no-type"], "jobtype") || failures["b"] == "" || failures["u2"] == "" {
		t.Fatalf("a duplicate, a bad job, a repeated jid, and a unique lock must be refused, and only them: %+v", failures)
	}

	stats, _ := m.Stats()

	if stats.Queues["mail"] != 4 {
		t.Fatalf("taken, a, b, and u1 must be in the queue: %d", stats.Queues["mail"])
	}
}

func TestPushBulkTakesAtMostMaxBulkJobs(t *testing.T) {
	m := New(openStore(t, t.TempDir()))
	jobs := make([]*job.Job, 1001)

	for index := range jobs {
		jobs[index] = bulkJob(fmt.Sprint("j", index), "mail")
	}

	if _, err := m.PushBulk(jobs); err == nil {
		t.Fatal("1001 jobs must be refused")
	}

	failures, err := m.PushBulk(jobs[:1000])
	if err != nil || len(failures) != 0 {
		t.Fatalf("1000 jobs must go in: %v %v", failures, err)
	}

	if stats, _ := m.Stats(); stats.Queues["mail"] != 1000 {
		t.Fatalf("all 1000 jobs must be in: %d", stats.Queues["mail"])
	}
}
