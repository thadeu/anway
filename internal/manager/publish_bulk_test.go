package manager

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestPublishBulkSendsEachEventToEverySubscriptionAndKeepsTheOrder(t *testing.T) {
	m := New(openStore(t, t.TempDir()))

	subscribe(t, m, "realtime", "chat.broadcast")
	subscribe(t, m, "cassandra", "chat.store")

	results, err := m.PublishBulk([]PublishRequest{
		{Topic: "chat.message", Args: json.RawMessage(`[1]`), ID: "m1"},
		{Topic: "bad topic!", Args: json.RawMessage(`[2]`)},
		{Topic: "chat.message", Args: json.RawMessage(`{"not":"an array"}`)},
		{Topic: "chat.message", Args: json.RawMessage(`[3]`), ID: "m1"},
		{Topic: "chat.message", Args: json.RawMessage(`[4]`)},
		{Topic: "nobody.listens", Args: json.RawMessage(`[5]`)},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 6 {
		t.Fatalf("one result for each event: %+v", results)
	}

	if results[0].Jobs != 2 || results[0].Event != "m1" || results[0].Error != "" {
		t.Fatalf("the first event must make one job for each subscription: %+v", results[0])
	}

	if results[1].Error == "" || results[2].Error == "" || results[1].Jobs != 0 || results[2].Jobs != 0 {
		t.Fatalf("a bad topic and bad args must be refused, and make no job: %+v %+v", results[1], results[2])
	}

	if !results[3].Duplicate || results[3].Jobs != 0 {
		t.Fatalf("a second event id in the same bulk must be a duplicate: %+v", results[3])
	}

	if results[4].Jobs != 2 || results[4].Event == "" {
		t.Fatalf("an event without an id must get one and go in: %+v", results[4])
	}

	if results[5].Jobs != 0 || results[5].Error != "" {
		t.Fatalf("an event with no subscription goes to Dead, as with PUBLISH: %+v", results[5])
	}

	sizes := queueSizes(t, m)

	if sizes["realtime"] != 2 || sizes["cassandra"] != 2 {
		t.Fatalf("each queue must get one copy of the two events that went in: %+v", sizes)
	}

	stats, err := m.Stats()
	if err != nil || stats.Dead != 1 {
		t.Fatalf("the event with no subscription must be in Dead: %+v %v", stats, err)
	}
}

func TestPublishBulkHasLimitsOnEventsAndJobs(t *testing.T) {
	m := New(openStore(t, t.TempDir()))
	events := make([]PublishRequest, 1001)

	for index := range events {
		events[index] = PublishRequest{Topic: "chat.message", Args: json.RawMessage(fmt.Sprintf("[%d]", index))}
	}

	if _, err := m.PublishBulk(events); err == nil {
		t.Fatal("1001 events must be refused")
	}

	for index := range 11 {
		subscribe(t, m, fmt.Sprint("q", index), "chat.copy")
	}

	if _, err := m.PublishBulk(events[:1000]); err == nil || !strings.Contains(err.Error(), "at most 10000 jobs") {
		t.Fatalf("1000 events to 11 subscriptions make 11000 jobs, which is too many: %v", err)
	}

	if sizes := queueSizes(t, m); sizes["q0"] != 0 {
		t.Fatalf("a refused bulk must write nothing: %+v", sizes)
	}

	results, err := m.PublishBulk(events[:900])
	if err != nil || len(results) != 900 || results[899].Jobs != 11 {
		t.Fatalf("900 events to 11 subscriptions make 9900 jobs, which must go in: %v", err)
	}
}
