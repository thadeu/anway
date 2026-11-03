package manager

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/thadeu/anway/internal/job"
	"github.com/thadeu/anway/internal/storage"
)

func subscribe(t *testing.T, m *Manager, queue, jobtype string) {
	t.Helper()

	if err := m.Subscribe(SubscribeRequest{Topic: "chat.message", Queue: queue, JobType: jobtype}); err != nil {
		t.Fatal(err)
	}
}

func publish(t *testing.T, m *Manager, request PublishRequest) storage.Published {
	t.Helper()

	if request.Topic == "" {
		request.Topic = "chat.message"
	}

	published, err := m.Publish(request)
	if err != nil {
		t.Fatal(err)
	}

	return published
}

func queueSizes(t *testing.T, m *Manager) map[string]uint64 {
	t.Helper()

	stats, err := m.Stats()
	if err != nil {
		t.Fatal(err)
	}

	return stats.Queues
}

func TestPublishSendsOneJobToEachSubscription(t *testing.T) {
	m := New(openStore(t, t.TempDir()))
	retry := 2

	subscribe(t, m, "realtime", "chat.broadcast")
	subscribe(t, m, "cassandra", "chat.store")

	if err := m.Subscribe(SubscribeRequest{Topic: "chat.message", Queue: "push", JobType: "chat.notify", Retry: &retry}); err != nil {
		t.Fatal(err)
	}

	published := publish(t, m, PublishRequest{Args: json.RawMessage(`[{"room":"general","body":"hi"}]`), Custom: map[string]any{"trace": "t1"}})

	if published.Jobs != 3 || published.Duplicate || published.Event == "" {
		t.Fatalf("one event must make 3 jobs: %+v", published)
	}

	sizes := queueSizes(t, m)

	if sizes["realtime"] != 1 || sizes["cassandra"] != 1 || sizes["push"] != 1 {
		t.Fatalf("each subscriber queue must get one copy: %+v", sizes)
	}

	copied, err := m.Fetch(t.Context(), []string{"push"}, "w")
	if err != nil || copied == nil {
		t.Fatalf("fetch: %v %v", copied, err)
	}

	var custom map[string]any

	json.Unmarshal(copied.Custom, &custom)

	if copied.Type != "chat.notify" || string(copied.Args) != `[{"room":"general","body":"hi"}]` || copied.Retry == nil || *copied.Retry != 2 {
		t.Fatalf("the copy must have the subscription job type and retry, and the published args: %+v", copied)
	}

	if custom["topic"] != "chat.message" || custom["event"] != published.Event || custom["trace"] != "t1" {
		t.Fatalf("the copy must carry the topic, the event id, and the published custom: %+v", custom)
	}
}

func TestSubscribeIsIdempotentAndUnsubscribeRemoves(t *testing.T) {
	m := New(openStore(t, t.TempDir()))
	retry := 5

	subscribe(t, m, "push", "chat.notify")

	if err := m.Subscribe(SubscribeRequest{Topic: "chat.message", Queue: "push", JobType: "chat.notify", Retry: &retry}); err != nil {
		t.Fatal(err)
	}

	subscriptions, err := m.Subscriptions("chat.message")
	if err != nil {
		t.Fatal(err)
	}

	if len(subscriptions) != 1 || subscriptions[0].Retry == nil || *subscriptions[0].Retry != 5 {
		t.Fatalf("a second SUBSCRIBE must keep one subscription with the new options: %+v", subscriptions)
	}

	request := SubscribeRequest{Topic: "chat.message", Queue: "push", JobType: "chat.notify"}

	if err := m.Unsubscribe(request); err != nil {
		t.Fatal(err)
	}

	if err := m.Unsubscribe(request); err != nil {
		t.Fatalf("an UNSUBSCRIBE of a missing subscription must be OK: %v", err)
	}

	if subscriptions, _ := m.Subscriptions(""); len(subscriptions) != 0 {
		t.Fatalf("the subscription must be gone: %+v", subscriptions)
	}
}

func TestAnEventIDIsPublishedOnceInItsWindow(t *testing.T) {
	m := New(openStore(t, t.TempDir()))

	subscribe(t, m, "push", "chat.notify")

	first := publish(t, m, PublishRequest{ID: "msg-1"})
	second := publish(t, m, PublishRequest{ID: "msg-1"})

	if first.Jobs != 1 || first.Event != "msg-1" || !second.Duplicate || second.Jobs != 0 {
		t.Fatalf("a second PUBLISH of msg-1 must do nothing: %+v %+v", first, second)
	}

	if size := queueSizes(t, m)["push"]; size != 1 {
		t.Fatalf("a duplicate must make no job: %d", size)
	}

	none := 0.0

	publish(t, m, PublishRequest{ID: "msg-2", UniqueFor: &none})

	if again := publish(t, m, PublishRequest{ID: "msg-2", UniqueFor: &none}); again.Duplicate {
		t.Fatal("unique_for 0 must turn the dedup off")
	}

	if other := publish(t, m, PublishRequest{Topic: "chat.read", ID: "msg-1"}); other.Duplicate {
		t.Fatal("the same id in another topic is another event")
	}
}

func TestTheDedupWindowEnds(t *testing.T) {
	store := openStore(t, t.TempDir())
	now := time.Now()
	event := storage.Event{Topic: "chat.message", ID: "msg-1", Args: json.RawMessage(`[]`), Dedup: time.Minute}

	store.Subscribe(storage.Subscription{Topic: "chat.message", Queue: "push", JobType: "chat.notify"}, now)
	store.Publish(event, now)

	if later, err := store.Publish(event, now.Add(2*time.Minute)); err != nil || later.Duplicate || later.Jobs != 1 {
		t.Fatalf("after its window, the same id must publish again: %+v %v", later, err)
	}
}

func TestAnEventWithNoSubscriptionGoesToDeadAndARequeuePublishesItAgain(t *testing.T) {
	m := New(openStore(t, t.TempDir()))

	published := publish(t, m, PublishRequest{Args: json.RawMessage(`["hi"]`), ID: "msg-1"})

	if published.Jobs != 0 || published.Duplicate {
		t.Fatalf("no subscription must make no job: %+v", published)
	}

	stats, _ := m.Stats()

	if stats.Dead != 1 {
		t.Fatalf("the event must wait in Dead: %+v", stats)
	}

	requeue := storage.Mutation{Command: "requeue", Target: "dead", Match: func(*job.Job, []byte) bool { return true }}

	if _, err := m.Apply(requeue); err != nil {
		t.Fatal(err)
	}

	if stats, _ := m.Stats(); stats.Dead != 1 {
		t.Fatalf("a requeue with still no subscription must keep the event in Dead: %+v", stats)
	}

	subscribe(t, m, "push", "chat.notify")
	subscribe(t, m, "realtime", "chat.broadcast")

	if _, err := m.Apply(requeue); err != nil {
		t.Fatal(err)
	}

	stats, _ = m.Stats()

	if stats.Dead != 0 || stats.Queues["push"] != 1 || stats.Queues["realtime"] != 1 {
		t.Fatalf("a requeue must publish the event to the subscriptions of now: %+v", stats)
	}

	copied, _ := m.Fetch(t.Context(), []string{"push"}, "w")

	var custom map[string]any

	json.Unmarshal(copied.Custom, &custom)

	if string(copied.Args) != `["hi"]` || custom["event"] != "msg-1" || custom["unrouted"] != nil {
		t.Fatalf("the copy must have the event args and id, and no unrouted mark: %+v %+v", copied, custom)
	}
}

func TestATopicHasAtMost256Subscriptions(t *testing.T) {
	m := New(openStore(t, t.TempDir()))

	for index := range storage.MaxSubscriptions {
		subscribe(t, m, fmt.Sprint("q", index), "x")
	}

	if err := m.Subscribe(SubscribeRequest{Topic: "chat.message", Queue: "one-more", JobType: "x"}); err == nil {
		t.Fatal("the 257th subscription must fail")
	}

	subscribe(t, m, "q0", "x")
}

func TestPublishAndSubscribeCheckTheirInput(t *testing.T) {
	m := New(openStore(t, t.TempDir()))
	month := 31 * 24 * 3600.0

	bad := []PublishRequest{
		{Topic: "bad topic"},
		{Topic: "chat.message", Args: json.RawMessage(`{"a":1}`)},
		{Topic: "chat.message", ID: "x", UniqueFor: &month},
	}

	for _, request := range bad {
		if _, err := m.Publish(request); err == nil {
			t.Fatalf("PUBLISH must refuse %+v", request)
		}
	}

	for _, request := range []SubscribeRequest{{Topic: "chat.message", Queue: "push"}, {Topic: "chat", Queue: "bad queue", JobType: "x"}, {Topic: "chat", Queue: "push", JobType: "x", ReserveFor: 5}} {
		if err := m.Subscribe(request); err == nil {
			t.Fatalf("SUBSCRIBE must refuse %+v", request)
		}
	}
}
