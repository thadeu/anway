package server_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"jobserver/internal/manager"
	"jobserver/internal/server"
	"jobserver/internal/storage"
	"jobserver/internal/testclient"
)

type fixture struct {
	addr    string
	dir     string
	manager *manager.Manager
	server  *server.Server
	store   *storage.Bolt
}

func start(t *testing.T, dir, password string) *fixture {
	t.Helper()

	store, err := storage.OpenBolt(dir)
	if err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	jobs := manager.New(store)
	srv := server.New(jobs, password, slog.New(slog.NewTextHandler(io.Discard, nil)))

	go srv.Serve(context.Background(), listener)

	t.Cleanup(func() {
		listener.Close()
		srv.Shutdown(0)
		store.Close()
	})

	return &fixture{addr: listener.Addr().String(), dir: dir, manager: jobs, server: srv, store: store}
}

func dial(t *testing.T, f *fixture, wid string) *testclient.Client {
	t.Helper()

	client, err := testclient.Dial(f.addr, wid, "")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { client.Close() })

	return client
}

func push(t *testing.T, client *testclient.Client, payload map[string]any) {
	t.Helper()

	payload["jobtype"] = "test"
	payload["args"] = []any{1}

	if _, err := client.Command("PUSH", payload); err != nil {
		t.Fatal(err)
	}
}

func fetch(t *testing.T, client *testclient.Client, queues string) map[string]any {
	t.Helper()

	reply, err := client.Command("FETCH", queues)
	if err != nil {
		t.Fatal(err)
	}

	if reply == "" {
		return nil
	}

	decoded := map[string]any{}

	if err := json.Unmarshal([]byte(reply), &decoded); err != nil {
		t.Fatal(err)
	}

	return decoded
}

func stats(t *testing.T, f *fixture) storage.Stats {
	t.Helper()

	current, err := f.manager.Stats()
	if err != nil {
		t.Fatal(err)
	}

	return current
}

func TestPushFetchAck(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")

	push(t, worker, map[string]any{"jid": "a"})

	fetched := fetch(t, worker, "default")
	if fetched["jid"] != "a" || fetched["enqueued_at"] == nil {
		t.Fatalf("unexpected job: %v", fetched)
	}

	if _, err := worker.Command("ACK", map[string]string{"jid": "a"}); err != nil {
		t.Fatal(err)
	}

	if _, err := worker.Command("ACK", map[string]string{"jid": "a"}); err == nil {
		t.Fatal("second ACK must fail")
	}

	if current := stats(t, f); current.Working != 0 || current.Queues["default"] != 0 {
		t.Fatalf("unexpected stats: %+v", current)
	}
}

func TestRejectsInvalidJobs(t *testing.T) {
	f := start(t, t.TempDir(), "")
	client := dial(t, f, "")

	push(t, client, map[string]any{"jid": "a"})

	invalid := []string{
		`{"jid":"a","jobtype":"x","args":[]}`,
		`{"jobtype":"x","args":[]}`,
		`{"jid":"b","args":[]}`,
		`{"jid":"b","jobtype":"x"}`,
		`{"jid":"b","jobtype":"x","args":[],"reserve_for":10}`,
		`{"jid":"b","jobtype":"x","args":[],"queue":"a b"}`,
		`not json`,
	}

	for _, payload := range invalid {
		if _, err := client.Command("PUSH", payload); err == nil {
			t.Fatalf("PUSH must fail: %s", payload)
		}
	}
}

func TestFetchUsesQueueOrder(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")

	push(t, worker, map[string]any{"jid": "low", "queue": "low"})
	push(t, worker, map[string]any{"jid": "high", "queue": "high"})

	if fetched := fetch(t, worker, "high low"); fetched["jid"] != "high" {
		t.Fatalf("unexpected job: %v", fetched)
	}
}

func TestFIFOOrder(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")

	push(t, worker, map[string]any{"jid": "1"})
	push(t, worker, map[string]any{"jid": "2"})

	if fetched := fetch(t, worker, "default"); fetched["jid"] != "1" {
		t.Fatalf("unexpected job: %v", fetched)
	}
}

func TestFetchWakesOnPush(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")
	producer := dial(t, f, "")
	result := make(chan map[string]any, 1)

	go func() { result <- fetch(t, worker, "default") }()

	time.Sleep(100 * time.Millisecond)
	push(t, producer, map[string]any{"jid": "a"})

	select {
	case fetched := <-result:
		if fetched["jid"] != "a" {
			t.Fatalf("unexpected job: %v", fetched)
		}
	case <-time.After(time.Second):
		t.Fatal("FETCH did not wake")
	}
}

func TestProducerCannotFetch(t *testing.T) {
	f := start(t, t.TempDir(), "")
	producer := dial(t, f, "")

	if _, err := producer.Command("FETCH", "default"); err == nil {
		t.Fatal("FETCH must fail")
	}
}

func TestScheduledJob(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")
	at := time.Now().Add(time.Hour)

	push(t, worker, map[string]any{"jid": "a", "at": at.Format(time.RFC3339)})

	if current := stats(t, f); current.Scheduled != 1 {
		t.Fatalf("unexpected stats: %+v", current)
	}

	if err := f.manager.Tick(at.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	if fetched := fetch(t, worker, "default"); fetched["jid"] != "a" {
		t.Fatalf("unexpected job: %v", fetched)
	}
}

func TestFailRetriesThenDies(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")
	report := map[string]any{"jid": "a", "errtype": "Boom", "message": "failed", "backtrace": []string{"one", "two"}}

	push(t, worker, map[string]any{"jid": "a", "retry": 2, "backtrace": 1})

	fetch(t, worker, "default")

	if _, err := worker.Command("FAIL", report); err != nil {
		t.Fatal(err)
	}

	if current := stats(t, f); current.Retries != 1 {
		t.Fatalf("unexpected stats: %+v", current)
	}

	if err := f.manager.Tick(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	fetched := fetch(t, worker, "default")
	failure := fetched["failure"].(map[string]any)

	if failure["errtype"] != "Boom" || len(failure["backtrace"].([]any)) != 1 {
		t.Fatalf("unexpected failure: %v", failure)
	}

	if _, err := worker.Command("FAIL", report); err != nil {
		t.Fatal(err)
	}

	if err := f.manager.Tick(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	fetch(t, worker, "default")

	if _, err := worker.Command("FAIL", report); err != nil {
		t.Fatal(err)
	}

	if current := stats(t, f); current.Dead != 1 || current.Retries != 0 {
		t.Fatalf("unexpected stats: %+v", current)
	}
}

func TestRetryZeroDiscardsAndNegativeDies(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")

	push(t, worker, map[string]any{"jid": "a", "retry": 0})
	push(t, worker, map[string]any{"jid": "b", "retry": -1})

	for _, jid := range []string{"a", "b"} {
		fetch(t, worker, "default")

		if _, err := worker.Command("FAIL", map[string]any{"jid": jid}); err != nil {
			t.Fatal(err)
		}
	}

	if current := stats(t, f); current.Dead != 1 || current.Retries != 0 {
		t.Fatalf("unexpected stats: %+v", current)
	}
}

func TestExpiredReservationRetries(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")

	push(t, worker, map[string]any{"jid": "a", "reserve_for": 60})
	fetch(t, worker, "default")

	if err := f.manager.Tick(time.Now().Add(2 * time.Minute)); err != nil {
		t.Fatal(err)
	}

	if current := stats(t, f); current.Retries != 1 || current.Working != 0 {
		t.Fatalf("unexpected stats: %+v", current)
	}

	if _, err := worker.Command("ACK", map[string]string{"jid": "a"}); err == nil {
		t.Fatal("late ACK must fail")
	}
}

func TestPassword(t *testing.T) {
	f := start(t, t.TempDir(), "secret")

	if _, err := testclient.Dial(f.addr, "", "wrong"); err == nil {
		t.Fatal("wrong password must fail")
	}

	client, err := testclient.Dial(f.addr, "", "secret")
	if err != nil {
		t.Fatal(err)
	}

	client.Close()
}

func TestShutdownSendsQuietThenTerminate(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")
	done := make(chan struct{})

	go func() {
		f.server.Shutdown(400 * time.Millisecond)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)

	reply, err := worker.Command("BEAT", map[string]string{"wid": "w1"})
	if err != nil || !strings.Contains(reply, "quiet") {
		t.Fatalf("unexpected reply: %q %v", reply, err)
	}

	if fetched := fetch(t, worker, "default"); fetched != nil {
		t.Fatalf("quiet worker received a job: %v", fetched)
	}

	time.Sleep(200 * time.Millisecond)

	reply, err = worker.Command("BEAT", map[string]string{"wid": "w1"})
	if err != nil || !strings.Contains(reply, "terminate") {
		t.Fatalf("unexpected reply: %q %v", reply, err)
	}

	<-done
}

func TestBeatNeedsOwnWorkerID(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")

	if _, err := worker.Command("BEAT", map[string]string{"wid": "w2"}); err == nil {
		t.Fatal("BEAT must fail")
	}
}

func TestInfoReportsTotals(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")

	push(t, worker, map[string]any{"jid": "a"})
	push(t, worker, map[string]any{"jid": "b"})
	fetch(t, worker, "default")

	if _, err := worker.Command("ACK", map[string]string{"jid": "a"}); err != nil {
		t.Fatal(err)
	}

	reply, err := worker.Command("INFO", nil)
	if err != nil {
		t.Fatal(err)
	}

	var info struct {
		Faktory struct {
			Processed int            `json:"total_processed"`
			Enqueued  int            `json:"total_enqueued"`
			Queues    map[string]int `json:"queues"`
		} `json:"faktory"`
	}

	if err := json.Unmarshal([]byte(reply), &info); err != nil {
		t.Fatal(err)
	}

	if info.Faktory.Processed != 1 || info.Faktory.Enqueued != 1 || info.Faktory.Queues["default"] != 1 {
		t.Fatalf("unexpected info: %s", reply)
	}
}

func TestPauseSurvivesAndResumeWakes(t *testing.T) {
	dir := t.TempDir()
	f := start(t, dir, "")
	worker := dial(t, f, "w1")

	push(t, worker, map[string]any{"jid": "a"})

	if _, err := worker.Command("QUEUE", "PAUSE default"); err != nil {
		t.Fatal(err)
	}

	if fetched := fetch(t, worker, "default"); fetched != nil {
		t.Fatalf("paused queue sent a job: %v", fetched)
	}

	if _, err := worker.Command("QUEUE", "RESUME *"); err != nil {
		t.Fatal(err)
	}

	if fetched := fetch(t, worker, "default"); fetched["jid"] != "a" {
		t.Fatalf("unexpected job: %v", fetched)
	}
}

func TestQueueRemove(t *testing.T) {
	f := start(t, t.TempDir(), "")
	client := dial(t, f, "")

	push(t, client, map[string]any{"jid": "a", "queue": "old"})

	if _, err := client.Command("QUEUE", "REMOVE old"); err != nil {
		t.Fatal(err)
	}

	push(t, client, map[string]any{"jid": "a"})

	if current := stats(t, f); current.Queues["old"] != 0 || current.Queues["default"] != 1 {
		t.Fatalf("unexpected stats: %+v", current)
	}
}

func TestExpiringJobs(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")
	past := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)

	push(t, worker, map[string]any{"jid": "old", "custom": map[string]any{"expires_at": past}})
	push(t, worker, map[string]any{"jid": "live", "custom": map[string]any{"expires_in": 60}})

	fetched := fetch(t, worker, "default")
	if fetched["jid"] != "live" {
		t.Fatalf("unexpected job: %v", fetched)
	}

	if custom := fetched["custom"].(map[string]any); custom["expires_at"] == nil {
		t.Fatalf("expires_in was not converted: %v", custom)
	}

	if current := stats(t, f); current.Expired != 1 {
		t.Fatalf("unexpected stats: %+v", current)
	}
}

func TestUniqueUntilSuccess(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")
	custom := map[string]any{"unique_for": 600}

	push(t, worker, map[string]any{"jid": "a", "custom": custom})

	_, err := worker.Command("PUSH", `{"jid":"b","jobtype":"test","args":[1],"custom":{"unique_for":600}}`)
	if err == nil || !strings.HasPrefix(err.Error(), "NOTUNIQUE") {
		t.Fatalf("unexpected error: %v", err)
	}

	push(t, worker, map[string]any{"jid": "other-queue", "queue": "q2", "custom": custom})

	fetch(t, worker, "default")

	if _, err := worker.Command("FAIL", map[string]any{"jid": "a"}); err != nil {
		t.Fatal(err)
	}

	if _, err := worker.Command("PUSH", `{"jid":"c","jobtype":"test","args":[1],"custom":{"unique_for":600}}`); err == nil {
		t.Fatal("a job in retry must keep the lock")
	}

	if err := f.manager.Tick(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	fetch(t, worker, "default")

	if _, err := worker.Command("ACK", map[string]string{"jid": "a"}); err != nil {
		t.Fatal(err)
	}

	push(t, worker, map[string]any{"jid": "d", "custom": custom})
}

func TestUniqueUntilStart(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")
	custom := map[string]any{"unique_for": 600, "unique_until": "start"}

	push(t, worker, map[string]any{"jid": "a", "custom": custom})
	fetch(t, worker, "default")
	push(t, worker, map[string]any{"jid": "b", "custom": custom})
}

func TestUniqueLockExpires(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")
	custom := map[string]any{"unique_for": 0.05}

	push(t, worker, map[string]any{"jid": "a", "custom": custom})
	time.Sleep(100 * time.Millisecond)
	push(t, worker, map[string]any{"jid": "b", "custom": custom})
}

func TestMutate(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")
	at := time.Now().Add(time.Hour).Format(time.RFC3339)

	for _, jid := range []string{"a", "b", "c"} {
		push(t, worker, map[string]any{"jid": jid, "at": at})
	}

	commands := []string{
		`{"cmd":"kill","target":"scheduled","filter":{"jids":["a"]}}`,
		`{"cmd":"requeue","target":"scheduled","filter":{"regexp":"*\"jid\":\"b\"*"}}`,
		`{"cmd":"discard","target":"scheduled"}`,
	}

	for _, command := range commands {
		if _, err := worker.Command("MUTATE", command); err != nil {
			t.Fatal(err)
		}
	}

	current := stats(t, f)
	if current.Dead != 1 || current.Queues["default"] != 1 || current.Scheduled != 0 {
		t.Fatalf("unexpected stats: %+v", current)
	}

	if _, err := worker.Command("MUTATE", `{"cmd":"bad","target":"dead"}`); err == nil {
		t.Fatal("invalid cmd must fail")
	}
}

func TestMutateCrossesPages(t *testing.T) {
	f := start(t, t.TempDir(), "")
	client := dial(t, f, "")
	at := time.Now().Add(time.Hour).Format(time.RFC3339)

	for i := range 1200 {
		push(t, client, map[string]any{"jid": strings.Repeat("x", 1+i%5) + time.Now().Format("150405.000000000"), "at": at})
	}

	if _, err := client.Command("MUTATE", `{"cmd":"clear","target":"scheduled"}`); err != nil {
		t.Fatal(err)
	}

	if current := stats(t, f); current.Scheduled != 0 {
		t.Fatalf("unexpected stats: %+v", current)
	}
}

func TestThrottleConcurrencyAndRelease(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")

	f.manager.SetThrottles(map[string]storage.Throttle{"api": {Concurrency: 1, Timeout: time.Minute}})

	push(t, worker, map[string]any{"jid": "a", "queue": "api"})
	push(t, worker, map[string]any{"jid": "b", "queue": "api"})
	push(t, worker, map[string]any{"jid": "c"})

	if fetched := fetch(t, worker, "default api"); fetched["jid"] != "a" {
		t.Fatalf("throttled queue must be first: %v", fetched)
	}

	if fetched := fetch(t, worker, "default api"); fetched["jid"] != "c" {
		t.Fatalf("a full throttle must skip to the next queue: %v", fetched)
	}

	if _, err := worker.Command("ACK", map[string]string{"jid": "a"}); err != nil {
		t.Fatal(err)
	}

	if fetched := fetch(t, worker, "api"); fetched["jid"] != "b" {
		t.Fatalf("unexpected job: %v", fetched)
	}
}

func TestThrottlePerWorkerAndTimeout(t *testing.T) {
	f := start(t, t.TempDir(), "")
	first := dial(t, f, "w1")
	second := dial(t, f, "w2")

	f.manager.SetThrottles(map[string]storage.Throttle{"api": {Worker: 1, Timeout: 100 * time.Millisecond}})

	for _, jid := range []string{"a", "b", "c"} {
		push(t, first, map[string]any{"jid": jid, "queue": "api"})
	}

	fetch(t, first, "api")

	if fetched := fetch(t, second, "api"); fetched["jid"] != "b" {
		t.Fatalf("second worker must receive a job: %v", fetched)
	}

	time.Sleep(150 * time.Millisecond)

	if fetched := fetch(t, first, "api"); fetched["jid"] != "c" {
		t.Fatalf("an expired lease must free the worker: %v", fetched)
	}
}

func finishJob(t *testing.T, worker *testclient.Client, queues string) string {
	t.Helper()

	fetched := fetch(t, worker, queues)
	if fetched == nil {
		t.Fatal("expected a job")
	}

	jid := fetched["jid"].(string)

	if _, err := worker.Command("ACK", map[string]string{"jid": jid}); err != nil {
		t.Fatal(err)
	}

	return fetched["jobtype"].(string)
}

func newBatch(t *testing.T, client *testclient.Client, parent string) string {
	t.Helper()

	definition := map[string]any{
		"parent_bid": parent,
		"success":    map[string]any{"jobtype": "on-success", "args": []any{}},
		"complete":   map[string]any{"jobtype": "on-complete", "args": []any{}},
	}

	bid, err := client.Command("BATCH", "NEW "+encode(t, definition))
	if err != nil {
		t.Fatal(err)
	}

	return bid
}

func encode(t *testing.T, value any) string {
	t.Helper()

	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}

	return string(raw)
}

func batchStatus(t *testing.T, client *testclient.Client, bid string) storage.BatchStatus {
	t.Helper()

	reply, err := client.Command("BATCH", "STATUS "+bid)
	if err != nil {
		t.Fatal(err)
	}

	var status storage.BatchStatus

	if err := json.Unmarshal([]byte(reply), &status); err != nil {
		t.Fatal(err)
	}

	return status
}

func TestBatchCallbacksInOrder(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")
	bid := newBatch(t, worker, "")

	push(t, worker, map[string]any{"jid": "a", "custom": map[string]any{"bid": bid}})
	push(t, worker, map[string]any{"jid": "b", "custom": map[string]any{"bid": bid}})

	if _, err := worker.Command("BATCH", "COMMIT "+bid); err != nil {
		t.Fatal(err)
	}

	if _, err := worker.Command("PUSH", `{"jid":"c","jobtype":"x","args":[],"custom":{"bid":"`+bid+`"}}`); err == nil {
		t.Fatal("a committed batch must reject jobs")
	}

	finishJob(t, worker, "default")

	if fetched := fetch(t, worker, "other"); fetched != nil {
		t.Fatalf("unexpected job: %v", fetched)
	}

	finishJob(t, worker, "default")

	if kind := finishJob(t, worker, "default"); kind != "on-complete" {
		t.Fatalf("complete must be first: %s", kind)
	}

	fetched := fetch(t, worker, "default")
	if fetched["jobtype"] != "on-success" {
		t.Fatalf("unexpected job: %v", fetched)
	}

	custom := fetched["custom"].(map[string]any)
	if custom["_bid"] != bid || custom["_cb"] != "success" {
		t.Fatalf("unexpected custom: %v", custom)
	}

	status := batchStatus(t, worker, bid)
	if status.Total != 2 || status.Pending != 0 || status.Failed != 0 {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestBatchFailureFiresCompleteThenSuccessAfterRetry(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")
	bid := newBatch(t, worker, "")

	push(t, worker, map[string]any{"jid": "a", "custom": map[string]any{"bid": bid}})

	if _, err := worker.Command("BATCH", "COMMIT "+bid); err != nil {
		t.Fatal(err)
	}

	fetch(t, worker, "default")

	if _, err := worker.Command("FAIL", map[string]any{"jid": "a"}); err != nil {
		t.Fatal(err)
	}

	if status := batchStatus(t, worker, bid); status.Pending != 1 || status.Failed != 1 {
		t.Fatalf("unexpected status: %+v", status)
	}

	if kind := finishJob(t, worker, "default"); kind != "on-complete" {
		t.Fatalf("unexpected job: %s", kind)
	}

	if err := f.manager.Tick(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	finishJob(t, worker, "default")

	if status := batchStatus(t, worker, bid); status.Pending != 0 || status.Failed != 0 || status.SuccessState == "" {
		t.Fatalf("unexpected status: %+v", status)
	}

	if kind := finishJob(t, worker, "default"); kind != "on-success" {
		t.Fatalf("unexpected job: %s", kind)
	}
}

func TestEmptyBatchFiresAtCommit(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")
	bid := newBatch(t, worker, "")

	if _, err := worker.Command("BATCH", "COMMIT "+bid); err != nil {
		t.Fatal(err)
	}

	if kind := finishJob(t, worker, "default"); kind != "on-complete" {
		t.Fatalf("unexpected job: %s", kind)
	}
}

func TestParentWaitsForChildSuccessCallback(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")
	parent := newBatch(t, worker, "")
	child := newBatch(t, worker, parent)

	push(t, worker, map[string]any{"jid": "a", "queue": "child", "custom": map[string]any{"bid": child}})

	for _, bid := range []string{child, parent} {
		if _, err := worker.Command("BATCH", "COMMIT "+bid); err != nil {
			t.Fatal(err)
		}
	}

	finishJob(t, worker, "child")

	if kind := finishJob(t, worker, "default"); kind != "on-complete" {
		t.Fatalf("unexpected job: %s", kind)
	}

	if status := batchStatus(t, worker, parent); status.CompleteState == "" || status.SuccessState != "" {
		t.Fatalf("parent success must wait for the child: %+v", status)
	}

	if kind := finishJob(t, worker, "default"); kind != "on-success" {
		t.Fatalf("unexpected job: %s", kind)
	}

	if status := batchStatus(t, worker, parent); status.SuccessState == "" {
		t.Fatalf("unexpected parent status: %+v", status)
	}
}

func TestBatchReopenAndExpiry(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")
	bid := newBatch(t, worker, "")
	abandoned := newBatch(t, worker, "")

	push(t, worker, map[string]any{"jid": "a", "custom": map[string]any{"bid": bid}})

	if _, err := worker.Command("BATCH", "COMMIT "+bid); err != nil {
		t.Fatal(err)
	}

	if _, err := worker.Command("BATCH", "OPEN "+bid); err != nil {
		t.Fatal(err)
	}

	push(t, worker, map[string]any{"jid": "b", "custom": map[string]any{"bid": bid}})

	if status := batchStatus(t, worker, bid); status.Total != 2 {
		t.Fatalf("unexpected status: %+v", status)
	}

	if _, err := worker.Command("BATCH", "COMMIT "+bid); err != nil {
		t.Fatal(err)
	}

	if err := f.manager.Tick(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	if _, err := worker.Command("BATCH", "STATUS "+abandoned); err == nil {
		t.Fatal("an uncommitted batch must expire")
	}

	batchStatus(t, worker, bid)
}

func TestBatchRejectsBadInput(t *testing.T) {
	f := start(t, t.TempDir(), "")
	client := dial(t, f, "")

	if _, err := client.Command("BATCH", `NEW {"description":"no callback"}`); err == nil {
		t.Fatal("a batch needs a callback")
	}

	if _, err := client.Command("PUSH", `{"jid":"a","jobtype":"x","args":[],"custom":{"bid":"missing"}}`); err == nil {
		t.Fatal("an unknown batch must fail")
	}
}

func trackOf(t *testing.T, client *testclient.Client, jid string) storage.Track {
	t.Helper()

	reply, err := client.Command("TRACK", "GET "+jid)
	if err != nil {
		t.Fatal(err)
	}

	var track storage.Track

	if err := json.Unmarshal([]byte(reply), &track); err != nil {
		t.Fatal(err)
	}

	return track
}

func TestTrackingLifecycle(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")

	push(t, worker, map[string]any{"jid": "a", "custom": map[string]any{"track": 1}})
	push(t, worker, map[string]any{"jid": "plain"})

	if track := trackOf(t, worker, "plain"); track.State != "unknown" {
		t.Fatalf("unexpected track: %+v", track)
	}

	if track := trackOf(t, worker, "a"); track.State != "enqueued" {
		t.Fatalf("unexpected track: %+v", track)
	}

	fetch(t, worker, "default")

	update := map[string]any{
		"jid":           "a",
		"percent":       40,
		"desc":          "step 2",
		"reserve_until": time.Now().Add(2 * time.Hour).Format(time.RFC3339),
	}

	if _, err := worker.Command("TRACK", "SET "+encode(t, update)); err != nil {
		t.Fatal(err)
	}

	if err := f.manager.Tick(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	if current := stats(t, f); current.Working != 1 {
		t.Fatalf("the reservation must be extended: %+v", current)
	}

	sooner := map[string]any{"jid": "a", "reserve_until": time.Now().Add(time.Minute).Format(time.RFC3339)}

	if _, err := worker.Command("TRACK", "SET "+encode(t, sooner)); err == nil {
		t.Fatal("a sooner reservation must fail")
	}

	if _, err := worker.Command("ACK", map[string]string{"jid": "a"}); err != nil {
		t.Fatal(err)
	}

	track := trackOf(t, worker, "a")
	if track.State != "success" || track.Percent == nil || *track.Percent != 40 || track.Desc != "step 2" {
		t.Fatalf("unexpected track: %+v", track)
	}

	if err := f.manager.Tick(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	if track := trackOf(t, worker, "a"); track.State != "unknown" {
		t.Fatalf("the track must expire: %+v", track)
	}
}

func TestTrackFailedAndDead(t *testing.T) {
	f := start(t, t.TempDir(), "")
	worker := dial(t, f, "w1")

	push(t, worker, map[string]any{"jid": "a", "custom": map[string]any{"track": 1}})
	push(t, worker, map[string]any{"jid": "b", "retry": -1, "custom": map[string]any{"track": 1}})

	for _, jid := range []string{"a", "b"} {
		fetch(t, worker, "default")

		if _, err := worker.Command("FAIL", map[string]any{"jid": jid}); err != nil {
			t.Fatal(err)
		}
	}

	if track := trackOf(t, worker, "a"); track.State != "failed" {
		t.Fatalf("unexpected track: %+v", track)
	}

	if track := trackOf(t, worker, "b"); track.State != "dead" {
		t.Fatalf("unexpected track: %+v", track)
	}
}
