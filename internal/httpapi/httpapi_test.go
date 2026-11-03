package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/thadeu/anway/internal/httpapi"
	"github.com/thadeu/anway/internal/job"
	"github.com/thadeu/anway/internal/manager"
	"github.com/thadeu/anway/internal/storage"
)

const token = "secret"

type fixture struct {
	t       *testing.T
	manager *manager.Manager
	server  *httptest.Server
}

func start(t *testing.T) *fixture {
	t.Helper()

	store, err := storage.OpenBolt(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	jobs := manager.New(store)
	handler := httpapi.Handler(jobs, httpapi.Options{Token: token, Version: "test", Started: time.Now()})
	server := httptest.NewServer(handler)

	t.Cleanup(func() {
		server.Close()
		store.Close()
	})

	return &fixture{t: t, manager: jobs, server: server}
}

func (f *fixture) call(method, path string, body any, out any) int {
	f.t.Helper()

	var reader io.Reader

	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			f.t.Fatal(err)
		}

		reader = bytes.NewReader(raw)
	}

	request, err := http.NewRequest(method, f.server.URL+path, reader)
	if err != nil {
		f.t.Fatal(err)
	}

	request.Header.Set("Authorization", "Bearer "+token)

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		f.t.Fatal(err)
	}
	defer response.Body.Close()

	if out != nil {
		if err := json.NewDecoder(response.Body).Decode(out); err != nil {
			f.t.Fatal(err)
		}
	}

	return response.StatusCode
}

func (f *fixture) push(jid, jobType, queue string, at time.Time) {
	f.t.Helper()

	pushed := &job.Job{JID: jid, Type: jobType, Args: json.RawMessage(`[1]`), Queue: queue}

	if !at.IsZero() {
		pushed.At = at.UTC().Format(time.RFC3339)
	}

	if err := f.manager.Push(pushed); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) failJob(jobType, message string) string {
	f.t.Helper()

	jid := fmt.Sprintf("%s-%d", jobType, time.Now().UnixNano())

	f.push(jid, jobType, "default", time.Time{})

	if _, err := f.manager.Fetch(f.t.Context(), []string{"default"}, "w1"); err != nil {
		f.t.Fatal(err)
	}

	report := manager.FailReport{JID: jid, ErrType: "Timeout", Message: message}

	if err := f.manager.Fail(report); err != nil {
		f.t.Fatal(err)
	}

	return jid
}

func TestHealthNeedsNoTokenAndAPIDoes(t *testing.T) {
	f := start(t)

	response, err := http.Get(f.server.URL + "/healthz")
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("health failed: %v %v", response, err)
	}

	response, err = http.Get(f.server.URL + "/api/v1/overview")
	if err != nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("the API must need the token: %v %v", response, err)
	}
}

func TestOverviewAndMetrics(t *testing.T) {
	f := start(t)

	f.push("a", "x", "default", time.Time{})
	f.push("b", "x", "default", time.Time{})
	f.push("c", "x", "default", time.Now().Add(time.Hour))

	fetched, err := f.manager.Fetch(t.Context(), []string{"default"}, "w1")
	if err != nil || fetched == nil {
		t.Fatalf("fetch failed: %v", err)
	}

	if err := f.manager.Ack(fetched.JID); err != nil {
		t.Fatal(err)
	}

	var overview struct {
		States map[string]int `json:"states"`
		Totals map[string]int `json:"totals"`
	}

	f.call("GET", "/api/v1/overview", nil, &overview)

	if overview.States["waiting"] != 1 || overview.States["scheduled"] != 1 || overview.Totals["processed"] != 1 {
		t.Fatalf("unexpected overview: %+v", overview)
	}

	var metrics struct {
		Points []manager.Point `json:"points"`
	}

	f.call("GET", "/api/v1/metrics?range=15m", nil, &metrics)

	if len(metrics.Points) != 15 || metrics.Points[14].Done != 1 {
		t.Fatalf("unexpected metrics: %+v", metrics.Points)
	}

	if status := f.call("GET", "/api/v1/metrics?range=90d", nil, nil); status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	}

	until := time.Now().UTC()
	from := until.Add(-10 * time.Minute)

	var window struct {
		Range  string          `json:"range"`
		Points []manager.Point `json:"points"`
	}

	f.call("GET", "/api/v1/metrics?from="+from.Format(time.RFC3339)+"&until="+until.Format(time.RFC3339), nil, &window)

	if window.Range != "custom" || len(window.Points) != 11 || window.Points[10].Done != 1 {
		t.Fatalf("unexpected window: %+v", window)
	}

	for _, bad := range []string{"from=x&until=y", "from=" + until.Format(time.RFC3339) + "&until=" + from.Format(time.RFC3339), "from=2000-01-01T00:00:00Z&until=2026-01-01T00:00:00Z"} {
		if status := f.call("GET", "/api/v1/metrics?"+bad, nil, nil); status != http.StatusBadRequest {
			t.Fatalf("a bad window must fail: %s %d", bad, status)
		}
	}
}

func TestQueuesPauseResumeClear(t *testing.T) {
	f := start(t)

	f.push("a", "x", "mail", time.Time{})
	f.push("b", "x", "mail", time.Time{})

	if status := f.call("POST", "/api/v1/queues/mail/pause", nil, nil); status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	}

	var queues struct {
		Queues []struct {
			Name          string `json:"name"`
			Paused        bool   `json:"paused"`
			Size          int    `json:"size"`
			OldestSeconds *int   `json:"oldest_seconds"`
		} `json:"queues"`
	}

	f.call("GET", "/api/v1/queues", nil, &queues)

	if len(queues.Queues) != 1 || !queues.Queues[0].Paused || queues.Queues[0].Size != 2 || queues.Queues[0].OldestSeconds == nil {
		t.Fatalf("unexpected queues: %+v", queues)
	}

	var cleared struct {
		Deleted int `json:"deleted"`
	}

	f.call("DELETE", "/api/v1/queues/mail/jobs", nil, &cleared)
	f.call("GET", "/api/v1/queues", nil, &queues)

	if cleared.Deleted != 2 || queues.Queues[0].Size != 0 || !queues.Queues[0].Paused {
		t.Fatalf("clear must keep the paused state: %+v %+v", cleared, queues)
	}

	f.call("POST", "/api/v1/queues/mail/resume", nil, nil)
	f.call("GET", "/api/v1/queues", nil, &queues)

	if queues.Queues[0].Paused {
		t.Fatal("resume failed")
	}

	if status := f.call("POST", "/api/v1/queues/bad%20name/pause", nil, nil); status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestJobsPagesWithCursor(t *testing.T) {
	f := start(t)
	at := time.Now().Add(time.Hour)

	for i := range 120 {
		f.push(fmt.Sprintf("j%03d", i), "x", "default", at.Add(time.Duration(i)*time.Second))
	}

	seen := map[string]bool{}
	cursor := ""
	pages := 0
	returned := 0

	for {
		var page struct {
			Jobs []struct {
				JID string `json:"jid"`
			} `json:"jobs"`
			Next string `json:"next_cursor"`
		}

		f.call("GET", "/api/v1/jobs?set=scheduled&limit=50&cursor="+cursor, nil, &page)

		for _, entry := range page.Jobs {
			seen[entry.JID] = true
			returned++
		}

		pages++
		cursor = page.Next

		if cursor == "" {
			break
		}
	}

	if len(seen) != 120 || returned != 120 || pages != 3 {
		t.Fatalf("unexpected paging: %d jobs, %d returned, %d pages", len(seen), returned, pages)
	}

	if status := f.call("GET", "/api/v1/jobs?set=nope", nil, nil); status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestRunningAndWaitingJobsWithFilters(t *testing.T) {
	f := start(t)

	for i := range 6 {
		jobType := "mail.deliver"

		if i%2 == 1 {
			jobType = "report.build"
		}

		f.push(fmt.Sprintf("j%d", i), jobType, "default", time.Time{})
	}

	before := time.Now().Add(-time.Second)

	for _, wid := range []string{"w1", "w1", "w2"} {
		if _, err := f.manager.Fetch(t.Context(), []string{"default"}, wid); err != nil {
			t.Fatal(err)
		}
	}

	type page struct {
		Jobs []struct {
			JID        string     `json:"jid"`
			JobType    string     `json:"jobtype"`
			Worker     string     `json:"worker"`
			EnqueuedAt string     `json:"enqueued_at"`
			StartedAt  *time.Time `json:"started_at"`
		} `json:"jobs"`
		Next string `json:"next_cursor"`
	}

	var running page

	f.call("GET", "/api/v1/jobs?set=working&worker=w1", nil, &running)

	if len(running.Jobs) != 2 || running.Next != "" {
		t.Fatalf("unexpected running jobs: %+v", running)
	}

	for _, entry := range running.Jobs {
		if entry.Worker != "w1" || entry.StartedAt == nil || entry.StartedAt.Before(before) || entry.StartedAt.After(time.Now()) {
			t.Fatalf("a running job needs its worker and start time: %+v", entry)
		}
	}

	var waiting page

	f.call("GET", "/api/v1/jobs?set=queue:default&jobtype=report&limit=1", nil, &waiting)

	if len(waiting.Jobs) != 1 || waiting.Jobs[0].JobType != "report.build" || waiting.Jobs[0].EnqueuedAt == "" || waiting.Next == "" {
		t.Fatalf("unexpected waiting page: %+v", waiting)
	}

	f.call("GET", "/api/v1/jobs?set=queue:default&jobtype=report&limit=1&cursor="+waiting.Next, nil, &waiting)

	if len(waiting.Jobs) != 1 || waiting.Jobs[0].JobType != "report.build" || waiting.Next != "" {
		t.Fatalf("unexpected second waiting page: %+v", waiting)
	}

	wrapped := &job.Job{JID: "aj1", Type: "ActiveJob", Args: json.RawMessage(`[{}]`), Queue: "default", Custom: json.RawMessage(`{"wrapped":"PublishProductJob"}`)}

	if err := f.manager.Push(wrapped); err != nil {
		t.Fatal(err)
	}

	var byClass page

	f.call("GET", "/api/v1/jobs?set=queue:default&jobtype=Publish", nil, &byClass)

	if len(byClass.Jobs) != 1 || byClass.Jobs[0].JID != "aj1" {
		t.Fatalf("the filter must match the wrapped class: %+v", byClass)
	}

	var none page

	f.call("GET", "/api/v1/jobs?set=queue:default&queue=other", nil, &none)

	if len(none.Jobs) != 0 || none.Next != "" {
		t.Fatalf("the queue filter must match no job: %+v", none)
	}
}

func TestWaitingJobsOfAllQueuesOldestFirst(t *testing.T) {
	f := start(t)

	order := []struct{ jid, queue, jobType string }{
		{"w1", "beta", "mail.send"},
		{"w2", "alpha", "report.build"},
		{"w3", "beta", "mail.send"},
		{"w4", "gamma", "mail.send"},
		{"w5", "alpha", "mail.send"},
	}

	for _, entry := range order {
		f.push(entry.jid, entry.jobType, entry.queue, time.Time{})
		time.Sleep(2 * time.Millisecond)
	}

	type page struct {
		Jobs []struct {
			JID string `json:"jid"`
		} `json:"jobs"`
		Next string `json:"next_cursor"`
	}

	var seen []string

	cursor := ""

	for pages := 0; ; pages++ {
		var current page

		f.call("GET", "/api/v1/jobs?set=waiting&limit=2&cursor="+cursor, nil, &current)

		for _, entry := range current.Jobs {
			seen = append(seen, entry.JID)
		}

		cursor = current.Next

		if cursor == "" || pages > 5 {
			break
		}
	}

	if fmt.Sprint(seen) != "[w1 w2 w3 w4 w5]" {
		t.Fatalf("the waiting jobs must be oldest first across queues: %v", seen)
	}

	var filtered page

	f.call("GET", "/api/v1/jobs?set=waiting&jobtype=mail&queue=alpha", nil, &filtered)

	if len(filtered.Jobs) != 1 || filtered.Jobs[0].JID != "w5" {
		t.Fatalf("unexpected filtered jobs: %+v", filtered)
	}

	if status := f.call("GET", "/api/v1/jobs?set=waiting&cursor=bad", nil, nil); status != http.StatusBadRequest {
		t.Fatalf("a bad cursor must fail: %d", status)
	}
}

func TestKillRunningJobs(t *testing.T) {
	f := start(t)

	f.push("a1", "video.encode", "media", time.Time{})
	f.push("a2", "video.encode", "media", time.Time{})
	f.push("a3", "report.build", "media", time.Time{})
	f.push("b1", "mail.send", "mail", time.Time{})

	for _, step := range []struct{ queue, wid string }{{"media", "w1"}, {"media", "w1"}, {"media", "w2"}, {"mail", "w2"}} {
		if _, err := f.manager.Fetch(t.Context(), []string{step.queue}, step.wid); err != nil {
			t.Fatal(err)
		}
	}

	var result struct {
		Changed int `json:"changed"`
	}

	if status := f.call("POST", "/api/v1/jobs/actions", map[string]any{"action": "requeue", "set": "working", "all": true}, nil); status != http.StatusBadRequest {
		t.Fatalf("running jobs must accept only kill: %d", status)
	}

	f.call("POST", "/api/v1/jobs/actions", map[string]any{"action": "kill", "set": "working", "jids": []string{"b1"}}, &result)

	if result.Changed != 1 {
		t.Fatalf("unexpected kill by jid: %+v", result)
	}

	f.call("POST", "/api/v1/jobs/actions", map[string]any{"action": "kill", "set": "working", "all": true, "queue": "media", "jobtype": "video"}, &result)

	if result.Changed != 2 {
		t.Fatalf("the filter must limit kill all: %+v", result)
	}

	var dead struct {
		Jobs []struct {
			JID     string       `json:"jid"`
			Worker  string       `json:"worker"`
			Failure *job.Failure `json:"failure"`
		} `json:"jobs"`
	}

	f.call("GET", "/api/v1/jobs?set=dead", nil, &dead)

	if len(dead.Jobs) != 3 {
		t.Fatalf("unexpected dead jobs: %+v", dead)
	}

	for _, entry := range dead.Jobs {
		if entry.Failure == nil || entry.Failure.ErrType != "Killed" || entry.Worker != "" {
			t.Fatalf("a killed job needs a failure and no worker: %+v", entry)
		}
	}

	var overview struct {
		States map[string]int `json:"states"`
	}

	f.call("GET", "/api/v1/overview", nil, &overview)

	if overview.States["working"] != 1 || overview.States["dead"] != 3 {
		t.Fatalf("unexpected states: %+v", overview.States)
	}

	if err := f.manager.Ack("a1"); !errors.Is(err, storage.ErrNotReserved) {
		t.Fatalf("the worker ACK must fail after a kill: %v", err)
	}

	if err := f.manager.Ack("a3"); err != nil {
		t.Fatalf("a job that the filter skipped must still run: %v", err)
	}
}

func TestGroupsAndGroupActions(t *testing.T) {
	f := start(t)

	f.failJob("mail.deliver", "dial tcp 10.0.0.1:25: timeout after 30s")
	f.failJob("mail.deliver", "dial tcp 10.0.0.9:25: timeout after 31s")
	f.failJob("report", "disk full")

	var groups struct {
		Groups []struct {
			Group string `json:"group"`
			Count int    `json:"count"`
		} `json:"groups"`
	}

	f.call("GET", "/api/v1/jobs/groups?set=retries", nil, &groups)

	if len(groups.Groups) != 2 || groups.Groups[0].Count != 2 {
		t.Fatalf("numbers must not split a group: %+v", groups)
	}

	var result struct {
		Changed int `json:"changed"`
	}

	body := map[string]any{"action": "kill", "set": "retries", "group": groups.Groups[0].Group}

	f.call("POST", "/api/v1/jobs/actions", body, &result)

	if result.Changed != 2 {
		t.Fatalf("unexpected result: %+v", result)
	}

	if status := f.call("POST", "/api/v1/jobs/actions", map[string]any{"action": "discard", "set": "retries"}, nil); status != http.StatusBadRequest {
		t.Fatalf("an action without a target must fail: %d", status)
	}
}

func TestEnqueue(t *testing.T) {
	f := start(t)
	body := map[string]any{"jobtype": "x", "args": []any{1}, "custom": map[string]any{"unique_for": 60}}

	var created struct {
		JID string `json:"jid"`
	}

	if status := f.call("POST", "/api/v1/jobs", body, &created); status != http.StatusCreated || created.JID == "" {
		t.Fatalf("unexpected result: %d %+v", status, created)
	}

	if status := f.call("POST", "/api/v1/jobs", body, nil); status != http.StatusConflict {
		t.Fatalf("a duplicate unique job must conflict: %d", status)
	}

	if status := f.call("POST", "/api/v1/jobs", map[string]any{"args": []any{}}, nil); status != http.StatusBadRequest {
		t.Fatalf("a job without a type must fail: %d", status)
	}
}

func TestWorkersShowConcurrencyAndBusy(t *testing.T) {
	f := start(t)

	f.manager.Register(manager.WorkerInfo{WID: "w1", Hostname: "mta-1", Labels: []string{"go"}, Concurrency: 200, Tag: "shop", Queues: []string{"mailers", "default"}})
	f.push("a", "x", "default", time.Time{})

	if _, err := f.manager.Fetch(t.Context(), []string{"default"}, "w1"); err != nil {
		t.Fatal(err)
	}

	var workers struct {
		Workers []map[string]any `json:"workers"`
	}

	f.call("GET", "/api/v1/workers", nil, &workers)

	if len(workers.Workers) != 1 || workers.Workers[0]["concurrency"] != float64(200) || workers.Workers[0]["busy"] != float64(1) {
		t.Fatalf("unexpected workers: %+v", workers)
	}

	if workers.Workers[0]["tag"] != "shop" || fmt.Sprint(workers.Workers[0]["queues"]) != "[mailers default]" || workers.Workers[0]["started_seconds"] == nil {
		t.Fatalf("a worker needs its tag, queues, and start: %+v", workers.Workers[0])
	}

	var overview struct {
		Threads map[string]int `json:"threads"`
	}

	f.call("GET", "/api/v1/overview", nil, &overview)

	if overview.Threads["busy"] != 1 || overview.Threads["total"] != 200 || overview.Threads["max"] != 200 {
		t.Fatalf("unexpected threads: %+v", overview.Threads)
	}

	f.manager.Register(manager.WorkerInfo{WID: "w2", Hostname: "mta-2", Concurrency: 5, Autoscale: &manager.Autoscale{Max: 30}})
	f.call("GET", "/api/v1/overview", nil, &overview)

	if overview.Threads["total"] != 205 || overview.Threads["max"] != 230 {
		t.Fatalf("max must add the autoscale max of a worker, not its count: %+v", overview.Threads)
	}
}

func TestScaleWorkerSetsThreadsAndAutoscale(t *testing.T) {
	f := start(t)

	f.manager.Register(manager.WorkerInfo{WID: "old", Concurrency: 5})
	f.manager.Register(manager.WorkerInfo{WID: "w1", Hostname: "mac", Tag: "shop", Concurrency: 5})
	f.manager.Beat("w1", "", 5)

	if status := f.call("PATCH", "/api/v1/workers/nobody", map[string]any{"concurrency": 3}, nil); status != http.StatusNotFound {
		t.Fatalf("an unknown worker must be 404: %d", status)
	}

	if status := f.call("PATCH", "/api/v1/workers/old", map[string]any{"concurrency": 3}, nil); status != http.StatusConflict {
		t.Fatalf("a worker that does not report its threads must be 409: %d", status)
	}

	if status := f.call("PATCH", "/api/v1/workers/w1", map[string]any{"concurrency": 0}, nil); status != http.StatusBadRequest {
		t.Fatalf("0 threads must be 400: %d", status)
	}

	var worker map[string]any

	if status := f.call("PATCH", "/api/v1/workers/w1", map[string]any{"concurrency": 12}, &worker); status != http.StatusOK || worker["wanted"] != float64(12) || worker["concurrency"] != float64(5) {
		t.Fatalf("the reply must show the wanted count: %d %+v", status, worker)
	}

	f.call("PATCH", "/api/v1/workers/w1", map[string]any{"autoscale": map[string]int{"max": 30}}, &worker)

	if fmt.Sprint(worker["autoscale"]) != "map[max:30 min:5]" || worker["scalable"] != true {
		t.Fatalf("autoscale must start at the start count: %+v", worker)
	}

	if status := f.call("PATCH", "/api/v1/workers/w1", map[string]any{"autoscale": map[string]int{"max": 2}}, nil); status != http.StatusBadRequest {
		t.Fatalf("max under min must be 400: %d", status)
	}

	if status := f.call("PATCH", "/api/v1/workers/w1", map[string]any{"autoscale": false, "concurrency": 2000}, nil); status != http.StatusBadRequest {
		t.Fatalf("a bad count must be 400: %d", status)
	}

	f.call("GET", "/api/v1/workers", nil, nil)

	if f.manager.Workers()[1].Autoscale == nil {
		t.Fatal("a refused change must not turn autoscale off")
	}

	if worker := f.manager.Workers()[1]; !worker.Saved {
		t.Fatal("a change for a named worker must be saved")
	}

	if status := f.call("PATCH", "/api/v1/workers/w1", map[string]any{"reset": true, "concurrency": 3}, nil); status != http.StatusBadRequest {
		t.Fatalf("a reset with a change must be 400: %d", status)
	}

	f.call("PATCH", "/api/v1/workers/w1", map[string]any{"reset": true}, &worker)

	if worker["saved"] != false {
		t.Fatalf("a reset must drop the saved setting: %+v", worker)
	}

	f.call("PATCH", "/api/v1/workers/w1", map[string]any{"autoscale": false}, &worker)

	if worker["autoscale"] != nil {
		t.Fatalf("false must turn autoscale off: %+v", worker)
	}
}

func TestTopicsListSubscriptionsAndTheirWorkers(t *testing.T) {
	f := start(t)

	for _, subscription := range []manager.SubscribeRequest{
		{Topic: "chat.message", Queue: "push", JobType: "chat.notify"},
		{Topic: "chat.message", Queue: "realtime", JobType: "chat.broadcast"},
		{Topic: "audit", Queue: "lonely", JobType: "audit.log"},
	} {
		if err := f.manager.Subscribe(subscription); err != nil {
			t.Fatal(err)
		}
	}

	f.manager.Register(manager.WorkerInfo{WID: "ruby", Hostname: "mac", PID: 7, Tag: "shop", Queues: []string{"push"}})
	f.manager.Register(manager.WorkerInfo{WID: "go", Hostname: "box", PID: 9})

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	f.manager.Fetch(ctx, []string{"realtime"}, "go")

	var listed struct {
		Topics []struct {
			Name          string `json:"name"`
			Subscriptions []struct {
				Queue   string           `json:"queue"`
				JobType string           `json:"jobtype"`
				Workers []map[string]any `json:"workers"`
			} `json:"subscriptions"`
		} `json:"topics"`
	}

	f.call("GET", "/api/v1/topics", nil, &listed)

	if len(listed.Topics) != 2 || listed.Topics[0].Name != "audit" || listed.Topics[1].Name != "chat.message" {
		t.Fatalf("topics must be listed by name: %+v", listed)
	}

	audit := listed.Topics[0].Subscriptions
	chat := listed.Topics[1].Subscriptions

	if len(audit) != 1 || len(audit[0].Workers) != 0 {
		t.Fatalf("a subscription that no worker fetches from must have no workers: %+v", audit)
	}

	if len(chat) != 2 || chat[0].Queue != "push" || len(chat[0].Workers) != 1 || chat[0].Workers[0]["tag"] != "shop" {
		t.Fatalf("a worker with the queue in HELLO must show: %+v", chat)
	}

	if chat[1].Queue != "realtime" || len(chat[1].Workers) != 1 || chat[1].Workers[0]["hostname"] != "box" {
		t.Fatalf("a worker that fetches from the queue must show, also without queues in HELLO: %+v", chat)
	}
}

func TestSchedulesAndBatches(t *testing.T) {
	f := start(t)

	f.manager.SetThrottles(map[string]storage.Throttle{"mail": {Concurrency: 4, Timeout: time.Minute}})

	var schedules struct {
		Throttles []map[string]any `json:"throttles"`
	}

	f.call("GET", "/api/v1/schedules", nil, &schedules)

	if len(schedules.Throttles) != 1 || schedules.Throttles[0]["concurrency"] != float64(4) {
		t.Fatalf("unexpected schedules: %+v", schedules)
	}

	callback := &job.Job{JID: "cb", Type: "done", Args: json.RawMessage(`[]`)}

	if _, err := f.manager.BatchNew(storage.BatchDefinition{Success: callback}); err != nil {
		t.Fatal(err)
	}

	var batches struct {
		Batches []map[string]any `json:"batches"`
	}

	f.call("GET", "/api/v1/batches", nil, &batches)

	if len(batches.Batches) != 1 || batches.Batches[0]["committed"] != false {
		t.Fatalf("unexpected batches: %+v", batches)
	}
}
