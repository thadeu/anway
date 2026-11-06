// Package httpapi serves the JSON API for the web UI. Job traffic never uses it.
package httpapi

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/thadeu/anway/internal/cron"
	"github.com/thadeu/anway/internal/job"
	"github.com/thadeu/anway/internal/manager"
	"github.com/thadeu/anway/internal/storage"
)

const (
	maxBody      = 1 << 20
	groupScan    = 10_000
	batchesLimit = 200
	defaultPage  = 50
)

// Options holds what the API needs besides the manager.
type Options struct {
	Token       string
	Version     string
	Started     time.Time
	Connections func() int
	Scheduler   *cron.Scheduler
	Logger      *slog.Logger
}

type API struct {
	manager *manager.Manager
	options Options
}

// Handler returns the routes. All routes under /api need the token, when one is set.
func Handler(m *manager.Manager, options Options) http.Handler {
	api := &API{manager: m, options: options}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", api.health)
	mux.Handle("/api/", api.authorize(api.routes()))

	return mux
}

func (a *API) routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/v1/overview", a.overview)
	mux.HandleFunc("GET /api/v1/metrics", a.metrics)
	mux.HandleFunc("GET /api/v1/queues", a.queues)
	mux.HandleFunc("POST /api/v1/queues/{name}/pause", a.pause)
	mux.HandleFunc("POST /api/v1/queues/{name}/resume", a.resume)
	mux.HandleFunc("DELETE /api/v1/queues/{name}/jobs", a.clear)
	mux.HandleFunc("GET /api/v1/jobs", a.jobs)
	mux.HandleFunc("GET /api/v1/jobs/groups", a.groups)
	mux.HandleFunc("GET /api/v1/jobs/{jid}", a.job)
	mux.HandleFunc("POST /api/v1/jobs/actions", a.actions)
	mux.HandleFunc("POST /api/v1/jobs", a.enqueue)
	mux.HandleFunc("GET /api/v1/workers", a.workers)
	mux.HandleFunc("PATCH /api/v1/workers/{wid}", a.scaleWorker)
	mux.HandleFunc("GET /api/v1/schedules", a.schedules)
	mux.HandleFunc("GET /api/v1/topics", a.topics)
	mux.HandleFunc("GET /api/v1/batches", a.batches)

	return mux
}

func (a *API) authorize(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.options.Token != "" {
			given := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")

			if subtle.ConstantTimeCompare([]byte(given), []byte(a.options.Token)) != 1 {
				writeError(w, http.StatusUnauthorized, "missing or wrong token")

				return
			}
		}

		r.Body = http.MaxBytesReader(w, r.Body, maxBody)

		next.ServeHTTP(w, r)
	})
}

func (a *API) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) overview(w http.ResponseWriter, _ *http.Request) {
	stats, err := a.manager.Stats()
	if err != nil {
		a.fail(w, err)

		return
	}

	var waiting uint64

	for _, size := range stats.Queues {
		waiting += size
	}

	// total is the threads that the workers run now. most is how far they can
	// go: the autoscale max of each worker, or its count when autoscale is off.
	threads, most := 0, 0
	workers := a.manager.Workers()

	for _, worker := range workers {
		threads += worker.Concurrency
		most += worker.Concurrency

		if worker.Autoscale != nil {
			most += max(worker.Autoscale.Max-worker.Concurrency, 0)
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"server": map[string]any{
			"version":        a.options.Version,
			"uptime_seconds": int(time.Since(a.options.Started).Seconds()),
			"connections":    a.connections(),
		},
		"states": map[string]any{
			"scheduled": stats.Scheduled,
			"waiting":   waiting,
			"working":   stats.Working,
			"retries":   stats.Retries,
			"dead":      stats.Dead,
		},
		"totals": map[string]any{
			"processed": stats.Processed,
			"failures":  stats.Failures,
			"expired":   stats.Expired,
		},
		"threads": map[string]any{"busy": stats.Working, "total": threads, "max": most},
		"rate":    a.liveRate(),
		"workers": len(workers),
		"queues":  len(stats.Queues),
		"paused":  nonNil(stats.Paused),
	})
}

// maxWindow is the longest window that from and until can ask for.
const maxWindow = 60 * 24 * time.Hour

func (a *API) metrics(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	if query.Get("from") != "" || query.Get("until") != "" {
		a.window(w, query.Get("from"), query.Get("until"))

		return
	}

	rangeName := query.Get("range")
	if rangeName == "" {
		rangeName = "1h"
	}

	points, err := a.manager.Series(rangeName)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"range": rangeName, "points": points})
}

// window answers a custom window, such as a slice that a user drags on the
// chart. Both ends are RFC 3339 times.
func (a *API) window(w http.ResponseWriter, fromText, untilText string) {
	from, fromErr := time.Parse(time.RFC3339, fromText)
	until, untilErr := time.Parse(time.RFC3339, untilText)

	if fromErr != nil || untilErr != nil {
		writeError(w, http.StatusBadRequest, "from and until must be RFC 3339 times")

		return
	}

	if !until.After(from) || until.Sub(from) > maxWindow {
		writeError(w, http.StatusBadRequest, "until must be after from, and the window at most 60 days")

		return
	}

	points, err := a.manager.Window(from, until)
	if err != nil {
		a.fail(w, err)

		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"range": "custom", "from": from.UTC(), "until": until.UTC(), "points": points})
}

type queueView struct {
	Name            string  `json:"name"`
	Paused          bool    `json:"paused"`
	Size            uint64  `json:"size"`
	OldestSeconds   *int64  `json:"oldest_seconds"`
	PerMinute       float64 `json:"per_minute"`
	ClearsInSeconds *int64  `json:"clears_in_seconds"`
}

func (a *API) queues(w http.ResponseWriter, _ *http.Request) {
	details, err := a.manager.QueueDetails()
	if err != nil {
		a.fail(w, err)

		return
	}

	now := time.Now()
	views := make([]queueView, 0, len(details))

	for _, queue := range details {
		rate := math.Round(a.manager.QueueRate(queue.Name)*10) / 10
		view := queueView{Name: queue.Name, Paused: queue.Paused, Size: queue.Size, PerMinute: rate}

		if !queue.OldestAt.IsZero() {
			oldest := int64(now.Sub(queue.OldestAt).Seconds())
			view.OldestSeconds = &oldest
		}

		if !queue.Paused && rate > 0 {
			clears := int64(float64(queue.Size) / rate * 60)
			view.ClearsInSeconds = &clears
		}

		views = append(views, view)
	}

	writeJSON(w, http.StatusOK, map[string]any{"queues": views})
}

func (a *API) pause(w http.ResponseWriter, r *http.Request) {
	a.queueAction(w, r, a.manager.PauseQueues)
}

func (a *API) resume(w http.ResponseWriter, r *http.Request) {
	a.queueAction(w, r, a.manager.ResumeQueues)
}

func (a *API) queueAction(w http.ResponseWriter, r *http.Request, action func([]string) error) {
	name := r.PathValue("name")

	if !job.ValidQueue(name) {
		writeError(w, http.StatusBadRequest, "invalid queue name")

		return
	}

	if err := action([]string{name}); err != nil {
		a.fail(w, err)

		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"queue": name})
}

func (a *API) clear(w http.ResponseWriter, r *http.Request) {
	deleted, err := a.manager.ClearQueue(r.PathValue("name"))
	if err != nil {
		a.fail(w, err)

		return
	}

	writeJSON(w, http.StatusOK, map[string]int{"deleted": deleted})
}

type jobView struct {
	JID        string          `json:"jid"`
	JobType    string          `json:"jobtype"`
	Queue      string          `json:"queue"`
	Args       json.RawMessage `json:"args"`
	State      string          `json:"state"`
	At         *time.Time      `json:"at,omitempty"`
	Worker     string          `json:"worker,omitempty"`
	CreatedAt  string          `json:"created_at,omitempty"`
	EnqueuedAt string          `json:"enqueued_at,omitempty"`
	StartedAt  *time.Time      `json:"started_at,omitempty"`
	Failure    *job.Failure    `json:"failure,omitempty"`
	Custom     json.RawMessage `json:"custom,omitempty"`
	Group      string          `json:"group"`
}

func viewOf(entry storage.JobEntry) jobView {
	view := jobView{
		JID:        entry.Job.JID,
		JobType:    entry.Job.Type,
		Queue:      entry.Job.Queue,
		Args:       entry.Job.Args,
		State:      entry.State,
		Worker:     entry.WID,
		CreatedAt:  entry.Job.CreatedAt,
		EnqueuedAt: entry.Job.EnqueuedAt,
		Failure:    entry.Job.Failure,
		Custom:     entry.Job.Custom,
		Group:      storage.Fingerprint(&entry.Job),
	}

	if !entry.At.IsZero() {
		at := entry.At
		view.At = &at
	}

	// A working job keeps its reservation expiry, which is the fetch time plus
	// reserve_for, and the reservation never changes.
	if entry.State == "working" && view.At != nil {
		started := entry.At.Add(-time.Duration(entry.Job.ReserveFor) * time.Second)
		view.StartedAt = &started
	}

	return view
}

func (a *API) jobs(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limit, _ := strconv.Atoi(query.Get("limit"))

	if limit == 0 {
		limit = defaultPage
	}

	filter := storage.JobFilter{Queue: query.Get("queue"), JobType: query.Get("jobtype"), Worker: query.Get("worker")}

	page, err := a.manager.ListJobs(query.Get("set"), query.Get("cursor"), limit, filter)
	if err != nil {
		a.fail(w, err)

		return
	}

	views := make([]jobView, 0, len(page.Jobs))

	for _, entry := range page.Jobs {
		views = append(views, viewOf(entry))
	}

	writeJSON(w, http.StatusOK, map[string]any{"jobs": views, "next_cursor": page.Next})
}

// job returns one job in any set, with its set, or 404. A finished job is
// not kept, so it is not found.
func (a *API) job(w http.ResponseWriter, r *http.Request) {
	entry, found, err := a.manager.Job(r.PathValue("jid"))
	if err != nil {
		a.fail(w, err)

		return
	}

	if !found {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})

		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"job": viewOf(entry), "set": setOf(entry.State)})
}

// setOf names a stored state as the sets of GET /api/v1/jobs do.
func setOf(state string) string {
	if state == "queues" {
		return "waiting"
	}

	return state
}

func (a *API) groups(w http.ResponseWriter, r *http.Request) {
	result, err := a.manager.JobGroups(r.URL.Query().Get("set"), groupScan)
	if err != nil {
		a.fail(w, err)

		return
	}

	groups := make([]map[string]any, 0, len(result.Groups))

	for _, group := range result.Groups {
		groups = append(groups, map[string]any{"group": group.Fingerprint, "count": group.Count, "latest": viewOf(group.Latest)})
	}

	writeJSON(w, http.StatusOK, map[string]any{"groups": groups, "scanned": result.Scanned, "truncated": result.Truncated})
}

type actionRequest struct {
	Action string   `json:"action"`
	Set    string   `json:"set"`
	JIDs   []string `json:"jids"`
	Group  string   `json:"group"`
	All    bool     `json:"all"`

	Queue   string `json:"queue"`
	JobType string `json:"jobtype"`
	Worker  string `json:"worker"`
}

// actions changes jobs in one set. A request names jids, a group, or all, so
// that an empty body never changes every job. The optional filters limit the
// change, the same as in the job list. Running jobs accept only kill.
func (a *API) actions(w http.ResponseWriter, r *http.Request) {
	var request actionRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())

		return
	}

	commands := map[string]string{"requeue": "requeue", "kill": "kill", "discard": "discard"}
	command, known := commands[request.Action]

	if !known {
		writeError(w, http.StatusBadRequest, "action must be requeue, kill, or discard")

		return
	}

	if !slices.Contains([]string{"retries", "scheduled", "dead", "working"}, request.Set) {
		writeError(w, http.StatusBadRequest, "set must be retries, scheduled, dead, or working")

		return
	}

	if request.Set == "working" && command != "kill" {
		writeError(w, http.StatusBadRequest, "running jobs accept only kill")

		return
	}

	if len(request.JIDs) == 0 && request.Group == "" && !request.All {
		writeError(w, http.StatusBadRequest, "name jids, a group, or all")

		return
	}

	match := func(j *job.Job, _ []byte) bool {
		if request.All {
			return true
		}

		if request.Group != "" {
			return storage.Fingerprint(j) == request.Group
		}

		return slices.Contains(request.JIDs, j.JID)
	}

	filter := storage.JobFilter{Queue: request.Queue, JobType: request.JobType, Worker: request.Worker}

	changed, err := a.manager.Apply(storage.Mutation{Command: command, Target: request.Set, Match: match, Filter: filter})
	if err != nil {
		a.fail(w, err)

		return
	}

	writeJSON(w, http.StatusOK, map[string]int{"changed": changed})
}

func (a *API) enqueue(w http.ResponseWriter, r *http.Request) {
	var pushed job.Job

	if err := json.NewDecoder(r.Body).Decode(&pushed); err != nil {
		writeError(w, http.StatusBadRequest, "invalid job: "+err.Error())

		return
	}

	if pushed.JID == "" {
		pushed.JID = "web-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}

	if err := a.manager.Push(&pushed); err != nil {
		a.fail(w, err)

		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{"jid": pushed.JID})
}

func (a *API) workers(w http.ResponseWriter, _ *http.Request) {
	busy, err := a.manager.BusyByWorker()
	if err != nil {
		a.fail(w, err)

		return
	}

	now := time.Now()
	workers := a.manager.Workers()
	views := make([]map[string]any, 0, len(workers))

	for _, worker := range workers {
		views = append(views, workerView(worker, busy[worker.WID], now))
	}

	writeJSON(w, http.StatusOK, map[string]any{"workers": views})
}

func workerView(worker manager.WorkerInfo, busy int, now time.Time) map[string]any {
	state := worker.State
	if state == "" {
		state = "running"
	}

	var wanted, limit any

	if worker.Wanted > 0 {
		wanted = worker.Wanted
	}

	if worker.Limit > 0 {
		limit = worker.Limit
	}

	return map[string]any{
		"wid":               worker.WID,
		"hostname":          worker.Hostname,
		"pid":               worker.PID,
		"labels":            nonNil(worker.Labels),
		"concurrency":       worker.Concurrency,
		"wanted":            wanted,
		"max_concurrency":   limit,
		"scalable":          worker.Scalable,
		"autoscale":         worker.Autoscale,
		"saved":             worker.Saved,
		"tag":               worker.Tag,
		"queues":            nonNil(worker.Queues),
		"started_seconds":   int(now.Sub(worker.Started).Seconds()),
		"busy":              busy,
		"connections":       worker.Connections,
		"state":             state,
		"last_beat_seconds": int(now.Sub(worker.LastBeat).Seconds()),
	}
}

// scaleWorker changes the threads of one worker. The body has concurrency,
// autoscale as {"max": n} or {"min": n, "max": n}, or both. autoscale false
// turns autoscale off. The worker gets the change at its next beat.
func (a *API) scaleWorker(w http.ResponseWriter, r *http.Request) {
	wid := r.PathValue("wid")

	var request struct {
		Concurrency *int            `json:"concurrency"`
		Autoscale   json.RawMessage `json:"autoscale"`
		Reset       bool            `json:"reset"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())

		return
	}

	if request.Reset {
		a.resetWorker(w, wid, request.Concurrency != nil || request.Autoscale != nil)

		return
	}

	if request.Concurrency == nil && request.Autoscale == nil {
		writeError(w, http.StatusBadRequest, "set concurrency, autoscale, both, or reset")

		return
	}

	change := manager.ScaleChange{SetAutoscale: request.Autoscale != nil}

	if request.Concurrency != nil {
		if *request.Concurrency == 0 {
			writeError(w, http.StatusBadRequest, manager.ErrBadConcurrency.Error())

			return
		}

		change.Concurrency = *request.Concurrency
	}

	if change.SetAutoscale {
		scale, err := readAutoscale(request.Autoscale)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())

			return
		}

		change.Autoscale = scale
	}

	if err := a.manager.Scale(wid, change); err != nil {
		scaleError(w, err)

		return
	}

	a.workerReply(w, wid)
}

// resetWorker drops the saved setting of a worker name, and brings back the
// config of the worker. A reset goes alone, so a body cannot ask for a change
// and its undo at once.
func (a *API) resetWorker(w http.ResponseWriter, wid string, mixed bool) {
	if mixed {
		writeError(w, http.StatusBadRequest, "reset goes alone, without concurrency or autoscale")

		return
	}

	if err := a.manager.Scale(wid, manager.ScaleChange{Reset: true}); err != nil {
		scaleError(w, err)

		return
	}

	a.workerReply(w, wid)
}

// readAutoscale reads false or null as off, and an object as on.
func readAutoscale(raw json.RawMessage) (*manager.Autoscale, error) {
	text := strings.TrimSpace(string(raw))

	if text == "false" || text == "null" {
		return nil, nil
	}

	var scale manager.Autoscale

	if err := json.Unmarshal(raw, &scale); err != nil {
		return nil, errors.New(`autoscale must be false, or {"max": n}`)
	}

	return &scale, nil
}

func scaleError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, manager.ErrUnknownWorker):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, manager.ErrNotScalable):
		writeError(w, http.StatusConflict, err.Error())
	default:
		writeError(w, http.StatusBadRequest, err.Error())
	}
}

func (a *API) workerReply(w http.ResponseWriter, wid string) {
	busy, err := a.manager.BusyByWorker()
	if err != nil {
		a.fail(w, err)

		return
	}

	for _, worker := range a.manager.Workers() {
		if worker.WID == wid {
			writeJSON(w, http.StatusOK, workerView(worker, busy[wid], time.Now()))

			return
		}
	}

	writeError(w, http.StatusNotFound, manager.ErrUnknownWorker.Error())
}

func (a *API) schedules(w http.ResponseWriter, _ *http.Request) {
	leases, err := a.manager.LeaseCounts()
	if err != nil {
		a.fail(w, err)

		return
	}

	cronViews := []map[string]any{}

	if a.options.Scheduler != nil {
		for _, entry := range a.options.Scheduler.Upcoming() {
			cronViews = append(cronViews, map[string]any{
				"schedule": entry.Schedule,
				"jobtype":  entry.JobType,
				"queue":    entry.Queue,
				"next":     entry.Next,
			})
		}
	}

	throttleViews := []map[string]any{}

	for queue, throttle := range a.manager.Throttles() {
		throttleViews = append(throttleViews, map[string]any{
			"queue":           queue,
			"concurrency":     throttle.Concurrency,
			"worker":          throttle.Worker,
			"timeout_seconds": int(throttle.Timeout.Seconds()),
			"in_use":          leases[queue],
		})
	}

	slices.SortFunc(throttleViews, func(left, right map[string]any) int {
		return strings.Compare(left["queue"].(string), right["queue"].(string))
	})

	writeJSON(w, http.StatusOK, map[string]any{"cron": cronViews, "throttles": throttleViews})
}

func (a *API) batches(w http.ResponseWriter, _ *http.Request) {
	batches, err := a.manager.Batches(batchesLimit)
	if err != nil {
		a.fail(w, err)

		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"batches": nonNil(batches)})
}

func (a *API) liveRate() map[string]float64 {
	done, failed := a.manager.LiveRate()

	return map[string]float64{"done_per_second": done, "failed_per_second": failed}
}

func (a *API) connections() int {
	if a.options.Connections == nil {
		return 0
	}

	return a.options.Connections()
}

// fail maps store errors to HTTP status codes.
func (a *API) fail(w http.ResponseWriter, err error) {
	var rejected *storage.RejectedError

	switch {
	case errors.As(err, &rejected):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, storage.ErrNotUnique), errors.Is(err, storage.ErrDuplicateJob):
		writeError(w, http.StatusConflict, err.Error())
	case strings.Contains(err.Error(), "invalid"), strings.Contains(err.Error(), "required"), strings.Contains(err.Error(), "must"):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		if a.options.Logger != nil {
			a.options.Logger.Error("web API failed", "error", err)
		}

		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)

	json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func nonNil[T any](values []T) []T {
	if values == nil {
		return []T{}
	}

	return values
}
