// Package manager applies the job lifecycle rules.
package manager

import (
	"context"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/thadeu/anway/internal/job"
	"github.com/thadeu/anway/internal/storage"
)

const (
	fetchWait      = 2 * time.Second
	moveLimit      = 1000
	workerLifetime = 60 * time.Second
)

const (
	StateRunning   = ""
	StateQuiet     = "quiet"
	StateTerminate = "terminate"
)

type FailReport struct {
	JID       string   `json:"jid"`
	ErrType   string   `json:"errtype"`
	Message   string   `json:"message"`
	Backtrace []string `json:"backtrace"`
}

// WorkerInfo is what the server knows about one worker process.
type WorkerInfo struct {
	WID         string
	Hostname    string
	PID         int
	Labels      []string
	Concurrency int
	Tag         string
	Queues      []string
	Connections int
	State       string
	Started     time.Time
	LastBeat    time.Time

	// Base is the thread count in HELLO, and Limit the most that the worker
	// can run, or 0 for no limit. Scalable is true once the worker sends its
	// thread count in BEAT. Wanted is the count that the worker must run, or
	// 0 when it runs the right count.
	Base      int
	Limit     int
	Scalable  bool
	Wanted    int
	Autoscale *Autoscale

	// Saved is true when the worker runs on a setting that an operator saved
	// for its name. config is the autoscale from HELLO, which a reset brings
	// back. manual is the last thread count that an operator set.
	Saved  bool
	config *Autoscale
	manual int

	lowSince time.Time
}

type Manager struct {
	store   storage.Store
	metrics *metrics

	mu        sync.Mutex
	arrival   chan struct{}
	workers   map[string]*WorkerInfo
	state     string
	draining  bool
	throttles map[string]storage.Throttle

	flushMu   sync.Mutex
	retention time.Duration
	lastFlush int64
}

func New(store storage.Store) *Manager {
	return &Manager{
		store:   store,
		metrics: newMetrics(),
		arrival: make(chan struct{}),
		workers: map[string]*WorkerInfo{},
	}
}

func (m *Manager) Push(j *job.Job) error {
	now := time.Now()

	if err := j.Normalize(now); err != nil {
		return err
	}

	if err := m.store.Push(j, now); err != nil {
		return err
	}

	m.wake()

	return nil
}

// Fetch returns nil when no job arrives in the wait time, and at once while
// the server drains.
func (m *Manager) Fetch(ctx context.Context, queues []string, wid string) (*job.Job, error) {
	timer := time.NewTimer(fetchWait)
	defer timer.Stop()

	for {
		if !m.givesJobs() {
			return nil, nil
		}

		arrival := m.arrivalSignal()

		fetched, err := m.store.Fetch(queues, wid, time.Now())
		if err != nil || fetched != nil {
			return fetched, err
		}

		select {
		case <-arrival:
		case <-timer.C:
			return nil, nil
		case <-ctx.Done():
			return nil, nil
		}
	}
}

func (m *Manager) Ack(jid string) error {
	queue, err := m.store.Ack(jid)
	if err != nil {
		return err
	}

	m.metrics.add(queue, 1, 0, time.Now())
	m.wake()

	return nil
}

func (m *Manager) Fail(report FailReport) error {
	now := time.Now()

	queue, err := m.store.Fail(report.JID, now, func(j *job.Job) storage.Disposition {
		return decideRetry(j, report, now)
	})
	if err != nil {
		return err
	}

	m.metrics.add(queue, 0, 1, now)
	m.wake()

	return nil
}

func (m *Manager) PauseQueues(names []string) error {
	return m.store.PauseQueues(names)
}

func (m *Manager) ResumeQueues(names []string) error {
	if err := m.store.ResumeQueues(names); err != nil {
		return err
	}

	m.wake()

	return nil
}

func (m *Manager) RemoveQueues(names []string) error {
	return m.store.RemoveQueues(names)
}

func (m *Manager) Mutate(mutation storage.Mutation) error {
	_, err := m.Apply(mutation)

	return err
}

// Apply runs a mutation and returns the number of changed jobs.
func (m *Manager) Apply(mutation storage.Mutation) (int, error) {
	changed, err := m.store.Mutate(mutation, time.Now())
	if err != nil {
		return changed, err
	}

	m.wake()

	return changed, nil
}

func (m *Manager) ClearQueue(name string) (int, error) {
	return m.store.ClearQueue(name)
}

func (m *Manager) QueueDetails() ([]storage.QueueInfo, error) {
	return m.store.QueueDetails()
}

func (m *Manager) ListJobs(set, cursor string, limit int, filter storage.JobFilter) (storage.JobPage, error) {
	return m.store.ListJobs(set, cursor, limit, filter)
}

func (m *Manager) JobGroups(set string, maxScan int) (storage.JobGroups, error) {
	return m.store.JobGroups(set, maxScan)
}

func (m *Manager) BusyByWorker() (map[string]int, error) {
	return m.store.BusyByWorker()
}

func (m *Manager) LeaseCounts() (map[string]int, error) {
	return m.store.LeaseCounts(time.Now())
}

func (m *Manager) Batches(limit int) ([]storage.BatchSummary, error) {
	return m.store.Batches(limit)
}

// Throttles returns the limits in use.
func (m *Manager) Throttles() map[string]storage.Throttle {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.throttles
}

// SetThrottles replaces the limits. A blank map removes all limits.
func (m *Manager) SetThrottles(throttles map[string]storage.Throttle) {
	m.mu.Lock()
	m.throttles = throttles
	m.mu.Unlock()

	m.store.SetThrottles(throttles)
	m.wake()
}

func (m *Manager) BatchNew(definition storage.BatchDefinition) (string, error) {
	return m.store.BatchNew(definition, time.Now())
}

func (m *Manager) BatchOpen(bid string) error {
	return m.store.BatchOpen(bid)
}

func (m *Manager) BatchCommit(bid string) error {
	if err := m.store.BatchCommit(bid, time.Now()); err != nil {
		return err
	}

	m.wake()

	return nil
}

func (m *Manager) BatchStatus(bid string) (storage.BatchStatus, error) {
	return m.store.BatchStatus(bid)
}

func (m *Manager) TrackGet(jid string) (storage.Track, error) {
	return m.store.TrackGet(jid, time.Now())
}

func (m *Manager) TrackSet(update storage.TrackUpdate) error {
	return m.store.TrackSet(update, time.Now())
}

// Tick moves due jobs and expires reservations. The background loop calls it.
func (m *Manager) Tick(now time.Time) error {
	for {
		moved, err := m.store.MoveDue(now, moveLimit)
		if err != nil {
			return err
		}

		if moved > 0 {
			m.wake()
		}

		if moved < moveLimit {
			break
		}
	}

	for {
		expired, err := m.store.ExpireReservations(now, moveLimit, func(j *job.Job) storage.Disposition {
			report := FailReport{ErrType: "ReservationExpired", Message: "the reservation time expired"}

			return decideRetry(j, report, now)
		})
		if err != nil {
			return err
		}

		if expired > 0 {
			m.metrics.add("", 0, uint64(expired), now)
			m.wake()
		}

		if expired < moveLimit {
			break
		}
	}

	for {
		swept, err := m.store.SweepLocks(now, moveLimit)
		if err != nil {
			return err
		}

		if swept < moveLimit {
			break
		}
	}

	for {
		swept, err := m.store.SweepExpired(now, moveLimit)
		if err != nil {
			return err
		}

		if swept < moveLimit {
			break
		}
	}

	m.metrics.sampleThreads(m.connectedThreads(), now)

	if err := m.tickMetrics(now); err != nil {
		return err
	}

	m.reapWorkers(now)

	return nil
}

func (m *Manager) Run(ctx context.Context, onError func(error)) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if err := m.Tick(now); err != nil {
				onError(err)
			}
		}
	}
}

// Register records a worker connection. A process can open many connections
// with one worker id.
func (m *Manager) Register(info WorkerInfo) {
	saved, found := m.loadSetting(workerName(info.Hostname, info.Tag))

	m.mu.Lock()
	defer m.mu.Unlock()

	current := m.workers[info.WID]

	// Each connection of a process sends HELLO. Only the first one sets the
	// threads and autoscale, so that a later connection does not undo a change.
	// A setting that an operator saved for the worker name wins over HELLO.
	if current == nil {
		current = &WorkerInfo{WID: info.WID, Started: time.Now(), Concurrency: info.Concurrency, Base: info.Concurrency, Limit: max(info.Limit, 0)}
		m.workers[info.WID] = current

		current.config = helloAutoscale(info.Autoscale, current.Base, current.Limit)
		current.Autoscale = copyScale(current.config)

		if found {
			applySetting(current, saved)
		}
	}

	current.Hostname = info.Hostname
	current.PID = info.PID
	current.Labels = info.Labels
	current.Tag = info.Tag
	current.Queues = info.Queues
	current.Connections++
	current.LastBeat = time.Now()
}

func (m *Manager) Disconnect(wid string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if current := m.workers[wid]; current != nil && current.Connections > 0 {
		current.Connections--
	}
}

// Beat records liveness. It returns the state that the worker must take, and
// the thread count that it must run, or 0 for no change. A worker reports
// its threads in concurrency, or 0 when it cannot change them.
func (m *Manager) Beat(wid, reportedState string, concurrency int) (string, int) {
	load := m.loadFor(wid)
	now := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()

	current := m.workers[wid]

	if current == nil {
		current = &WorkerInfo{WID: wid, Started: now}
		m.workers[wid] = current
	}

	current.LastBeat = now
	current.State = reportedState

	if concurrency > 0 {
		current.Scalable = true
		current.Concurrency = concurrency
	}

	if !current.Scalable {
		return m.state, 0
	}

	// A beat that reports a count that the server asked for only confirms the
	// change. Autoscale waits for the next beat, so one change is not stacked
	// on top of a change that just happened.
	confirms := current.Wanted > 0 && concurrency == current.Wanted

	if current.Autoscale != nil && load != nil && !confirms {
		autoscale(current, *load, now)
	}

	if current.Wanted == current.Concurrency {
		current.Wanted = 0
	}

	return m.state, current.Wanted
}

// Workers returns a copy of the worker table, sorted by host.
func (m *Manager) Workers() []WorkerInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

	workers := make([]WorkerInfo, 0, len(m.workers))

	for _, worker := range m.workers {
		workers = append(workers, *worker)
	}

	sort.Slice(workers, func(i, j int) bool {
		if workers[i].Hostname != workers[j].Hostname {
			return workers[i].Hostname < workers[j].Hostname
		}

		return workers[i].WID < workers[j].WID
	})

	return workers
}

func (m *Manager) SetState(state string) {
	m.mu.Lock()
	m.state = state
	m.mu.Unlock()

	m.wake()
}

// Drain stops Fetch from giving out jobs. ACK and FAIL still work, so that
// running jobs can report before the server stops. Workers do not see a state
// change, so they fetch again when the server comes back.
func (m *Manager) Drain() {
	m.mu.Lock()
	m.draining = true
	m.mu.Unlock()

	m.wake()
}

func (m *Manager) givesJobs() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.state == StateRunning && !m.draining
}

func (m *Manager) State() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.state
}

// connectedThreads is the sum of threads of the workers that have a
// connection now.
func (m *Manager) connectedThreads() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	var threads uint64

	for _, worker := range m.workers {
		if worker.Connections > 0 {
			threads += uint64(max(worker.Concurrency, 0))
		}
	}

	return threads
}

func (m *Manager) WorkerCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.workers)
}

func (m *Manager) Snapshot(path string) error {
	return m.store.Snapshot(path)
}

func (m *Manager) Stats() (storage.Stats, error) {
	return m.store.Stats()
}

func (m *Manager) Flush() error {
	return m.store.Flush()
}

func (m *Manager) reapWorkers(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for wid, worker := range m.workers {
		if worker.Connections == 0 && now.Sub(worker.LastBeat) > workerLifetime {
			delete(m.workers, wid)
		}
	}
}

func (m *Manager) arrivalSignal() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.arrival
}

func (m *Manager) wake() {
	m.mu.Lock()
	defer m.mu.Unlock()

	close(m.arrival)
	m.arrival = make(chan struct{})
}

func decideRetry(j *job.Job, report FailReport, now time.Time) storage.Disposition {
	failure := &job.Failure{
		FailedAt: now.UTC().Format(time.RFC3339Nano),
		Message:  report.Message,
		ErrType:  report.ErrType,
	}

	if j.Failure != nil {
		failure.RetryCount = j.Failure.RetryCount + 1
	}

	if lines := min(j.Backtrace, len(report.Backtrace)); lines > 0 {
		failure.Backtrace = report.Backtrace[:lines]
	}

	j.Failure = failure

	retry := job.DefaultRetry
	if j.Retry != nil {
		retry = *j.Retry
	}

	if retry < 0 {
		return storage.Disposition{Outcome: storage.Dead}
	}

	if retry == 0 {
		return storage.Disposition{Outcome: storage.Discard}
	}

	if failure.RetryCount >= retry {
		return storage.Disposition{Outcome: storage.Dead}
	}

	at := now.Add(RetryDelay(failure.RetryCount))
	failure.NextAt = at.UTC().Format(time.RFC3339Nano)

	return storage.Disposition{Outcome: storage.Retry, At: at}
}

func RetryDelay(count int) time.Duration {
	seconds := count*count*count*count + 15 + rand.IntN(31)*(count+1)

	return time.Duration(seconds) * time.Second
}
