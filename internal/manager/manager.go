// Package manager applies the job lifecycle rules.
package manager

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"jobserver/internal/job"
	"jobserver/internal/storage"
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

type Manager struct {
	store storage.Store

	mu      sync.Mutex
	arrival chan struct{}
	workers map[string]time.Time
	state   string
}

func New(store storage.Store) *Manager {
	return &Manager{
		store:   store,
		arrival: make(chan struct{}),
		workers: map[string]time.Time{},
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

// Fetch returns nil when no job arrives in the wait time.
func (m *Manager) Fetch(ctx context.Context, queues []string, wid string) (*job.Job, error) {
	if m.State() != StateRunning {
		return nil, nil
	}

	timer := time.NewTimer(fetchWait)
	defer timer.Stop()

	for {
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
	if err := m.store.Ack(jid); err != nil {
		return err
	}

	m.wake()

	return nil
}

func (m *Manager) Fail(report FailReport) error {
	now := time.Now()

	err := m.store.Fail(report.JID, now, func(j *job.Job) storage.Disposition {
		return decideRetry(j, report, now)
	})
	if err != nil {
		return err
	}

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
	if _, err := m.store.Mutate(mutation, time.Now()); err != nil {
		return err
	}

	m.wake()

	return nil
}

// SetThrottles replaces the limits. A blank map removes all limits.
func (m *Manager) SetThrottles(throttles map[string]storage.Throttle) {
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

func (m *Manager) Beat(wid string) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.workers[wid] = time.Now()

	return m.state
}

func (m *Manager) SetState(state string) {
	m.mu.Lock()
	m.state = state
	m.mu.Unlock()

	m.wake()
}

func (m *Manager) State() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.state
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

	for wid, lastBeat := range m.workers {
		if now.Sub(lastBeat) > workerLifetime {
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
