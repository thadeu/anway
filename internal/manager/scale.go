package manager

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/thadeu/anway/internal/storage"
)

// The server holds the thread count that each worker must run. A worker that
// sends its thread count in BEAT is scalable: when the wanted count differs,
// the BEAT reply carries it, and the worker starts or stops threads. With
// autoscale on, each beat moves the wanted count between min and max.

// MaxConcurrency is the highest thread count that the server asks for.
const MaxConcurrency = 1000

// scaleDownAfter is how long a worker must stay at half load or less, with
// no waiting jobs in its queues, before autoscale removes threads.
const scaleDownAfter = 60 * time.Second

var (
	ErrUnknownWorker  = errors.New("unknown worker")
	ErrNotScalable    = errors.New("this worker does not report its threads, so it cannot change them")
	ErrBadConcurrency = fmt.Errorf("concurrency must be 1 to %d", MaxConcurrency)
)

// limitError tells that a worker cannot run more threads, for example
// because its database pool is that size.
func limitError(limit int) error {
	return fmt.Errorf("this worker can run at most %d threads. Its limit comes from the worker, such as the size of its database pool", limit)
}

// Autoscale keeps the threads of a worker between Min and Max.
type Autoscale struct {
	Min int `json:"min"`
	Max int `json:"max"`
}

// workerLoad is what autoscale reads: the waiting jobs in the queues of the
// worker, and the jobs that the worker runs now.
type workerLoad struct {
	backlog uint64
	busy    int
}

// ScaleChange is one change to the threads of a worker. Concurrency 0 keeps
// the count. SetAutoscale with a nil Autoscale turns autoscale off. Reset
// drops the saved setting and goes back to the config of the worker.
type ScaleChange struct {
	Concurrency  int
	SetAutoscale bool
	Autoscale    *Autoscale
	Reset        bool
}

// SetConcurrency asks a worker to run n threads. With autoscale on, n must be
// between its min and max.
func (m *Manager) SetConcurrency(wid string, n int) error {
	if n < 1 {
		return ErrBadConcurrency
	}

	return m.Scale(wid, ScaleChange{Concurrency: n})
}

// SetAutoscale turns autoscale on for a worker, or off with nil. A zero Min
// is the thread count that the worker started with.
func (m *Manager) SetAutoscale(wid string, scale *Autoscale) error {
	return m.Scale(wid, ScaleChange{SetAutoscale: true, Autoscale: scale})
}

// Scale applies a change to a running worker, and saves it for the worker
// name, so a worker with that name starts with it after a restart. A worker
// with no hostname has no name, so its change lasts only until it stops.
func (m *Manager) Scale(wid string, change ScaleChange) error {
	name, setting, err := m.applyScale(wid, change)
	if err != nil {
		return err
	}

	if name == "" {
		return nil
	}

	if change.Reset {
		return m.store.DeleteWorkerSetting(name)
	}

	return m.store.SaveWorkerSetting(name, setting)
}

// applyScale checks the whole change first, so a refused change has no
// effect. It returns the worker name and the setting to save.
func (m *Manager) applyScale(wid string, change ScaleChange) (string, storage.WorkerSetting, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	current := m.workers[wid]

	if current == nil {
		return "", storage.WorkerSetting{}, ErrUnknownWorker
	}

	if !current.Scalable {
		return "", storage.WorkerSetting{}, ErrNotScalable
	}

	name := workerName(current.Hostname, current.Tag)

	if change.Reset {
		resetScale(current)

		return name, storage.WorkerSetting{}, nil
	}

	scale := current.Autoscale

	if change.SetAutoscale {
		scale = nil

		if change.Autoscale != nil {
			checked, err := checkAutoscale(*change.Autoscale, current.Base, current.Limit)
			if err != nil {
				return "", storage.WorkerSetting{}, err
			}

			scale = &checked
		}
	}

	if n := change.Concurrency; n != 0 {
		if err := checkCount(n, current.Limit, scale); err != nil {
			return "", storage.WorkerSetting{}, err
		}

		current.Wanted = n
		current.manual = n
	}

	current.Autoscale = scale
	current.Saved = name != ""
	current.lowSince = time.Time{}

	return name, savedSetting(current), nil
}

func checkCount(n, limit int, scale *Autoscale) error {
	if n < 1 || n > MaxConcurrency {
		return ErrBadConcurrency
	}

	if limit > 0 && n > limit {
		return limitError(limit)
	}

	if scale != nil && (n < scale.Min || n > scale.Max) {
		return fmt.Errorf("autoscale is on, so concurrency must be %d to %d", scale.Min, scale.Max)
	}

	return nil
}

// resetScale brings back the config of the worker: the autoscale from HELLO,
// and the thread count that it started with.
func resetScale(current *WorkerInfo) {
	current.Autoscale = copyScale(current.config)
	current.Saved = false
	current.manual = 0
	current.lowSince = time.Time{}
	current.Wanted = 0

	if current.Concurrency != current.Base {
		current.Wanted = current.Base
	}
}

// savedSetting is what a restart must bring back: autoscale when it is on,
// or else the last thread count that an operator set.
func savedSetting(current *WorkerInfo) storage.WorkerSetting {
	if current.Autoscale != nil {
		return storage.WorkerSetting{Autoscale: &storage.ScaleRange{Min: current.Autoscale.Min, Max: current.Autoscale.Max}}
	}

	return storage.WorkerSetting{Concurrency: current.manual}
}

// applySetting puts a saved setting on a new worker. A limit that is lower
// now, such as a smaller database pool, lowers the saved counts.
func applySetting(current *WorkerInfo, saved storage.WorkerSetting) {
	current.Saved = true
	current.Autoscale = nil

	if saved.Autoscale != nil {
		scale := Autoscale{Min: saved.Autoscale.Min, Max: saved.Autoscale.Max}

		if current.Limit > 0 {
			scale.Max = min(scale.Max, current.Limit)
			scale.Min = min(scale.Min, scale.Max)
		}

		if checked, err := checkAutoscale(scale, current.Base, current.Limit); err == nil {
			current.Autoscale = &checked
		}

		return
	}

	if saved.Concurrency > 0 {
		current.manual = saved.Concurrency
		current.Wanted = saved.Concurrency

		if current.Limit > 0 {
			current.Wanted = min(current.Wanted, current.Limit)
		}
	}
}

// helloAutoscale checks the autoscale that a worker sends in HELLO. A max
// above the limit of the worker goes down to the limit.
func helloAutoscale(hello *Autoscale, base, limit int) *Autoscale {
	if hello == nil {
		return nil
	}

	scale := *hello

	if limit > 0 {
		scale.Max = min(scale.Max, limit)
	}

	checked, err := checkAutoscale(scale, base, limit)
	if err != nil {
		return nil
	}

	return &checked
}

func copyScale(scale *Autoscale) *Autoscale {
	if scale == nil {
		return nil
	}

	copied := *scale

	return &copied
}

// workerName is the name that a saved setting belongs to: the hostname and
// the tag. It stays the same when the process restarts, and the wid does not.
func workerName(hostname, tag string) string {
	if hostname == "" {
		return ""
	}

	return hostname + "\x00" + tag
}

// loadSetting reads the saved setting of a worker name. A read error counts
// as no setting, so a worker can always connect.
func (m *Manager) loadSetting(name string) (storage.WorkerSetting, bool) {
	if name == "" {
		return storage.WorkerSetting{}, false
	}

	setting, found, err := m.store.LoadWorkerSetting(name)
	if err != nil {
		return storage.WorkerSetting{}, false
	}

	return setting, found
}

func checkAutoscale(scale Autoscale, base, limit int) (Autoscale, error) {
	if scale.Min == 0 {
		scale.Min = max(base, 1)
	}

	if scale.Max < 1 || scale.Max > MaxConcurrency {
		return scale, fmt.Errorf("autoscale max must be 1 to %d", MaxConcurrency)
	}

	if limit > 0 && scale.Max > limit {
		return scale, limitError(limit)
	}

	if scale.Min < 1 || scale.Min > scale.Max {
		return scale, fmt.Errorf("autoscale min must be 1 to max (%d), not %d", scale.Max, scale.Min)
	}

	return scale, nil
}

// loadFor reads the load of a worker with autoscale on. It returns nil when
// autoscale is off, or when the store cannot answer.
func (m *Manager) loadFor(wid string) *workerLoad {
	m.mu.Lock()

	current := m.workers[wid]

	if current == nil || current.Autoscale == nil {
		m.mu.Unlock()

		return nil
	}

	queues := append([]string(nil), current.Queues...)

	m.mu.Unlock()

	stats, err := m.store.Stats()
	if err != nil {
		return nil
	}

	busy, err := m.store.BusyByWorker()
	if err != nil {
		return nil
	}

	load := &workerLoad{busy: busy[wid]}

	for _, queue := range queues {
		load.backlog += stats.Queues[queue]
	}

	return load
}

// burstBacklog is how many waiting jobs for each thread count as a burst.
// In a burst, a step doubles the threads and adds burstExtra more.
const (
	burstBacklog = 2
	burstExtra   = 0.15
)

// autoscale sets the wanted count of a worker from its load. When jobs wait
// and all threads are busy, it adds a quarter of the threads. In a burst, with
// at least burstBacklog waiting jobs for each thread, it doubles the threads
// and adds 15% more, so a large backlog reaches max in a few beats. It removes
// a quarter after scaleDownAfter at half load or less with no waiting jobs.
func autoscale(current *WorkerInfo, load workerLoad, now time.Time) {
	scale := current.Autoscale

	threads := current.Concurrency

	if current.Wanted > 0 {
		threads = current.Wanted
	}

	step := max(1, (threads+3)/4)
	growth := step

	if load.backlog >= uint64(threads*burstBacklog) {
		growth = threads + int(math.Ceil(float64(threads)*burstExtra))
	}

	switch {
	case threads > scale.Max:
		current.Wanted = scale.Max
	case threads < scale.Min:
		current.Wanted = scale.Min
	case load.backlog > 0 && load.busy >= threads:
		current.Wanted = min(scale.Max, threads+growth)
		current.lowSince = time.Time{}
	case load.backlog == 0 && load.busy*2 <= threads:
		if current.lowSince.IsZero() {
			current.lowSince = now

			return
		}

		if now.Sub(current.lowSince) >= scaleDownAfter {
			current.Wanted = max(scale.Min, threads-step)
			current.lowSince = now
		}
	default:
		current.lowSince = time.Time{}
	}
}
