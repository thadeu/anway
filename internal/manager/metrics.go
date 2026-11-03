package manager

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/thadeu/anway/internal/storage"
)

const (
	minutesKept     = 1440
	secondsKept     = 60
	queueRateWindow = 5
	liveRateWindow  = 5
	pruneLimit      = 1000
)

type bucket struct {
	minute  int64
	done    uint64
	failed  uint64
	threads uint64
	queues  map[string]uint64
}

type secondBucket struct {
	second int64
	done   uint64
	failed uint64
}

// Point is one step of a throughput series. Done and Failed add up over the
// step. Threads is the peak of worker threads in the step, because a sum of
// thread counts has no meaning.
type Point struct {
	Time    time.Time `json:"time"`
	Done    uint64    `json:"done"`
	Failed  uint64    `json:"failed"`
	Threads uint64    `json:"threads"`
}

// metrics keeps the last 24 hours by minute and the last minute by second in
// memory. Changed minutes are saved to storage once a minute.
type metrics struct {
	mu      sync.Mutex
	total   [minutesKept]bucket
	seconds [secondsKept]secondBucket
	dirty   map[int64]bool
}

func newMetrics() *metrics {
	return &metrics{dirty: map[int64]bool{}}
}

func (m *metrics) add(queue string, done, failed uint64, now time.Time) {
	minute := now.Unix() / 60
	second := now.Unix()

	m.mu.Lock()
	defer m.mu.Unlock()

	slot := &m.total[minute%minutesKept]

	if slot.minute != minute {
		*slot = bucket{minute: minute}
	}

	slot.done += done
	slot.failed += failed

	if queue != "" && done > 0 {
		if slot.queues == nil {
			slot.queues = map[string]uint64{}
		}

		slot.queues[queue] += done
	}

	m.dirty[minute] = true

	live := &m.seconds[second%secondsKept]

	if live.second != second {
		*live = secondBucket{second: second}
	}

	live.done += done
	live.failed += failed
}

// sampleThreads keeps the peak of worker threads in the current minute.
func (m *metrics) sampleThreads(threads uint64, now time.Time) {
	minute := now.Unix() / 60

	m.mu.Lock()
	defer m.mu.Unlock()

	slot := &m.total[minute%minutesKept]

	if slot.minute != minute {
		*slot = bucket{minute: minute}
	}

	if threads > slot.threads {
		slot.threads = threads
		m.dirty[minute] = true
	}
}

// series returns the last minutes, grouped in steps. The last point is the
// current minute, which is not complete.
func (m *metrics) series(minutes, step int, now time.Time) []Point {
	current := now.Unix() / 60

	m.mu.Lock()
	defer m.mu.Unlock()

	var points []Point

	for start := current - int64(minutes) + 1; start <= current; start += int64(step) {
		point := Point{Time: time.Unix(start*60, 0).UTC()}

		for minute := start; minute < start+int64(step) && minute <= current; minute++ {
			slot := m.total[minute%minutesKept]

			if slot.minute == minute {
				point.Done += slot.done
				point.Failed += slot.failed
				point.Threads = max(point.Threads, slot.threads)
			}
		}

		points = append(points, point)
	}

	return points
}

func (m *metrics) secondSeries(now time.Time) []Point {
	current := now.Unix()

	m.mu.Lock()
	defer m.mu.Unlock()

	points := make([]Point, 0, secondsKept)

	for second := current - secondsKept + 1; second <= current; second++ {
		point := Point{Time: time.Unix(second, 0).UTC()}
		slot := m.seconds[second%secondsKept]

		if slot.second == second {
			point.Done = slot.done
			point.Failed = slot.failed
		}

		points = append(points, point)
	}

	return points
}

// liveRate is the average per second over the last complete seconds.
func (m *metrics) liveRate(now time.Time) (float64, float64) {
	current := now.Unix()

	m.mu.Lock()
	defer m.mu.Unlock()

	var done, failed uint64

	for second := current - liveRateWindow; second < current; second++ {
		slot := m.seconds[second%secondsKept]

		if slot.second == second {
			done += slot.done
			failed += slot.failed
		}
	}

	return float64(done) / liveRateWindow, float64(failed) / liveRateWindow
}

// queueRate is the average of done jobs per minute in the last complete minutes.
func (m *metrics) queueRate(queue string, now time.Time) float64 {
	current := now.Unix() / 60

	m.mu.Lock()
	defer m.mu.Unlock()

	var total uint64

	for minute := current - queueRateWindow; minute < current; minute++ {
		slot := m.total[minute%minutesKept]

		if slot.minute == minute {
			total += slot.queues[queue]
		}
	}

	return float64(total) / queueRateWindow
}

// takeDirty returns copies of the changed minutes and clears the marks.
func (m *metrics) takeDirty() []storage.MetricMinute {
	m.mu.Lock()
	defer m.mu.Unlock()

	minutes := make([]storage.MetricMinute, 0, len(m.dirty))

	for minute := range m.dirty {
		slot := m.total[minute%minutesKept]

		if slot.minute == minute {
			queues := make(map[string]uint64, len(slot.queues))

			for name, done := range slot.queues {
				queues[name] = done
			}

			minutes = append(minutes, storage.MetricMinute{Minute: minute, Done: slot.done, Failed: slot.failed, Threads: slot.threads, Queues: queues})
		}

		delete(m.dirty, minute)
	}

	sort.Slice(minutes, func(i, j int) bool { return minutes[i].Minute < minutes[j].Minute })

	return minutes
}

func (m *metrics) markDirty(minutes []storage.MetricMinute) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, minute := range minutes {
		m.dirty[minute.Minute] = true
	}
}

// restore fills memory with saved minutes of the last 24 hours.
func (m *metrics) restore(minutes []storage.MetricMinute) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, saved := range minutes {
		m.total[saved.Minute%minutesKept] = bucket{minute: saved.Minute, done: saved.Done, failed: saved.Failed, threads: saved.Threads, queues: saved.Queues}
	}
}

type rangeShape struct {
	minutes int
	step    int
	disk    bool
}

// ranges maps a range name to its minutes and step. Ranges over 24 hours read
// the saved minutes.
var ranges = map[string]rangeShape{
	"15m": {15, 1, false},
	"1h":  {60, 1, false},
	"24h": {1440, 10, false},
	"7d":  {7 * 1440, 60, true},
	"30d": {30 * 1440, 240, true},
	"60d": {60 * 1440, 480, true},
}

// Series returns a throughput series. "60s" returns the last minute by second.
func (m *Manager) Series(rangeName string) ([]Point, error) {
	now := time.Now()

	if rangeName == "60s" {
		return m.metrics.secondSeries(now), nil
	}

	shape, ok := ranges[rangeName]
	if !ok {
		return nil, fmt.Errorf("unknown range %q", rangeName)
	}

	if !shape.disk {
		return m.metrics.series(shape.minutes, shape.step, now), nil
	}

	return m.diskSeries(shape, now)
}

func (m *Manager) diskSeries(shape rangeShape, now time.Time) ([]Point, error) {
	current := now.Unix() / 60
	first := current - int64(shape.minutes) + 1

	saved, err := m.store.LoadMetrics(first, current)
	if err != nil {
		return nil, err
	}

	var points []Point

	index := 0

	for start := first; start <= current; start += int64(shape.step) {
		point := Point{Time: time.Unix(start*60, 0).UTC()}
		end := start + int64(shape.step)

		for index < len(saved) && saved[index].Minute < end {
			point.Done += saved[index].Done
			point.Failed += saved[index].Failed
			point.Threads = max(point.Threads, saved[index].Threads)
			index++
		}

		points = append(points, point)
	}

	return points, nil
}

// windowPoints is the most points that Window returns. It picks the smallest
// step in windowSteps that stays under it.
const windowPoints = 120

var windowSteps = []int64{1, 5, 10, 15, 30, 60, 120, 240, 480, 720, 1440}

// Window returns the throughput from `from` to `until`, by minute or by a
// longer step. Minutes of the last 24 hours come from memory, and older ones
// from the saved minutes. A window shorter than two minutes grows to two.
func (m *Manager) Window(from, until time.Time) ([]Point, error) {
	now := time.Now()
	current := now.Unix() / 60
	first := from.Unix() / 60
	last := min(until.Unix()/60, current)

	if last-first < 1 {
		first = last - 1
	}

	step := windowSteps[len(windowSteps)-1]

	for _, candidate := range windowSteps {
		if (last-first+1)/candidate <= windowPoints {
			step = candidate

			break
		}
	}

	byMinute := map[int64]storage.MetricMinute{}
	memoryFirst := current - minutesKept + 1

	if first < memoryFirst {
		saved, err := m.store.LoadMetrics(first, min(last, memoryFirst-1))
		if err != nil {
			return nil, err
		}

		for _, minute := range saved {
			byMinute[minute.Minute] = minute
		}
	}

	m.metrics.mu.Lock()

	for minute := max(first, memoryFirst); minute <= last; minute++ {
		slot := m.metrics.total[minute%minutesKept]

		if slot.minute == minute {
			byMinute[minute] = storage.MetricMinute{Minute: minute, Done: slot.done, Failed: slot.failed, Threads: slot.threads}
		}
	}

	m.metrics.mu.Unlock()

	var points []Point

	for start := first; start <= last; start += step {
		point := Point{Time: time.Unix(start*60, 0).UTC()}

		for minute := start; minute < start+step && minute <= last; minute++ {
			point.Done += byMinute[minute].Done
			point.Failed += byMinute[minute].Failed
			point.Threads = max(point.Threads, byMinute[minute].Threads)
		}

		points = append(points, point)
	}

	return points, nil
}

func (m *Manager) QueueRate(queue string) float64 {
	return m.metrics.queueRate(queue, time.Now())
}

// LiveRate returns done and failed jobs per second, from the last seconds.
func (m *Manager) LiveRate() (float64, float64) {
	return m.metrics.liveRate(time.Now())
}

// RestoreMetrics loads the saved minutes of the last 24 hours and sets the
// retention for saved minutes.
func (m *Manager) RestoreMetrics(retention time.Duration) error {
	now := time.Now().Unix() / 60

	saved, err := m.store.LoadMetrics(now-minutesKept+1, now)
	if err != nil {
		return err
	}

	m.metrics.restore(saved)

	m.flushMu.Lock()
	m.retention = retention
	m.lastFlush = now
	m.flushMu.Unlock()

	return nil
}

// SaveMetrics writes the changed minutes. The server calls it once a minute
// and at shutdown.
func (m *Manager) SaveMetrics() error {
	m.flushMu.Lock()
	defer m.flushMu.Unlock()

	minutes := m.metrics.takeDirty()
	if len(minutes) == 0 {
		return nil
	}

	if err := m.store.SaveMetrics(minutes); err != nil {
		m.metrics.markDirty(minutes)

		return err
	}

	return nil
}

// tickMetrics saves and prunes once each minute.
func (m *Manager) tickMetrics(now time.Time) error {
	minute := now.Unix() / 60

	m.flushMu.Lock()
	due := minute != m.lastFlush
	retention := m.retention
	m.lastFlush = minute
	m.flushMu.Unlock()

	if !due {
		return nil
	}

	if err := m.SaveMetrics(); err != nil {
		return err
	}

	if retention <= 0 {
		return nil
	}

	before := minute - int64(retention/time.Minute)

	_, err := m.store.PruneMetrics(before, pruneLimit)

	return err
}
