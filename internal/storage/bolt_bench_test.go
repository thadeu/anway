package storage

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thadeu/anway/internal/job"
)

func BenchmarkDrain(b *testing.B) {
	store, err := OpenBolt(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	now := time.Now()
	total := max(b.N, 20000)

	for start := 0; start < total; start += 32 {
		var group sync.WaitGroup

		for i := start; i < min(start+32, total); i++ {
			group.Add(1)

			go func() {
				defer group.Done()

				pushed := &job.Job{JID: fmt.Sprintf("1793369529359000000-%d", i), Type: "load", Args: json.RawMessage(`[1,"payload"]`)}

				if err := pushed.Normalize(now); err != nil {
					b.Error(err)
				}

				if err := store.Push(pushed, now); err != nil {
					b.Error(err)
				}
			}()
		}

		group.Wait()
	}

	b.ResetTimer()

	var done atomic.Int64
	var group sync.WaitGroup

	for range 32 {
		group.Add(1)

		go func() {
			defer group.Done()

			for done.Add(1) <= int64(total) {
				fetched, err := store.Fetch([]string{job.DefaultQueue}, "bench", time.Now())
				if err != nil || fetched == nil {
					b.Error("fetch failed", err)

					return
				}

				if err := store.Ack(fetched.JID); err != nil {
					b.Error(err)

					return
				}
			}
		}()
	}

	group.Wait()
	b.ReportMetric(float64(total)/b.Elapsed().Seconds(), "jobs/s")
}
