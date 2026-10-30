// Command loadtest measures the server through its TCP protocol.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thadeu/anway/internal/testclient"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7419", "server address")
	password := flag.String("password", "", "server password")
	total := flag.Int("jobs", 100000, "number of jobs")
	connections := flag.Int("connections", 32, "connections for each side")
	mode := flag.String("mode", "both", "push, drain, or both")
	flag.Parse()

	if *mode == "push" || *mode == "both" {
		measure("push", *total, func() { push(*addr, *password, *total, *connections) })
	}

	if *mode == "drain" || *mode == "both" {
		measure("fetch+ack", *total, func() { drain(*addr, *password, *total, *connections) })
	}
}

func measure(name string, total int, work func()) {
	start := time.Now()
	work()
	elapsed := time.Since(start)

	fmt.Printf("%s: %d jobs in %s (%.0f jobs/s)\n", name, total, elapsed.Round(time.Millisecond), float64(total)/elapsed.Seconds())
}

func push(addr, password string, total, connections int) {
	var next atomic.Int64

	run := time.Now().UnixNano()

	parallel(connections, func(int) {
		client, err := testclient.Dial(addr, "", password)
		if err != nil {
			log.Fatal(err)
		}
		defer client.Close()

		for {
			number := next.Add(1)
			if number > int64(total) {
				return
			}

			payload := map[string]any{
				"jid":     fmt.Sprintf("%d-%d", run, number),
				"jobtype": "load",
				"args":    []any{number, "payload"},
			}

			if _, err := client.Command("PUSH", payload); err != nil {
				log.Fatal(err)
			}
		}
	})
}

func drain(addr, password string, total, connections int) {
	var done atomic.Int64

	parallel(connections, func(worker int) {
		client, err := testclient.Dial(addr, fmt.Sprintf("load-%d", worker), password)
		if err != nil {
			log.Fatal(err)
		}
		defer client.Close()

		for done.Load() < int64(total) {
			reply, err := client.Command("FETCH", "default")
			if err != nil {
				log.Fatal(err)
			}

			if reply == "" {
				continue
			}

			var fetched struct {
				JID string `json:"jid"`
			}

			if err := json.Unmarshal([]byte(reply), &fetched); err != nil {
				log.Fatal(err)
			}

			if _, err := client.Command("ACK", map[string]string{"jid": fetched.JID}); err != nil {
				log.Fatal(err)
			}

			done.Add(1)
		}
	})
}

func parallel(count int, work func(int)) {
	var group sync.WaitGroup

	for i := range count {
		group.Add(1)
		time.Sleep(time.Millisecond)

		go func() {
			defer group.Done()

			work(i)
		}()
	}

	group.Wait()
}
