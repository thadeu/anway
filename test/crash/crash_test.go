// Package crash stops a real server with SIGKILL under load.
package crash

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"jobserver/internal/job"
	"jobserver/internal/storage"
	"jobserver/internal/testclient"
)

func startServer(t *testing.T, binary, dir, addr string) *exec.Cmd {
	t.Helper()

	cmd := exec.Command(binary)
	cmd.Env = append(os.Environ(), "BIND_ADDR="+addr, "DATA_DIR="+dir)

	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})

	for range 100 {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()

			return cmd
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatal("server did not start")

	return nil
}

func freeAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	return listener.Addr().String()
}

// TestConfirmedChangesSurviveKill checks both directions: a confirmed PUSH
// must exist after restart, and a confirmed ACK must not run again.
func TestConfirmedChangesSurviveKill(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "server")

	build := exec.Command("go", "build", "-o", binary, "jobserver/cmd/server")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %s", output)
	}

	dir := t.TempDir()
	addr := freeAddr(t)

	for round := range 5 {
		server := startServer(t, binary, dir, addr)
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)

		var mu sync.Mutex

		pushed := map[string]bool{}
		acked := map[string]bool{}
		ackSent := map[string]bool{}

		var group sync.WaitGroup

		for producer := range 8 {
			group.Add(1)

			go func() {
				defer group.Done()

				client, err := testclient.Dial(addr, "", "")
				if err != nil {
					return
				}
				defer client.Close()

				for n := 0; ctx.Err() == nil; n++ {
					jid := fmt.Sprintf("%d-%d-%d", round, producer, n)
					payload := map[string]any{"jid": jid, "jobtype": "crash", "args": []any{}}

					if _, err := client.Command("PUSH", payload); err != nil {
						return
					}

					mu.Lock()
					pushed[jid] = true
					mu.Unlock()
				}
			}()
		}

		for worker := range 8 {
			group.Add(1)

			go func() {
				defer group.Done()

				client, err := testclient.Dial(addr, fmt.Sprintf("r%d-w%d", round, worker), "")
				if err != nil {
					return
				}
				defer client.Close()

				for ctx.Err() == nil {
					reply, err := client.Command("FETCH", "default")
					if err != nil {
						return
					}

					if reply == "" {
						continue
					}

					var fetched struct {
						JID string `json:"jid"`
					}

					json.Unmarshal([]byte(reply), &fetched)

					mu.Lock()
					ackSent[fetched.JID] = true
					mu.Unlock()

					if _, err := client.Command("ACK", map[string]string{"jid": fetched.JID}); err != nil {
						return
					}

					mu.Lock()
					acked[fetched.JID] = true
					mu.Unlock()
				}
			}()
		}

		<-ctx.Done()
		cancel()
		server.Process.Kill()
		server.Wait()
		group.Wait()

		releaseReservations(t, dir)

		server = startServer(t, binary, dir, addr)
		survivors := drain(t, addr)

		for jid := range pushed {
			if !ackSent[jid] && !survivors[jid] {
				t.Fatalf("round %d: confirmed job %s is lost", round, jid)
			}
		}

		for jid := range acked {
			if survivors[jid] {
				t.Fatalf("round %d: acknowledged job %s came back", round, jid)
			}
		}

		t.Logf("round %d: pushed=%d acked=%d survivors=%d", round, len(pushed), len(acked), len(survivors))

		server.Process.Kill()
		server.Wait()
	}
}

// releaseReservations reads the killed data file and returns reserved jobs to
// their queues, so that the check sees all jobs that the file still holds.
func releaseReservations(t *testing.T, dir string) {
	t.Helper()

	store, err := storage.OpenBolt(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Now()
	future := now.Add(24 * time.Hour)

	_, err = store.ExpireReservations(future, 1<<30, func(*job.Job) storage.Disposition {
		return storage.Disposition{Outcome: storage.Retry, At: now}
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := store.MoveDue(future, 1<<30); err != nil {
		t.Fatal(err)
	}
}

// drain returns the jobs that remain in the queue.
func drain(t *testing.T, addr string) map[string]bool {
	t.Helper()

	worker, err := testclient.Dial(addr, "drain", "")
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()

	survivors := map[string]bool{}

	for {
		reply, err := worker.Command("FETCH", "default")
		if err != nil {
			t.Fatal(err)
		}

		if reply == "" {
			break
		}

		var fetched struct {
			JID string `json:"jid"`
		}

		if err := json.Unmarshal([]byte(reply), &fetched); err != nil {
			t.Fatal(err)
		}

		survivors[fetched.JID] = true

		if _, err := worker.Command("ACK", map[string]string{"jid": fetched.JID}); err != nil {
			t.Fatal(err)
		}
	}

	return survivors
}

// TestBatchSurvivesKill stops the server between jobs of a committed batch.
func TestBatchSurvivesKill(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "server")

	build := exec.Command("go", "build", "-o", binary, "jobserver/cmd/server")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %s", output)
	}

	dir := t.TempDir()
	addr := freeAddr(t)
	server := startServer(t, binary, dir, addr)

	client, err := testclient.Dial(addr, "w", "")
	if err != nil {
		t.Fatal(err)
	}

	bid, err := client.Command("BATCH", `NEW {"success":{"jobtype":"done","args":[]}}`)
	if err != nil {
		t.Fatal(err)
	}

	for _, jid := range []string{"a", "b"} {
		payload := map[string]any{"jid": jid, "jobtype": "x", "args": []any{}, "custom": map[string]any{"bid": bid}}

		if _, err := client.Command("PUSH", payload); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := client.Command("BATCH", "COMMIT "+bid); err != nil {
		t.Fatal(err)
	}

	finish := func(c *testclient.Client) string {
		reply, err := c.Command("FETCH", "default")
		if err != nil {
			t.Fatal(err)
		}

		var fetched struct {
			JID  string `json:"jid"`
			Type string `json:"jobtype"`
		}

		if err := json.Unmarshal([]byte(reply), &fetched); err != nil {
			t.Fatal(err)
		}

		if _, err := c.Command("ACK", map[string]string{"jid": fetched.JID}); err != nil {
			t.Fatal(err)
		}

		return fetched.Type
	}

	finish(client)
	client.Close()

	server.Process.Kill()
	server.Wait()

	server = startServer(t, binary, dir, addr)

	client, err = testclient.Dial(addr, "w", "")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	finish(client)

	if kind := finish(client); kind != "done" {
		t.Fatalf("unexpected job: %s", kind)
	}

	server.Process.Kill()
}
