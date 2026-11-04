// Package crash stops a real server with SIGKILL under load.
package crash

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/thadeu/anway/internal/job"
	"github.com/thadeu/anway/internal/storage"
	"github.com/thadeu/anway/internal/testclient"
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

	build := exec.Command("go", "build", "-o", binary, "github.com/thadeu/anway/cmd/server")
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

// TestBackupRestores restores a snapshot that a running server wrote.
func TestBackupRestores(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "server")

	build := exec.Command("go", "build", "-o", binary, "github.com/thadeu/anway/cmd/server")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %s", output)
	}

	dir := t.TempDir()
	addr := freeAddr(t)
	server := startServer(t, binary, dir, addr)

	client, err := testclient.Dial(addr, "", "")
	if err != nil {
		t.Fatal(err)
	}

	payload := map[string]any{"jid": "saved", "jobtype": "x", "args": []any{}}

	if _, err := client.Command("PUSH", payload); err != nil {
		t.Fatal(err)
	}

	if err := server.Process.Signal(syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}

	var backups []string

	for range 100 {
		backups, _ = filepath.Glob(filepath.Join(dir, "backups", "*.db"))
		if len(backups) == 1 {
			break
		}

		time.Sleep(50 * time.Millisecond)
	}

	if len(backups) != 1 {
		t.Fatalf("unexpected backups: %v", backups)
	}

	late := map[string]any{"jid": "late", "jobtype": "x", "args": []any{}}

	if _, err := client.Command("PUSH", late); err != nil {
		t.Fatal(err)
	}

	client.Close()
	server.Process.Kill()
	server.Wait()

	restored := t.TempDir()

	raw, err := os.ReadFile(backups[0])
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(restored, "jobs.db"), raw, 0o600); err != nil {
		t.Fatal(err)
	}

	startServer(t, binary, restored, addr)

	if survivors := drain(t, addr); len(survivors) != 1 || !survivors["saved"] {
		t.Fatalf("unexpected jobs: %v", survivors)
	}
}

// TestShutdownUnderLoadIsClean checks that SIGTERM ends with exit code 0.
func TestShutdownUnderLoadIsClean(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "server")

	build := exec.Command("go", "build", "-o", binary, "github.com/thadeu/anway/cmd/server")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %s", output)
	}

	conf := t.TempDir()
	settings := "cron:\n  - schedule: \"* * * * *\"\n    job:\n      type: x\n"

	if err := os.WriteFile(filepath.Join(conf, "c.yml"), []byte(settings), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("CONF_DIR", conf)
	t.Setenv("SHUTDOWN_TIMEOUT", "200ms")

	addr := freeAddr(t)
	server := startServer(t, binary, t.TempDir(), addr)

	if err := server.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}

	if err := server.Wait(); err != nil {
		t.Fatalf("unclean shutdown: %v", err)
	}
}

// drain returns the jobs that remain in the queue.
func drain(t *testing.T, addr string) map[string]bool {
	t.Helper()

	return drainQueue(t, addr, "default")
}

// drainQueue fetches every job of one queue, and returns their jids.
func drainQueue(t *testing.T, addr, queue string) map[string]bool {
	t.Helper()

	worker, err := testclient.Dial(addr, "drain", "")
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()

	survivors := map[string]bool{}

	for {
		reply, err := worker.Command("FETCH", queue)
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

// TestKilledRunningJobsStayDead kills running jobs through the HTTP API, stops
// the server with SIGKILL, and checks that the jobs stay dead and that the
// worker cannot finish them.
func TestKilledRunningJobsStayDead(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "server")

	build := exec.Command("go", "build", "-o", binary, "github.com/thadeu/anway/cmd/server")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %s", output)
	}

	dir := t.TempDir()
	addr := freeAddr(t)
	web := freeAddr(t)

	t.Setenv("WEB_ADDR", web)

	server := startServer(t, binary, dir, addr)

	client, err := testclient.Dial(addr, "w", "")
	if err != nil {
		t.Fatal(err)
	}

	for _, jid := range []string{"a", "b", "c"} {
		if _, err := client.Command("PUSH", map[string]any{"jid": jid, "jobtype": "x", "args": []any{}}); err != nil {
			t.Fatal(err)
		}
	}

	for range 2 {
		if _, err := client.Command("FETCH", "default"); err != nil {
			t.Fatal(err)
		}
	}

	body := strings.NewReader(`{"action":"kill","set":"working","all":true,"queue":"default"}`)

	response, err := http.Post("http://"+web+"/api/v1/jobs/actions", "application/json", body)
	if err != nil {
		t.Fatal(err)
	}

	var result struct {
		Changed int `json:"changed"`
	}

	json.NewDecoder(response.Body).Decode(&result)
	response.Body.Close()

	if result.Changed != 2 {
		t.Fatalf("unexpected kill result: %+v", result)
	}

	client.Close()
	server.Process.Kill()
	server.Wait()

	startServer(t, binary, dir, addr)

	response, err = http.Get("http://" + web + "/api/v1/overview")
	if err != nil {
		t.Fatal(err)
	}

	var overview struct {
		States map[string]int `json:"states"`
	}

	json.NewDecoder(response.Body).Decode(&overview)
	response.Body.Close()

	if overview.States["working"] != 0 || overview.States["dead"] != 2 || overview.States["waiting"] != 1 {
		t.Fatalf("unexpected states after restart: %+v", overview.States)
	}

	worker, err := testclient.Dial(addr, "w", "")
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()

	if _, err := worker.Command("ACK", map[string]string{"jid": "a"}); err == nil {
		t.Fatal("the ACK of a killed job must fail")
	}

	if survivors := drain(t, addr); len(survivors) != 1 || !survivors["c"] {
		t.Fatalf("unexpected waiting jobs: %v", survivors)
	}
}

// TestBatchSurvivesKill stops the server between jobs of a committed batch.
func TestBatchSurvivesKill(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "server")

	build := exec.Command("go", "build", "-o", binary, "github.com/thadeu/anway/cmd/server")
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

// TestWorkerSettingSurvivesKill saves a console setting for a worker name,
// kills the server, and checks that a new process with that name gets it.
func TestWorkerSettingSurvivesKill(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "server")

	build := exec.Command("go", "build", "-o", binary, "github.com/thadeu/anway/cmd/server")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %s", output)
	}

	dir := t.TempDir()
	addr := freeAddr(t)
	web := freeAddr(t)

	t.Setenv("WEB_ADDR", web)

	server := startServer(t, binary, dir, addr)
	first := connectWorker(t, addr, "first")

	body := strings.NewReader(`{"autoscale":{"min":1,"max":1}}`)
	request, _ := http.NewRequest(http.MethodPatch, "http://"+web+"/api/v1/workers/first", body)

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}

	response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("the setting was not saved: %d", response.StatusCode)
	}

	first.Close()
	server.Process.Kill()
	server.Wait()

	startServer(t, binary, dir, addr)

	second := connectWorker(t, addr, "second")
	defer second.Close()

	response, err = http.Get("http://" + web + "/api/v1/workers")
	if err != nil {
		t.Fatal(err)
	}

	var listed struct {
		Workers []struct {
			WID       string         `json:"wid"`
			Saved     bool           `json:"saved"`
			Autoscale map[string]int `json:"autoscale"`
		} `json:"workers"`
	}

	json.NewDecoder(response.Body).Decode(&listed)
	response.Body.Close()

	if len(listed.Workers) != 1 || !listed.Workers[0].Saved || listed.Workers[0].Autoscale["max"] != 1 {
		t.Fatalf("the saved 1-1 must win over max 50 from HELLO after a kill: %+v", listed.Workers)
	}
}

// connectWorker opens a worker connection with a fixed name and autoscale up
// to 50 in HELLO, and sends a first BEAT with its threads.
func connectWorker(t *testing.T, addr, wid string) net.Conn {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}

	read := func() string {
		buffer := make([]byte, 512)

		conn.SetReadDeadline(time.Now().Add(5 * time.Second))

		n, _ := conn.Read(buffer)

		return string(buffer[:n])
	}

	read()

	fmt.Fprintf(conn, `HELLO {"v":2,"wid":%q,"hostname":"mac","tag":"shop","concurrency":1,"autoscale":{"max":50}}`+"\r\n", wid)

	if reply := read(); !strings.HasPrefix(reply, "+OK") {
		t.Fatalf("HELLO failed: %q", reply)
	}

	fmt.Fprintf(conn, `BEAT {"wid":%q,"concurrency":1}`+"\r\n", wid)
	read()

	return conn
}

// TestPublishSurvivesKill kills the server right after PUBLISH replies. Every
// copy, and the subscriptions, must exist after the restart.
func TestPublishSurvivesKill(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "server")

	build := exec.Command("go", "build", "-o", binary, "github.com/thadeu/anway/cmd/server")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %s", output)
	}

	dir := t.TempDir()
	addr := freeAddr(t)
	server := startServer(t, binary, dir, addr)

	client, err := testclient.Dial(addr, "", "")
	if err != nil {
		t.Fatal(err)
	}

	queues := []string{"realtime", "cassandra", "push"}

	for _, queue := range queues {
		if _, err := client.Command("SUBSCRIBE", map[string]string{"topic": "chat.message", "queue": queue, "jobtype": "chat." + queue}); err != nil {
			t.Fatal(err)
		}
	}

	for index := range 20 {
		if _, err := client.Command("PUBLISH", map[string]any{"topic": "chat.message", "args": []any{index}, "id": fmt.Sprint("msg-", index)}); err != nil {
			t.Fatal(err)
		}
	}

	client.Close()
	server.Process.Kill()
	server.Wait()

	startServer(t, binary, dir, addr)

	for _, queue := range queues {
		if got := len(drainQueue(t, addr, queue)); got != 20 {
			t.Fatalf("queue %s has %d of 20 copies after kill -9", queue, got)
		}
	}

	again, err := testclient.Dial(addr, "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()

	reply, err := again.Command("PUBLISH", map[string]any{"topic": "chat.message", "args": []any{}})
	if err != nil || !strings.Contains(reply, `"jobs":3`) {
		t.Fatalf("the subscriptions must survive kill -9: %q %v", reply, err)
	}
}

// TestPushBulkSurvivesKill kills the server right after PUSHB replies. Every
// job of the bulk must exist after the restart.
func TestPushBulkSurvivesKill(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "server")

	build := exec.Command("go", "build", "-o", binary, "github.com/thadeu/anway/cmd/server")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %s", output)
	}

	dir := t.TempDir()
	addr := freeAddr(t)
	server := startServer(t, binary, dir, addr)

	client, err := testclient.Dial(addr, "", "")
	if err != nil {
		t.Fatal(err)
	}

	jobs := make([]map[string]any, 500)

	for index := range jobs {
		jobs[index] = map[string]any{"jid": fmt.Sprint("bulk-", index), "jobtype": "x", "args": []any{index}}
	}

	reply, err := client.Command("PUSHB", jobs)
	if err != nil || reply != "{}" {
		t.Fatalf("PUSHB: %q %v", reply, err)
	}

	client.Close()
	server.Process.Kill()
	server.Wait()

	startServer(t, binary, dir, addr)

	if survivors := drain(t, addr); len(survivors) != 500 {
		t.Fatalf("%d of 500 bulk jobs survived kill -9", len(survivors))
	}
}
