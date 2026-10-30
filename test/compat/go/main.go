// Command compat runs the core flow with the Go client.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	faktory "github.com/contribsys/faktory/client"
)

func main() {
	mode, jid := os.Args[1], os.Args[2]

	if mode != "push" {
		faktory.RandomProcessWid = "compat-go"
	}

	client, err := faktory.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	switch mode {
	case "beat":
		beatUntilTerminate(client)

		return
	case "admin":
		adminFlow(client)

		return
	case "batch":
		batchFlow(client)

		return
	case "stats":
		info, err := client.Info()
		if err != nil {
			log.Fatal(err)
		}

		tasks := info["faktory"].(map[string]any)["tasks"].(map[string]any)

		fmt.Println(tasks["Retries"].(map[string]any)["size"])

		return
	}

	if mode == "push" {
		job := faktory.NewJob("compat", "go", 42)
		job.Jid = jid

		if err := client.Push(job); err != nil {
			log.Fatal(err)
		}

		return
	}

	job, err := client.Fetch("default")
	if err != nil {
		log.Fatal(err)
	}

	if job == nil || job.Jid != jid {
		log.Fatalf("unexpected job: %+v", job)
	}

	if mode == "fail" {
		if err := client.Fail(job.Jid, errors.New("compat failure"), nil); err != nil {
			log.Fatal(err)
		}

		return
	}

	if err := client.Ack(job.Jid); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("go consumed %s with args %v\n", job.Jid, job.Args)
}

func beatUntilTerminate(client *faktory.Client) {
	seen := ""

	for {
		reply, err := client.Beat()
		if err != nil {
			log.Fatal(err)
		}

		var decoded struct {
			State string `json:"state"`
		}

		json.Unmarshal([]byte(reply), &decoded)

		state := decoded.State

		if state != "" && state != seen {
			seen = state
			fmt.Println("go beat", state)
		}

		if state == "terminate" {
			return
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func adminFlow(client *faktory.Client) {
	check := func(name string, err error) {
		if err != nil {
			log.Fatalf("%s: %v", name, err)
		}
	}

	check("pause", client.PauseQueues("admin"))
	check("resume", client.ResumeQueues("admin"))

	unique := faktory.NewJob("compat-unique", 1)
	unique.Queue = "admin"
	unique.SetCustom("unique_for", 60)
	check("unique push", client.Push(unique))

	duplicate := faktory.NewJob("compat-unique", 1)
	duplicate.Queue = "admin"
	duplicate.SetCustom("unique_for", 60)

	if err := client.Push(duplicate); err == nil || !strings.Contains(err.Error(), "NOTUNIQUE") {
		log.Fatalf("duplicate: %v", err)
	}

	sizes, err := client.QueueSizes()
	check("sizes", err)

	if sizes["admin"] != 1 {
		log.Fatalf("sizes: %v", sizes)
	}

	check("requeue", client.Requeue(faktory.Retries, faktory.OfType("compat")))
	check("clear", client.Clear(faktory.Dead))
	check("remove", client.RemoveQueues("admin"))

	fmt.Println("go admin flow passed")
}

func batchFlow(client *faktory.Client) {
	check := func(name string, err error) {
		if err != nil {
			log.Fatalf("%s: %v", name, err)
		}
	}

	success := faktory.NewJob("compat-success")
	success.Queue = "callbacks"

	complete := faktory.NewJob("compat-complete")
	complete.Queue = "callbacks"

	batch, err := client.BatchNew(&faktory.Batch{Description: "compat", Success: success, Complete: complete})
	check("new", err)

	member := faktory.NewJob("compat-member")
	member.Queue = "batch"
	member.SetCustom("bid", batch.Bid)
	member.SetCustom("track", 1)
	check("push", client.Push(member))

	_, err = client.BatchOpen(batch.Bid)
	check("open", err)
	check("commit", client.BatchCommit(batch.Bid))

	fetched, err := client.Fetch("batch")
	check("fetch", err)

	until := time.Now().Add(time.Hour)
	check("track set", client.TrackSet(fetched.Jid, 50, "half", &until))

	track, err := client.TrackGet(fetched.Jid)
	check("track get", err)

	if track.State != "enqueued" && track.State != "working" || track.Percent != 50 {
		log.Fatalf("track: %+v", track)
	}

	check("ack", client.Ack(fetched.Jid))

	status, err := client.BatchStatus(batch.Bid)
	check("status", err)

	if status.Total != 1 || status.Pending != 0 {
		log.Fatalf("status: %+v", status)
	}

	for _, kind := range []string{"compat-complete", "compat-success"} {
		callback, err := client.Fetch("callbacks")
		check("callback", err)

		if callback == nil || callback.Type != kind {
			log.Fatalf("callback: %+v", callback)
		}

		check("callback ack", client.Ack(callback.Jid))
	}

	fmt.Println("go batch flow passed")
}
