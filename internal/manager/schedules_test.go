package manager

import (
	"testing"
	"time"
)

func TestTagsCountOnlineWorkersAndKeepTheLastSeenTime(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	m := New(store)

	m.Register(WorkerInfo{WID: "a", Tag: "shop", Concurrency: 5})
	m.Register(WorkerInfo{WID: "a", Tag: "shop", Concurrency: 5})
	m.Register(WorkerInfo{WID: "b", Tag: "shop", Concurrency: 5})
	m.Register(WorkerInfo{WID: "c", Concurrency: 5})

	tags, err := m.Tags()
	if err != nil {
		t.Fatal(err)
	}

	if len(tags) != 1 || tags["shop"].Online != 2 || tags["shop"].LastSeen.IsZero() {
		t.Fatalf("unexpected tags: %+v", tags)
	}

	m.Disconnect("a")
	m.Disconnect("b")

	if tags, _ := m.Tags(); tags["shop"].Online != 1 {
		t.Fatalf("a process with an open connection stays online: %+v", tags)
	}

	m.Disconnect("a")

	store.Close()

	store = openStore(t, dir)
	defer store.Close()

	m = New(store)

	tags, err = m.Tags()
	if err != nil {
		t.Fatal(err)
	}

	if tags["shop"].Online != 0 || time.Since(tags["shop"].LastSeen) > time.Minute {
		t.Fatalf("the last seen time of a tag must survive a restart: %+v", tags)
	}
}
