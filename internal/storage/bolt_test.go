package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/thadeu/anway/internal/job"
)

func TestWriteAfterCloseFails(t *testing.T) {
	store, err := OpenBolt(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	pushed := &job.Job{JID: "a", Type: "x", Args: json.RawMessage(`[]`)}

	if err := store.Push(pushed, time.Now()); !errors.Is(err, ErrClosed) {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRefusesNewerSchema(t *testing.T) {
	dir := t.TempDir()

	store, err := OpenBolt(dir)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMeta).Put(keySchema, binary.BigEndian.AppendUint64(nil, schemaVersion+1))
	})
	if err != nil {
		t.Fatal(err)
	}

	db.Close()

	if _, err := OpenBolt(dir); err == nil {
		t.Fatal("a newer schema must fail")
	}
}
