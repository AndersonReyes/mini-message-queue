package broker

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/andersonreyes/mini-message-queue/storage"
)

// helper: open a registry in a fresh temp dir.
func openTempRegistry(t *testing.T) (*Registry, string) {
	t.Helper()
	dir := t.TempDir()
	// dir := "./test-temp"
	r, err := OpenRegistry(dir)
	if err != nil {
		t.Fatalf("OpenRegistry(%q): %v", dir, err)
	}
	return r, dir
}

func closeRegistry(t *testing.T, r *Registry) {
	t.Helper()
	if err := r.Close(); err != nil {
		t.Fatalf("test closeRegistry(): %v", err)
	}
}

// ── CreateTopic ───────────────────────────────────────────────────────────────

func TestCreateTopicBasic(t *testing.T) {
	r, dir := openTempRegistry(t)
	defer closeRegistry(t, r)

	if err := r.CreateTopic("events", 3); err != nil {
		t.Fatalf("CreateTopic(events, 3): %v", err)
	}

	for p := range 3 {
		want := fmt.Sprintf("%s/events/%d/data.log", dir, p)

		if _, err := os.Stat(want); os.IsNotExist(err) {
			t.Fatalf("missing partition log: %s", want)
		}
	}

	n, err := r.NumPartitions("events")
	if err != nil {
		t.Fatalf("NumPartitions(events): %v", err)
	}
	if n != 3 {
		t.Errorf("NumPartitions(events) = %d, want 3", n)
	}
}

func TestCreateTopicIdempotent(t *testing.T) {
	r, _ := openTempRegistry(t)
	defer closeRegistry(t, r)

	if err := r.CreateTopic("orders", 2); err != nil {
		t.Fatalf("first CreateTopic: %v", err)
	}
	// Same name, same partition count — must be a no-op, not an error.
	if err := r.CreateTopic("orders", 2); err != nil {
		t.Fatalf("idempotent CreateTopic: %v", err)
	}
}

func TestCreateTopicConflict(t *testing.T) {
	r, _ := openTempRegistry(t)
	defer closeRegistry(t, r)

	if err := r.CreateTopic("logs", 4); err != nil {
		t.Fatal(err)
	}
	// Different partition count — must error.
	if err := r.CreateTopic("logs", 8); err == nil {
		t.Fatal("CreateTopic with conflicting partition count: expected error, got nil")
	}
}

func TestTopicNamesEmpty(t *testing.T) {
	r, _ := openTempRegistry(t)
	defer closeRegistry(t, r)

	names := r.TopicNames()
	if len(names) != 0 {
		t.Errorf("TopicNames() = %v, want []", names)
	}
}

func TestTopicNamesSorted(t *testing.T) {
	r, _ := openTempRegistry(t)
	defer closeRegistry(t, r)

	for _, name := range []string{"zebra", "alpha", "middle"} {
		if err := r.CreateTopic(name, 1); err != nil {
			t.Fatal(err)
		}
	}

	names := r.TopicNames()
	want := []string{"alpha", "middle", "zebra"}
	if len(names) != 3 {
		t.Fatalf("TopicNames() = %v, want %v", names, want)
	}
	for i, n := range names {
		if n != want[i] {
			t.Errorf("names[%d] = %q, want %q", i, n, want[i])
		}
	}
}

func TestNumPartitionsUnknownTopic(t *testing.T) {
	r, _ := openTempRegistry(t)
	defer closeRegistry(t, r)

	_, err := r.NumPartitions("nonexistent")
	if err == nil {
		t.Fatal("NumPartitions(nonexistent): expected error, got nil")
	}
}

func TestProduce(t *testing.T) {
	r, dir := openTempRegistry(t)
	topic := "test-topic"

	if err := r.CreateTopic(topic, 1); err != nil {
		t.Fatal(err)
	}

	payload := []byte("hello broker")
	partition, offset, err := r.Produce(topic, payload, []byte("testkey"))
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	if partition != 0 {
		t.Errorf("want partition 0 but got %d", partition)
	}

	if offset != 0 {
		t.Errorf("want offset 0 but got %d", offset)
	}

	closeRegistry(t, r)

	expectedPartitionDir := path.Join(dir, topic, "0")
	partitionLog, err := storage.LogOpen(expectedPartitionDir )
	if err != nil {
		t.Fatalf("failed to open raw log %v", err)
	}

	defer partitionLog.Close()

	entry, err := partitionLog.Read(offset)
	if err != nil {
		t.Fatalf("%s: error reading log record at offset %d: %v", expectedPartitionDir, offset, err)
	}

	if !bytes.Equal(payload, entry) {
		t.Errorf("want %v, but got %v", payload, entry)
	}

}
