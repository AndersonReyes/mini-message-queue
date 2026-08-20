package broker

import (
	"fmt"
	"os"
	"testing"
)

// helper: open a registry in a fresh temp dir.
func openTempRegistry(t *testing.T) (*Registry, string) {
	t.Helper()
	dir := t.TempDir()
	r, err := OpenRegistry(dir)
	if err != nil {
		t.Fatalf("OpenRegistry(%q): %v", dir, err)
	}
	return r, dir
}

func closeRegistry(t *testing.T, r *Registry) {
	t.Helper()
	if err := r.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
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
	r, _ := openTempRegistry(t)
	defer closeRegistry(t, r)

	if err := r.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}

	payload := []byte("hello broker")
	part, off, err := r.Produce("t", payload, nil)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// TODO: finish this
	// we need to ensure the data is in the log
}
