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
func openTempBroker(t *testing.T) (*Broker, string) {
	t.Helper()
	dir := t.TempDir()
	// dir := "./test-temp"
	r, err := OpenBroker(dir)
	if err != nil {
		t.Fatalf("OpenBroker(%q): %v", dir, err)
	}
	return r, dir
}

func closeBroker(t *testing.T, r *Broker) {
	t.Helper()
	if err := r.Close(); err != nil {
		t.Fatalf("test closeBroker(): %v", err)
	}
}

// ── CreateTopic ───────────────────────────────────────────────────────────────

func TestCreateTopicBasic(t *testing.T) {
	r, dir := openTempBroker(t)
	defer closeBroker(t, r)

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
	r, _ := openTempBroker(t)
	defer closeBroker(t, r)

	if err := r.CreateTopic("orders", 2); err != nil {
		t.Fatalf("first CreateTopic: %v", err)
	}
	// Same name, same partition count — must be a no-op, not an error.
	if err := r.CreateTopic("orders", 2); err != nil {
		t.Fatalf("idempotent CreateTopic: %v", err)
	}
}

func TestCreateTopicConflict(t *testing.T) {
	r, _ := openTempBroker(t)
	defer closeBroker(t, r)

	if err := r.CreateTopic("logs", 4); err != nil {
		t.Fatal(err)
	}
	// Different partition count — must error.
	if err := r.CreateTopic("logs", 8); err == nil {
		t.Fatal("CreateTopic with conflicting partition count: expected error, got nil")
	}
}

func TestTopicNamesEmpty(t *testing.T) {
	r, _ := openTempBroker(t)
	defer closeBroker(t, r)

	names := r.TopicNames()
	if len(names) != 0 {
		t.Errorf("TopicNames() = %v, want []", names)
	}
}

func TestTopicNamesSorted(t *testing.T) {
	r, _ := openTempBroker(t)
	defer closeBroker(t, r)

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
	r, _ := openTempBroker(t)
	defer closeBroker(t, r)

	_, err := r.NumPartitions("nonexistent")
	if err == nil {
		t.Fatal("NumPartitions(nonexistent): expected error, got nil")
	}
}

func TestProduce(t *testing.T) {
	r, dir := openTempBroker(t)
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

	closeBroker(t, r)

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

func TestProduceUnknownTopic(t *testing.T) {
	r, _ := openTempBroker(t)
	defer closeBroker(t, r)

	_, _, err := r.Produce("missing", []byte("x"), nil)
	if err == nil {
		t.Fatal("Produce to unknown topic: expected error, got nil")
	}
}

func TestFetchUnknownTopic(t *testing.T) {
	r, _ := openTempBroker(t)
	defer closeBroker(t, r)

	_, err := r.Fetch("missing", 0, 0)
	if err == nil {
		t.Fatal("Fetch from unknown topic: expected error, got nil")
	}
}

func TestFetchOutOfRange(t *testing.T) {
	r, _ := openTempBroker(t)
	defer closeBroker(t, r)

	if err := r.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Produce("t", []byte("a"), nil); err != nil {
		t.Fatal(err)
	}

	_, err := r.Fetch("t", 0, 99)
	if err == nil {
		t.Fatal("Fetch out-of-range offset: expected error, got nil")
	}
}

func TestFetchUnknownPartition(t *testing.T) {
	r, _ := openTempBroker(t)
	defer closeBroker(t, r)

	if err := r.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}

	_, err := r.Fetch("t", 99, 0)
	if err == nil {
		t.Fatal("Fetch from nonexistent partition: expected error, got nil")
	}
}

func TestProduceFetchRoundtrip(t *testing.T) {
	r, _ := openTempBroker(t)
	defer closeBroker(t, r)

	if err := r.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}

	payload := []byte("hello broker")
	part, off, err := r.Produce("t", payload, nil)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	got, err := r.Fetch("t", part, off)
	if err != nil {
		t.Fatalf("Fetch(t, %d, %d): %v", part, off, err)
	}
	if string(got) != string(payload) {
		t.Errorf("Fetch = %q, want %q", got, payload)
	}
}


func TestRoundRobinNoKey(t *testing.T) {
	r, _ := openTempBroker(t)
	defer closeBroker(t, r)

	const numPartitions = 3
	if err := r.CreateTopic("rr", numPartitions); err != nil {
		t.Fatal(err)
	}

	// Produce 6 messages with no key; each partition should get exactly 2.
	counts := make(map[uint32]int)
	for range 6 {
		p, _, err := r.Produce("rr", []byte("msg"), nil)
		if err != nil {
			t.Fatalf("Produce: %v", err)
		}
		counts[p]++
	}
	if len(counts) != numPartitions {
		t.Fatalf("round-robin used %d partitions, want %d", len(counts), numPartitions)
	}
	for part, count := range counts {
		if count != 2 {
			t.Errorf("partition %d got %d messages, want 2", part, count)
		}
	}
}

func TestKeyRoutingDeterministic(t *testing.T) {
	r, _ := openTempBroker(t)
	defer closeBroker(t, r)

	if err := r.CreateTopic("k", 5); err != nil {
		t.Fatal(err)
	}

	key := []byte("user-42")
	var firstPart uint32
	for i := range 10 {
		p, _, err := r.Produce("k", []byte("payload"), key)
		if err != nil {
			t.Fatalf("Produce #%d: %v", i, err)
		}
		if i == 0 {
			firstPart = p
		} else if p != firstPart {
			t.Fatalf("key routing not deterministic: got partition %d then %d", firstPart, p)
		}
	}
}
