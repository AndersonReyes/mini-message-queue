package storage

import (
	"bytes"
	"io"
	"os"
	"path"
	"testing"
	// "fmt"
	// "github.com/andersonreyes/mini-message-queue/utils"
)

func openLog(t *testing.T, dir string) *Log {

	l, err := LogOpen(dir)
	if err != nil {
		t.Fatalf("LogOpen(%q) error: %v", dir, err)
	}
	return l
}

// helper: open a log in a fresh temp dir.
func openTempLog(t *testing.T) (*Log, string) {
	t.Helper()
	dir := t.TempDir()
	// dir := "./test-temp"
	return openLog(t, dir), dir
}

// helper: close log, asserting no error.
func closeLog(t *testing.T, l *Log) {
	t.Helper()
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
}

// ── Phase 1: basic append + read ─────────────────────────────────────────────

func TestAppendAndReadSingleRecord(t *testing.T) {
	l, dir := openTempLog(t)
	defer closeLog(t, l)

	payload := []byte("hello world")
	off, err := l.Append(payload)
	if err != nil {
		t.Fatalf("Append() error: %v", err)
	}
	if off != 0 {
		t.Fatalf("first Append() offset = %d, want 0", off)
	}

	f, err := os.Open(path.Join(dir, "data.log"))
	if err != nil {
		t.Fatalf("Failed to open data log: %v", err)
	}

	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("Failed to read data log: %v", err)
	}

	if !bytes.Equal(got[12:], payload) {
		t.Errorf("payload = %q, want %q", got, payload)
	}

	if !bytes.Equal(got[8:12], []byte{0, 0, 0, 11}) {
		t.Errorf("length = %q, want %d", got[8:12], len(payload))
	}
}

func TestReadEachOffset(t *testing.T) {
	/// Write some data, close the log, reopen and ensure dat still there
	l, _ := openTempLog(t)
	defer closeLog(t, l)

	testCases := [][]byte{
		[]byte("alpha"),
		[]byte("beta"),
		[]byte("gamma"),
		[]byte("delta"),
	}

	for want, p := range testCases {
		got, err := l.Append(p)
		if err != nil {
			t.Fatalf("failed to append: %v", err)
		}

		if uint64(want) != got {
			t.Errorf("offset = %d, want %d", got, want)
		}
	}

	for offset, want := range testCases {
		got, err := l.Read(uint64(offset))

		if err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		if !bytes.Equal(want, got) {
			t.Errorf("payload = %q, want %q", got, want)
		}
	}
}

func TestStorageIsDurable(t *testing.T) {
	testCases := [][]byte{
		[]byte("alpha"),
		[]byte("beta"),
		[]byte("gamma"),
		[]byte("delta"),
	}

	 dir := t.TempDir()

	{
		l := openLog(t, dir)
		defer closeLog(t, l)
		for want, p := range testCases {
			got, err := l.Append(p)

			if err != nil {
				t.Fatalf("failed to append: %v", err)
			}

			if uint64(want) != got {
				t.Errorf("offset = %d, want %d", got, want)
			}
		}
	}

	{
		l2 := openLog(t, dir)
		defer closeLog(t, l2)

		for offset, want := range testCases {
			got, err := l2.Read(uint64(offset))

			if err != nil {
				t.Fatalf("failed to read offset %d: %v", offset, err)
			}

			if !bytes.Equal(want, got) {
				t.Errorf("payload = %q, want %q", got, want)
			}
		}

	}
}

func TestCrashRecoveryPartialPayload(t *testing.T) {
	// dir := t.TempDir()
	dir := "./test-temp"

	l, err := LogOpen(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append([]byte("good")); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// Append a full 12-byte header claiming 1000-byte payload, then only 3 bytes.
	logPath := path.Join(dir, "data.log")
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("open data.log for corruption: %v", err)
	}
	// offset=1, length=1000 in big-endian, then 3 bytes of "payload"
	header := []byte{
		0, 0, 0, 0, 0, 0, 0, 1, // offset=1 uint64 BE
		0, 0, 3, 232, // length=1000 uint32 BE
	}
	if _, err := f.Write(header); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("pay")); err != nil { // only 3 of 1000 bytes
		t.Fatal(err)
	}
	f.Close()

	// Reopen — should truncate the partial payload record.
	l2, err := LogOpen(dir)
	if err != nil {
		t.Fatalf("LogOpen after partial-payload corruption: %v", err)
	}
	defer closeLog(t, l2)

	got, err := l2.Read(0)
	if err != nil {
		t.Fatalf("Read(0) after crash recovery: %v", err)
	}
	if !bytes.Equal(got, []byte("good")) {
		t.Errorf("Read(0) = %q, want \"good\"", got)
	}

	_, err = l2.Read(1)
	if err == nil {
		t.Error("Read(1) should error after crash recovery of partial payload")
	}
}
