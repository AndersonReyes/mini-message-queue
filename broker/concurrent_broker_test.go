package broker

import "testing"

func openBroker(t *testing.T) (*Broker, string) {
	t.Helper()
	dir := t.TempDir()
	r, err := OpenBroker(dir)
	if err != nil {
		t.Fatalf("OpenBroker: %v", err)
	}
	return r, dir
}
