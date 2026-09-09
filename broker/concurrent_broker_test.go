package broker

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/andersonreyes/mini-message-queue/utils"
)

func openConcurrentBroker(t *testing.T) (*ConcurrentBroker, string) {
	t.Helper()
	dir := t.TempDir()
	r, err := OpenBroker(dir)
	if err != nil {
		t.Fatalf("OpenBroker: %v", err)
	}

	cr, err := OpenConcurrentBroker(r, time.Hour)
	if err != nil {
		t.Fatalf("OpenConcurrentBroker: %v", err)
	}
	return cr, dir
}

func closeConcurrentBroker(t *testing.T, c *ConcurrentBroker) {
	t.Helper()
	if err := c.Close(); err != nil {
		t.Fatalf("ConcurrentBroker.Close(): %v", err)
	}
}

func TestConcurrentProducers(t *testing.T) {
	c, _ := openConcurrentBroker(t)
	defer closeConcurrentBroker(t, c)

	if err := c.broker.CreateTopic("concurrent", 4); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	const numGoRoutines = 10
	const msgsPerRoutine = 50
	var wg sync.WaitGroup

	errCh := make(chan error, numGoRoutines*msgsPerRoutine)

	for g := range numGoRoutines {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			for m := range msgsPerRoutine {
				payload := fmt.Appendf(nil, "id%d-m%d", id, m)
				utils.Logger.Debug("Sending payload", "payload", payload)
				_, _, err := c.Produce("concurrent", payload, nil)

				if err != nil {
					errCh <- err
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent produced error: %v", err)
	}
}

func TestConcurrentProducersAndReaders(t *testing.T) {
	// go lang will detect concurrent writes to the log and fail this test if no locks are used
	c, _ := openConcurrentBroker(t)
	defer closeConcurrentBroker(t, c)

	if err := c.broker.CreateTopic("concurrent", 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	const numGoRoutines = 10
	var wg sync.WaitGroup

	stop := make(chan bool)

	for g := range numGoRoutines {

		// Writer thread until stop channel has a value
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			done := false
			for !done {
				payload := fmt.Appendf(nil, "message to id=%d", id)

				select {
				case <-stop:
					done = true
				default:
					_, _, _ = c.Produce("concurrent", payload, nil)
				}
			}
		}(g)

		// reader
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			done := false
			for !done {

				for off := range numGoRoutines {
					select {
					case <-stop:
						done = true
					default:
						_, _ = c.Fetch("concurrent", 0, uint64(off))
					}

				}
			}
		}(g)
	}

	// wait a bit to try to create some contention
	time.Sleep(50 * time.Millisecond)
	// allow the go routines to stop now
	close(stop)
	wg.Wait()
}

func (c *ConcurrentBroker) Close() error {
	return c.broker.Close()
}
