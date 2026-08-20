package broker

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/andersonreyes/mini-message-queue/storage"
)

type Registry struct {
	dir string
	/// map topics to logs
	topicLogs map[string]*storage.Log
	partitions map[string]int
}

// OpenRegistry opens (or creates) a registry rooted at dir.
// Layout: dir/<topic>/<partition_id>/ each containing a storage.Log.
func OpenRegistry(dir string) (*Registry, error) {
	return &Registry{
		dir: dir, topicLogs: map[string]*storage.Log{}, partitions: map[string]int{},
	}, nil
}

func (r *Registry) Close() error {
	var allErrors error
	for _, log := range r.topicLogs {
		if err := log.Close(); err != nil {
			allErrors = errors.Join(allErrors,err)
		}
	}

	return allErrors
}

func (r *Registry) CreateTopic(topic string , partitions int) error {

	n, ok := r.partitions[topic]
	if ok {
		if n == partitions {
			return nil
		} else {
			return fmt.Errorf("topic conflict. want %d but already have %d configured", partitions, n)
		}
	}

	r.partitions[topic] = partitions
	var allErrors error
	for p := range partitions {
		partition := fmt.Sprintf("%s-%d", topic, p)
		dir := fmt.Sprintf("%s/%s/%d", r.dir, topic, p)
		log, err := storage.LogOpen(dir)
		if err != nil {
			allErrors = errors.Join(allErrors, err)
		} else {
			r.topicLogs[partition] = log
		}
	}
	return nil
}

func (r *Registry) NumPartitions(topic string) (int, error) {
	n, ok := r.partitions[topic]

	if !ok {
		return 0, fmt.Errorf("Topic does not exist: %s", topic)
	}
	return n, nil
}

func (r *Registry) TopicNames() []string {
	return slices.Sorted(maps.Keys(r.partitions))
}
