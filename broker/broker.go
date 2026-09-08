package broker

import (
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"slices"

	"github.com/andersonreyes/mini-message-queue/storage"
	utils "github.com/andersonreyes/mini-message-queue/utils"
)

type Broker struct {
	dir string
	/// map topics to logs
	topicLogs map[string]*storage.Log
	/// track number of partitions for each topic
	partitions        map[string]uint32
	lastUsedPartition uint32
}

// OpenBroker opens (or creates) a registry rooted at dir.
// Layout: dir/<topic>/<partition_id>/ each containing a storage.Log.
func OpenBroker(dir string) (*Broker, error) {
	return &Broker{
		dir: dir, topicLogs: map[string]*storage.Log{}, partitions: map[string]uint32{},
		lastUsedPartition: 0,
	}, nil
}

func (r *Broker) Close() error {
	var allErrors []error
	for _, log := range r.topicLogs {

		if err := log.Close(); err != nil {
			allErrors = append(allErrors, err)
		}
	}

	if len(allErrors) > 0 {
		allErrors = append(allErrors, fmt.Errorf("Registry.Close(): failed to close some logs."))
	} else {
		allErrors = nil
	}

	return errors.Join(allErrors...)
}

func (r *Broker) CreateTopic(topic string, partitions uint32) error {

	n, ok := r.partitions[topic]
	if ok {
		if n == partitions {
			return nil
		} else {
			return fmt.Errorf("CreateTopic(): topic conflict. want %d but already have %d configured", partitions, n)
		}
	}

	r.partitions[topic] = partitions
	var allErrors error
	for p := range partitions {
		partition := fmt.Sprintf("%s/%d", topic, p)
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

func (r *Broker) NumPartitions(topic string) (uint32, error) {
	n, ok := r.partitions[topic]

	if !ok {
		return 0, fmt.Errorf("NumPartitions(): Topic does not exist: %s", topic)
	}
	return n, nil
}

func (r *Broker) TopicNames() []string {
	return slices.Sorted(maps.Keys(r.partitions))
}

// Produce appends payload to topic, routing by key (FNV-1a hash mod
// partitions) if key is non-nil, or round-robin across partitions if key is
// nil.
// Returns the partition index and the offset of the written record.
// Returns an error if the topic does not exist.
func (r *Broker) Produce(topic string, payload []byte, key []byte) (partition uint32, offset uint64, err error) {
	numPartitions, ok := r.partitions[topic]
	if !ok {
		return 0, 0, fmt.Errorf("Produce(): topic %s does not exist.", topic)
	}

	if key != nil {
		h := fnv.New32a()
		_, err := h.Write(key)

		if err != nil {
			return 0, 0, errors.Join(err, fmt.Errorf("Produce(): failed to hash key"))
		}

		partition = h.Sum32() % numPartitions
	} else {
		r.lastUsedPartition = (r.lastUsedPartition + 1) % numPartitions
		partition = r.lastUsedPartition
	}

	topicLogName := fmt.Sprintf("%s/%d", topic, partition)
	log, ok := r.topicLogs[topicLogName]
	if !ok {
		return 0, 0, fmt.Errorf("Failed to find a topic log using %s", topicLogName)
	}

	offset, err = log.Append(payload)
	if err != nil {
		return 0, 0, errors.Join(err, fmt.Errorf("Produce(): failed to append payload to log %s", topicLogName))
	}

	return partition, offset, nil
}

func (r *Broker) Fetch(topic string, partition uint32, offset uint64)([]byte, error) {
	topicLogName := fmt.Sprintf("%s/%d", topic, partition)
	log, ok := r.topicLogs[topicLogName]
	if !ok {
		utils.Logger.Error("topic or partition does not exist: ", "topic", topic, "partition", partition)
		return nil, fmt.Errorf("topic or partition does not exist %s\n", topicLogName)
	}

	payload, err := log.Read(offset)

	if err != nil {
		utils.Logger.Error("Failed to read ", "topic", topic, "partition", partition, "offset", offset)
		return nil, fmt.Errorf("Failed to read /topic/partition/offset: %s/%d/%d", topic, partition, offset)
	}
	return payload, nil
}
