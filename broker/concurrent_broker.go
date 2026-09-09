package broker

import (
	"sync"
	"time"
)

type ConcurrentBroker struct {
	broker *Broker
	m      sync.RWMutex
}

func OpenConcurrentBroker(r *Broker, flushInterval time.Duration) (*ConcurrentBroker, error) {
	return &ConcurrentBroker{broker: r}, nil
}

func (c *ConcurrentBroker) CreateTopic(topic string, numPartitions uint32) error {
	return c.broker.CreateTopic(topic, numPartitions)
}

func (c *ConcurrentBroker) Produce(topic string, payload []byte, key []byte) (partition uint32, offset uint64, err error) {
	c.m.Lock()
	defer c.m.Unlock()
	return c.broker.Produce(topic, payload, key)
}

func (c *ConcurrentBroker) Fetch(topic string, partition uint32, offset uint64) (payload []byte, err error) {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.broker.Fetch(topic, partition, offset)
}
