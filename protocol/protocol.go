package protocol

// Api request
type Request struct {
	Type       string   `json:"type"`
	Topic      string   `json:"topic,omitempty"`
	Topics     []string `json:"topics,omitempty"`
	Partition  int      `json:"partition,omitempty"`
	Partitions []int    `json:"partitions,omitempty"`
	Offset     uint64   `json:"offset,omitempty"`
	MaxCount   int      `json:"max_count,omitempty"`
	Key        string   `json:"key,omitempty"`     // base64 encoded
	Payload    string   `json:"payload,omitempty"` // base64 encoded
	Group      string   `json:"group,omitempty"`
	MemberId   string   `json:"member_id,omitempty"`
}

// meta about a topic
type TopicMetadata struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
}

// Assignment of topic, partition to a group member (consumers)
type Assigment struct {
	Topic     string `json:"topic"`
	Partition uint32 `json:"partition"`
}
