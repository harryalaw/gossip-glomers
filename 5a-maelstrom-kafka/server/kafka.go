package server

import (
	"sync"
)

type Kafka struct {
	Logs map[string]*Topic
	// Map consumers to a map of topics to offsets
	Consumers map[string]map[string]int

	logMu  sync.Mutex
	offset int
}

func NewKafka() *Kafka {
	logs := make(map[string]*Topic)
	consumers := make(map[string]map[string]int)
	return &Kafka{
		Logs:      logs,
		Consumers: consumers,
		offset:    0,
	}
}

func (k *Kafka) Append(key string, val int) int {
	topic, ok := k.Logs[key]

	if !ok {
		topic = &Topic{Logs: []int{}}
		k.Logs[key] = topic
	}

	offset := topic.Add(val)

	return offset
}

func (k *Kafka) Poll(offsets map[string]int) map[string][][2]int {
	msgs := make(map[string][][2]int)
	for key, offset := range offsets {
		topic, ok := k.Logs[key]

		if !ok {
			continue
		}

		messages := topic.Poll(offset)
		msgs[key] = messages
	}

	return msgs
}

func (k *Kafka) CommitOffsets(src string, offsets map[string]int) {
	consumer, ok := k.Consumers[src]

	if !ok {
		consumer = make(map[string]int)
	}

	for key, val := range offsets {
		consumer[key] = val
	}

	k.Consumers[src] = consumer

	return
}

func (k *Kafka) ListOffsets(src string, keys []string) map[string]int {
	consumer, ok := k.Consumers[src]
	if !ok {
		consumer = make(map[string]int)
	}

	offsets := make(map[string]int)

	for _, key := range keys {
		val, ok := consumer[key]
		if !ok {
			val = 0
		}

		offsets[key] = val
	}

	return offsets
}

type Topic struct {
	Logs []int
	mu   sync.Mutex
}

func (t *Topic) Add(val int) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.Logs = append(t.Logs, val)

	return len(t.Logs) - 1
}

func (t *Topic) Poll(offset int) [][2]int {
	t.mu.Lock()
	defer t.mu.Unlock()

	maxOffset := len(t.Logs)
	if offset >= maxOffset {
		return [][2]int{}
	}

	msgCount := min(10, maxOffset-offset)
	msgs := make([][2]int, msgCount)

	for i := 0; i < len(msgs); i++ {
		msgs[i] = [2]int{offset + i, t.Logs[offset+i]}
	}

	return msgs
}
