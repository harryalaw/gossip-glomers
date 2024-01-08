package snowflake

import (
	"fmt"
	"sync"
	"time"
)

type Worker struct {
	mu sync.Mutex

	nodeId   int64
	sequence int64

	lastTimestamp int64
}

func NewWorker(nodeId int64) (*Worker, error) {
	if 0 > nodeId {
		return nil, fmt.Errorf("invalid node ID={%d}: cannot be negative", nodeId)
	}

	return &Worker{
		nodeId:        nodeId & maxNodeId,
		sequence:      0,
		lastTimestamp: 0,
	}, nil
}

const (
	sequenceBits = uint64(12)
	nodeIdBits   = uint64(10)

	sequenceMask = -1 ^ (-1 << sequenceBits)
	maxNodeId    = -1 ^ (-1 << nodeIdBits)

	nodeIdShift        = sequenceBits
	timestampLeftShift = nodeIdBits + sequenceBits
	epoch              = 1704067200000
)

func (w *Worker) NextId() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.nextId()
}

func (w *Worker) nextId() (uint64, error) {
	timestamp := timeGen()

	if timestamp < w.lastTimestamp {
		return 0, fmt.Errorf("time is moving backwards")
	}

	if w.lastTimestamp == timestamp {
		w.sequence = (w.sequence + 1) & sequenceMask

		if w.sequence == 0 {
			timestamp = nextMillis(w.lastTimestamp)
		}
	} else {
		w.sequence = 0
	}

	w.lastTimestamp = timestamp

	id := ((timestamp - epoch) << timestampLeftShift) |
		(w.nodeId << nodeIdShift) |
		w.sequence

	return uint64(id), nil
}

func nextMillis(lastTimestamp int64) int64 {
	timestamp := timeGen()
	for timestamp <= lastTimestamp {
		timestamp = timeGen()
	}
	return timestamp
}

func timeGen() int64 {
	return time.Now().UnixMilli()
}
