package snowflake

import (
	"fmt"
	"sync"
	"time"
)

type Generator struct {
	mu sync.Mutex

	nodeId   int64
	sequence int64

	lastTimestamp int64
}

func NewGenerator(nodeId int64) (*Generator, error) {
	if 0 > nodeId {
		return nil, fmt.Errorf("invalid node ID={%d}: cannot be negative", nodeId)
	}

	return &Generator{
		nodeId: nodeId & maxNodeId,
	}, nil
}

const sequenceBits = 12
const nodeIdBits = 10
const sequenceMask = -1 ^ (-1 << sequenceBits)

const maxNodeId = -1 ^ (-1 << nodeIdBits)
const workerIdShift = sequenceBits
const timestampLeftShift = workerIdShift + sequenceBits

const epoch = 1704067200000

func (g *Generator) NextId() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	timestamp := timeGen()

	if g.lastTimestamp == timestamp {
		g.sequence = (g.sequence + 1) & sequenceMask

		if g.sequence == 0 {
			timestamp = nextMillis(g.lastTimestamp)
		}
	} else {
		g.sequence = 0
	}

	g.sequence += 1
	g.lastTimestamp = timestamp

	id := ((timestamp - epoch) << timestampLeftShift) |
		(g.nodeId << workerIdShift) |
		g.sequence

	return id
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
