package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	KEY_ID  = "counter"
	LOCK_ID = "lock"
)

type nbrIds map[int]struct{}

type Server struct {
	n  *maelstrom.Node
	kv *maelstrom.KV

	log *log.Logger

	localCache map[string]int
	muCache    sync.Mutex
}

func New(n *maelstrom.Node, kv *maelstrom.KV) (*Server, error) {
	log := log.New(
		os.Stderr,
		"",
		log.Ldate|log.Ltime|log.Lmicroseconds,
	)

	return &Server{
		n:   n,
		kv:  kv,
		log: log,
	}, nil

}

func (s *Server) Init(msg maelstrom.Message) error {
	var body maelstrom.InitMessageBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("unmarshal init message body: %w", err)
	}

	s.localCache = make(map[string]int)
	for _, id := range body.NodeIDs {
		s.localCache[id] = 0
	}

	ctx := context.TODO()
	return s.kv.CompareAndSwap(ctx, body.NodeID, 0, 0, true)
}

type ReadMSg struct {
	Type string
}

func (s *Server) HandleRead(msg maelstrom.Message) error {
	val := s.getCounter()

	s.log.Printf("value in counter is %d", val)
	out := map[string]any{
		"type":  "read_ok",
		"value": val,
	}

	return s.n.Reply(msg, out)
}

type AddMsg struct {
	Type  string
	Delta int
}

func (s *Server) HandleAdd(msg maelstrom.Message) error {
	var body AddMsg

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	if body.Delta != 0 {
		s.muCache.Lock()
		s.localCache[s.n.ID()] += body.Delta
		ctx := context.TODO()
		s.kv.Write(ctx, s.n.ID(), s.localCache[s.n.ID()])
		s.muCache.Unlock()
	}

	out := map[string]string{
		"type": "add_ok",
	}

	return s.n.Reply(msg, out)
}

func (s *Server) RefreshCache() {
	ticker := time.NewTicker(100 * time.Millisecond)
	go func() {
		for range ticker.C {
			ctx := context.TODO()
			selfId := s.n.ID()

			newValues := make(map[string]int)
			for _, node := range s.n.NodeIDs() {
				if node == selfId {
					continue
				}

				val, err := s.kv.ReadInt(ctx, node)
				if err != nil {
					s.log.Printf("error reading node: [%s] in CommitAdds: %v\n", node, err)
				}
				newValues[node] = val
			}

			s.muCache.Lock()
			for node, val := range newValues {
				s.localCache[node] = val
			}
			s.muCache.Unlock()
		}
	}()
}

func (s *Server) getCounter() int {
	s.muCache.Lock()
	defer s.muCache.Unlock()

	sum := 0

	for _, count := range s.localCache {
		sum += count
	}

	return sum
}
