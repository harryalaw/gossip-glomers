package server

import (
	"context"
	"encoding/json"
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

	localDelta int
	muDelta    sync.RWMutex
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
	ctx := context.TODO()
	s.kv.CompareAndSwap(ctx, LOCK_ID, 0, 0, true)
	return s.kv.CompareAndSwap(ctx, KEY_ID, 0, 0, true)
}

type ReadMSg struct {
	Type string
}

func (s *Server) HandleRead(msg maelstrom.Message) error {
	ctx := context.Background()

	// check that the global lock is not engaged and retry until it's free
	for {
		lockVal, err := s.kv.ReadInt(ctx, LOCK_ID)
		if err != nil {
			s.log.Printf("error getting global lock: %+v", err)
		}
		if lockVal == 1 {
			s.log.Printf("global lock engaged: %d", lockVal)
		} else {
			break
		}
	}

	val, err := s.kv.ReadInt(ctx, KEY_ID)

	if err != nil {
		return err
	}

	s.muDelta.RLock()
	s.log.Printf("value in counter is %d, remaining local writes are: %d", val, s.localDelta)
	out := map[string]any{
		"type":  "read_ok",
		"value": val + s.localDelta,
	}
	s.muDelta.RUnlock()

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
		s.muDelta.Lock()
		s.localDelta += body.Delta
		s.muDelta.Unlock()
	}

	out := map[string]string{
		"type": "add_ok",
	}

	return s.n.Reply(msg, out)
}

func (s *Server) CommitAdds() {
	ticker := time.NewTicker(25 * time.Millisecond)
	go func() {
		for range ticker.C {
			if s.localDelta == 0 {
				continue
			}
			ctx := context.TODO()

			// try to get the global lock
			err := s.kv.CompareAndSwap(ctx, LOCK_ID, 0, 1, false)
			if err != nil {
				continue
			}

			// we have obtained the global lock
			s.log.Printf("retrieving current value of counter")
			prev, err := s.kv.ReadInt(ctx, KEY_ID)
			s.log.Printf("counter has value %d", prev)
			if err != nil {
				s.log.Printf("error reading KEY_ID in CommitAdds: %v\n", err)
			}
			s.muDelta.Lock()
			s.log.Printf("next delta is %d, Updating counter from: %d to: %d", s.localDelta, prev, prev+s.localDelta)
			err = s.kv.CompareAndSwap(ctx, KEY_ID, prev, prev+s.localDelta, false)

			if err != nil {
				s.log.Printf("error updating KEY_ID in CommitAdds: %v\n", err)
			}
			s.localDelta = 0
			s.log.Printf("localDelta is now %d", s.localDelta)
			s.muDelta.Unlock()

			err = s.kv.CompareAndSwap(ctx, LOCK_ID, 1, 0, false)
			s.log.Printf("released global lock")
			if err != nil {
				s.log.Println("someone else unlocked it :(")
			}
		}
	}()
}
