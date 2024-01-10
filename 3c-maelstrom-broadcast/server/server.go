package server

import (
	"encoding/json"
	"maelstrom-broadcast/snowflake"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	n    *maelstrom.Node
	nbrs []string

	worker *snowflake.Worker

	idsMu sync.RWMutex
	ids   []int
}

func New(n *maelstrom.Node) (*Server, error) {
	worker, err := snowflake.NewWorker(int64(os.Getpid()))
	if err != nil {
		return nil, err
	}

	return &Server{
		n:      n,
		worker: worker,
	}, nil

}

func (s *Server) HandleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// message field is guaranteed to be an integer
	msgId := int(body["message"].(float64))

	s.idsMu.RLock()
	for _, id := range s.ids {
		if id == msgId {
			// might not be the one
			return nil
		}
	}
	s.idsMu.RUnlock()

	s.idsMu.Lock()
	s.ids = append(s.ids, msgId)
	s.idsMu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type": "broadcast_ok",
	})
}

func (s *Server) HandleRead(msg maelstrom.Message) error {
	body := make(map[string]any)

	s.idsMu.RLock()
	defer s.idsMu.RUnlock()
	body["messages"] = s.ids
	body["type"] = "read_ok"
	nextId, err := s.worker.NextId()
	if err != nil {
		return err
	}
	body["msg_id"] = nextId

	return s.n.Reply(msg, body)
}

type topologyBody struct {
	topology map[string][]string
}

func (s *Server) HandleTopology(msg maelstrom.Message) error {
	var body topologyBody

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	topology_nbrs := body.topology[s.n.ID()]

	nbrs := make([]string, len(topology_nbrs))

	for i := 0; i < len(topology_nbrs); i++ {
		nbrs[i] = topology_nbrs[i]
	}

	s.nbrs = nbrs

	out := make(map[string]any)
	out["type"] = "topology_ok"

	nextId, err := s.worker.NextId()
	if err != nil {
		return err
	}
	out["msg_id"] = nextId

	return s.n.Reply(msg, out)
}
