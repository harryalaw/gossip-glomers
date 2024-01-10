package server

import (
	"encoding/json"
	"maelstrom-broadcast/snowflake"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type nbrIds map[int]struct{}

type Server struct {
	n    *maelstrom.Node
	nbrs []string

	worker *snowflake.Worker

	idsMu  sync.RWMutex
	ids    map[int]struct{}
	nbrIds map[string]nbrIds
}

func New(n *maelstrom.Node) (*Server, error) {
	worker, err := snowflake.NewWorker(int64(os.Getpid()))
	if err != nil {
		return nil, err
	}
	ids := make(map[int]struct{})

	return &Server{
		n:      n,
		worker: worker,
		ids:    ids,
	}, nil

}

func (s *Server) HandleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// message field is guaranteed to be an integer
	msgId := int(body["message"].(float64))

	s.idsMu.Lock()
	s.ids[msgId] = struct{}{}
	s.idsMu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type": "broadcast_ok",
	})
}

func (s *Server) HandleRead(msg maelstrom.Message) error {
	body := make(map[string]any)

	s.idsMu.RLock()
	defer s.idsMu.RUnlock()
	ids := make([]int, len(s.ids))
	i := 0
	for key := range s.ids {
		ids[i] = key
		i += 1
	}
	body["messages"] = ids
	body["type"] = "read_ok"

	return s.n.Reply(msg, body)
}

type topologyBody struct {
	Topology map[string][]string
}

func (s *Server) HandleTopology(msg maelstrom.Message) error {
	var body topologyBody

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	topology_nbrs := body.Topology[s.n.ID()]

	nbrs := make([]string, len(topology_nbrs))

	for i := 0; i < len(topology_nbrs); i++ {
		nbrs[i] = topology_nbrs[i]
	}

	s.nbrs = nbrs

	s.nbrIds = make(map[string]nbrIds)
	for _, nbr := range s.nbrs {
		s.nbrIds[nbr] = make(nbrIds)
	}

	out := make(map[string]any)
	out["type"] = "topology_ok"

	return s.n.Reply(msg, out)
}

func (s *Server) Gossip() {
	ticker := time.NewTicker(300 * time.Millisecond)
	go func() {
		for range ticker.C {
			for _, nbr := range s.nbrs {
				msgId, err := s.worker.NextId()
				if err != nil {
					// print error to stderr
					continue
				}

				msg := map[string]any{
					"type":   "read",
					"msg_id": msgId,
				}

				// I think we should revisit this
				// We don't need to issue reads
				// We can take two approaches
				// -> Track who is sending us stuff (then recording where we've learnt stuff)
				// -> Track responses to our messages
				//     -> If we get an ACK then they now have it
				s.n.RPC(nbr, msg, s.gossip(nbr))
			}
		}
	}()
}

func (s *Server) gossip(nbr string) func(msg maelstrom.Message) error {
	// errorPrint := log.New(os.Stderr,"", 1)
	return func(msg maelstrom.Message) error {
		var body struct {
			messages []int
		}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.idsMu.Lock()
		for _, msg := range body.messages {
			s.ids[msg] = struct{}{}
			// _, seen := s.nbrIds[nbr][msg]
			// if !seen {
			// 	s.nbrIds[nbr][msg] = struct{}{}
			// }
		}
		s.idsMu.Unlock()

		// identify any differences between you and your neighbour
		// for msg := range s.ids {
		// 	_, seen := s.nbrIds[nbr][msg]
		// 	if !seen {
		//               errorPrint.Printf("Sending %d to %s\n", msg, nbr)
		// 		msgId, err := s.worker.NextId()
		// 		if err != nil {
		// 			// print error to stderr
		// 			continue
		// 		}
		//
		// 		msg := map[string]any{
		// 			"type":    "broadcast",
		// 			"message": msg,
		// 			"msg_id":  msgId,
		// 		}
		//
		// 		s.n.Send(nbr, msg)
		// 	}
		// }
		return nil
	}
}
