package server

import (
	"encoding/json"
	"log"
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

	idsMu sync.RWMutex
	ids   map[int]struct{}

	nbrIdsMu sync.RWMutex
	nbrIds   map[string]nbrIds

	log *log.Logger
}

func New(n *maelstrom.Node) (*Server, error) {
	log := log.New(os.Stderr, "", 1)
	worker, err := snowflake.NewWorker(int64(os.Getpid()))
	if err != nil {
		return nil, err
	}
	ids := make(map[int]struct{})

	return &Server{
		n:      n,
		worker: worker,
		ids:    ids,
		log:    log,
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
	ids := make([]int, len(s.ids))
	i := 0
	for key := range s.ids {
		ids[i] = key
		i += 1
	}
	s.idsMu.RUnlock()
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

	s.nbrIdsMu.Lock()
	defer s.nbrIdsMu.Unlock()
	s.nbrIds = make(map[string]nbrIds)
	for _, nbr := range s.nbrs {
		s.nbrIds[nbr] = make(nbrIds)
	}

	out := make(map[string]any)
	out["type"] = "topology_ok"

	return s.n.Reply(msg, out)
}

func (s *Server) HandleGossip(msg maelstrom.Message) error {
	var body struct {
		Ids []int
	}

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// assume that we never get gossip from the same node at the same time
	s.nbrIdsMu.Lock()
	nbrIds := s.nbrIds[msg.Src]

	s.idsMu.Lock()
	for _, id := range body.Ids {
		nbrIds[id] = struct{}{}
		s.ids[id] = struct{}{}
	}
	s.idsMu.Unlock()
	s.nbrIdsMu.Unlock()

	s.idsMu.RLock()
	s.nbrIdsMu.RLock()

	resIds := make([]int, 0)
	for id := range s.ids {
		if _, pres := nbrIds[id]; !pres {
			resIds = append(resIds, id)
		}
	}
	s.nbrIdsMu.RUnlock()
	s.idsMu.RUnlock()

	out := struct {
		Type string
		Ids  []int
	}{
		Type: "gossip_ok",
		Ids:  resIds,
	}

	return s.n.Reply(msg, out)
}

func (s *Server) Gossip() {
	ticker := time.NewTicker(200 * time.Millisecond)
	go func() {
		for range ticker.C {
			for _, nbr := range s.nbrs {
				// make a list of ids that we don't know that they know
				// and send it to the nbr
				newIds := make([]int, 0)
				s.idsMu.RLock()
				s.nbrIdsMu.RLock()
				nbrsIds := s.nbrIds[nbr]
				for id := range s.ids {
					if _, pres := nbrsIds[id]; !pres {
						newIds = append(newIds, id)
					}
				}
				s.nbrIdsMu.RUnlock()
				s.idsMu.RUnlock()

				if len(newIds) > 0 {
					msg := map[string]any{
						"type": "gossip",
						"ids":  newIds,
					}

					s.n.RPC(nbr, msg, s.gossip(nbr, newIds))
				}
			}
		}
	}()
}

func (s *Server) gossip(nbr string, sentIds []int) func(msg maelstrom.Message) error {
	return func(msg maelstrom.Message) error {
		var body struct {
			Messages []int
		}

		// if we have a response we know that they know about the sentIds
		s.nbrIdsMu.Lock()
		for _, id := range sentIds {
			s.nbrIds[nbr][id] = struct{}{}
		}
		s.nbrIdsMu.Unlock()

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// update our list of ids with the response that we get
		s.idsMu.RLock()
		currIds := s.ids
		newIds := make([]int, 0)

		for _, id := range body.Messages {
			if _, pres := currIds[id]; !pres {
				newIds = append(newIds, id)
			}
		}
		s.idsMu.RUnlock()

		s.idsMu.Lock()
		for _, msg := range newIds {
			s.ids[msg] = struct{}{}
		}
		s.idsMu.Unlock()

		return nil
	}
}
