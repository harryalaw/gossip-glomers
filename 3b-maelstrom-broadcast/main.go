package main

import (
	"encoding/json"
	"fmt"
	"log"
	"maelstrom-broadcast/snowflake"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	//errorPrint := log.New(os.Stderr, "", 1);

	worker, err := snowflake.NewWorker(int64(os.Getpid()))
	if err != nil {
		panic(fmt.Sprintf("Failed to create snowflake generator: %v", err))
	}
	server := &Server{
		msgIds: []int{},
	}
	var nbrs []string

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// message field is guaranteed to be an integer
		msgId := int(body["message"].(float64))

		// handle gossip
		if !server.seen(msgId) {
			var msgBody map[string]any
			if err := json.Unmarshal(msg.Body, &msgBody); err != nil {
				return err
			}

			for _, nbr := range nbrs {
				if nbr == msg.Src {
					continue
				}

				nextId, err := worker.NextId()
				if err != nil {
					return err
				}
				msgBody["msg_id"] = nextId
				n.Send(nbr, msgBody)
			}
			server.add(msgId)
		}

		delete(body, "message")
		body["type"] = "broadcast_ok"
		nextId, err := worker.NextId()
		if err != nil {
			return err
		}
		body["msg_id"] = nextId

		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["messages"] = server.Msgs()
		body["type"] = "read_ok"
		nextId, err := worker.NextId()
		if err != nil {
			return err
		}
		body["msg_id"] = nextId

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topology := body["topology"].(map[string]any)
		topology_nbrs := topology[n.ID()].([]any)

		nbrs = make([]string, len(topology_nbrs))

		for i := 0; i < len(topology_nbrs); i++ {
			nbrs[i] = topology_nbrs[i].(string)
		}

		out := make(map[string]any)
		out["type"] = "topology_ok"

		nextId, err := worker.NextId()
		if err != nil {
			return err
		}
		body["msg_id"] = nextId

		return n.Reply(msg, out)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type Server struct {
	msgIds []int
	mu     sync.Mutex
}

func (s *Server) seen(i int) bool {
	for _, val := range s.msgIds {
		if val == i {
			return true
		}
	}
	return false
}

func (s *Server) add(i int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msgIds = append(s.msgIds, i)
}

func (s *Server) Msgs() []int {
	return s.msgIds
}
