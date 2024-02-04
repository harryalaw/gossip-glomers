package server

import (
	"encoding/json"
	"errors"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	KEY_ID  = "counter"
	LOCK_ID = "lock"
)

type nbrIds map[int]struct{}

type Server struct {
	n *maelstrom.Node
	k *Kafka

	log *log.Logger
}

func New(n *maelstrom.Node) (*Server, error) {
	log := log.New(
		os.Stderr,
		"",
		log.Ldate|log.Ltime|log.Lmicroseconds,
	)

	k := NewKafka()

	return &Server{
		n:   n,
		log: log,
		k:   k,
	}, nil

}

func (s *Server) Todo(msg maelstrom.Message) error {
	return errors.New("todo")
}

type SendBody struct {
	Type string
	Key  string
	Msg  int
}

func (s *Server) HandleSend(msg maelstrom.Message) error {
	var body SendBody

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offset := s.k.Append(body.Key, body.Msg)

	out := map[string]any{
		"type":   "send_ok",
		"offset": offset,
	}
	return s.n.Reply(msg, out)
}

type PollBody struct {
	Type    string
	Offsets map[string]int
}

func (s *Server) HandlePoll(msg maelstrom.Message) error {
	var body PollBody

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	msgs := s.k.Poll(body.Offsets)

	out := map[string]any{
		"type": "poll_ok",
		"msgs": msgs,
	}

	return s.n.Reply(msg, out)
}

type CommitBody struct {
	Type    string
	Offsets map[string]int
}

func (s *Server) HandleCommitOffsets(msg maelstrom.Message) error {
	var body CommitBody

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.k.CommitOffsets(msg.Src, body.Offsets)

	out := map[string]any{
		"type": "commit_offsets_ok",
	}

	return s.n.Reply(msg, out)
}

type ListBody struct {
	Type string
	Keys []string
}

func (s *Server) HandleListCommittedOffsets(msg maelstrom.Message) error {
	var body ListBody

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offsets := s.k.ListOffsets(msg.Src, body.Keys)

	out := map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets,
	}

	return s.n.Reply(msg, out)
}
