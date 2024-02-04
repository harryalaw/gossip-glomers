package main

import (
	"log"
	"maelstrom-kafka/server"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	s, err := server.New(n)

	if err != nil {
		panic(err)
	}

	n.Handle("send", s.HandleSend)
	n.Handle("poll", s.HandlePoll)
	n.Handle("commit_offsets", s.HandleCommitOffsets)
	n.Handle("list_committed_offsets", s.HandleListCommittedOffsets)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
