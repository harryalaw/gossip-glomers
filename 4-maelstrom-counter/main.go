package main

import (
	"log"

	"maelstrom-counter/server"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	s, err := server.New(n, kv)

	if err != nil {
		panic(err)
	}

	n.Handle("init", s.Init)

	n.Handle("read", s.HandleRead)

	n.Handle("add", s.HandleAdd)

	s.CommitAdds()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
