package main

import (
	"log"

	"maelstrom-counter-alt/server"

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

	s.RefreshCache()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
