package main

import (
	"log"

	"maelstrom-broadcast/server"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// we should extract the gossiping to not be part of the broadcast loop
// broadcast should just get us to update the value we have
// then create a goroutine that invokes a method say every 500ms to try to gossip to its neighbours
// We can keep track of what nodes our neighbours know? Then just gossip to them every now and again

func main() {
	n := maelstrom.NewNode()
	//errorPrint := log.New(os.Stderr, "", 1);
	s, err := server.New(n)

	if err != nil {
		panic(err)
	}

	n.Handle("broadcast", s.HandleBroadcast)

	n.Handle("read", s.HandleRead)

	n.Handle("topology", s.HandleTopology)

	s.Gossip()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
