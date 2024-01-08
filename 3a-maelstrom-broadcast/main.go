package main

import (
	"encoding/json"
	"fmt"
	"log"
	"maelstrom-broadcast/snowflake"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
    //errorPrint := log.New(os.Stderr, "", 1);

	worker, err := snowflake.NewWorker(int64(os.Getpid()))
	if err != nil {
		panic(fmt.Sprintf("Failed to create snowflake generator: %v", err))
	}
	msgs := make([]int, 0)

	n.Handle("echo", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "echo_ok"

		return n.Reply(msg, body)
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		id, err := worker.NextId()
		if err != nil {
			return err
		}

		body["type"] = "generate_ok"
		body["id"] = id

		return n.Reply(msg, body)
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

        // message field is guaranteed to be an integer
		msgId:= int(body["message"].(float64))
        // handle gossip

        delete(body, "message")
		body["type"] = "broadcast_ok"
        // might need to make this less race condition-y
		msgs = append(msgs, msgId)

		return n.Reply(msg, body)
	})

    n.Handle("read", func(msg maelstrom.Message) error {
        var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

        body["messages"]  = msgs
        body["type"] = "read_ok"

        return n.Reply(msg,body)
    })

    n.Handle("topology", func(msg maelstrom.Message) error {
        var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

        // handle topology request

        out := make(map[string]any)
        out["type"] = "topology_ok"

        return n.Reply(msg,out)
    })

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
