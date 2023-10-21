package main

import (
	"encoding/json"
	"errors"
	"log"
	"os"

	"github.com/google/uuid"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var messages []int
	var neighbors []string

	// body.type == "echo"
	n.Handle("echo", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "echo_ok"

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "generate_ok"
		body["id"] = uuid.New().String()

		return n.Reply(msg, body)
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message, ok := body["message"].(float64)
		if !ok {
			return errors.New("message is not an int")
		}

		var alreadyHasMessage bool = false
		for _, i := range messages {
			if i == int(message) {
				alreadyHasMessage = true
				break
			}
		}

		if !alreadyHasMessage {
			messages = append(messages, int(message))
			for _, neighbor := range neighbors {
				if err := pollSend(n, neighbor, msg.Body); err != nil {
					return err
				}
			}
		}

		delete(body, "message")
		body["type"] = "broadcast_ok"

		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = messages

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topology := body["topology"].(map[string]interface{})
		almostneighbours := topology[n.ID()].([]interface{})
		neighbors = nil
		for _, i := range almostneighbours {
			neighbors = append(neighbors, i.(string))
		}

		body["type"] = "topology_ok"
		delete(body, "topology")

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}

func pollSend(n *maelstrom.Node, dest string, body any) error {
	return n.RPC(dest, body, func(msg maelstrom.Message) error {
		if msg.RPCError() != nil {
			return pollSend(n, dest, body)
		}
		return nil
	})
}
