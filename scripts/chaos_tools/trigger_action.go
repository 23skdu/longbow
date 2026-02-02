package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/23skdu/longbow/client"
	"github.com/apache/arrow-go/v18/arrow/flight"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: trigger <action> [params...]")
		os.Exit(1)
	}

	actionType := os.Args[1]
	c, err := client.NewSmartClient("127.0.0.1:3000")
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	var body []byte
	switch actionType {
	case "TieredOffload":
		req := map[string]any{
			"dataset": "soak_test_collection",
			"max_age": "1s",
		}
		body, _ = json.Marshal(req)
	case "Compact":
		req := map[string]any{
			"dataset": "soak_test_collection",
		}
		body, _ = json.Marshal(req)
	}

	action := &flight.Action{
		Type: actionType,
		Body: body,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := c.DoAction(ctx, action)
	if err != nil {
		log.Fatalf("Action failed: %v", err)
	}

	for {
		res, err := stream.Recv()
		if err != nil {
			break
		}
		fmt.Printf("Result: %s\n", string(res.Body))
	}
}
