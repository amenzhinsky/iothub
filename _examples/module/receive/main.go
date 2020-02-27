package main

import (
	"context"
	"log"
	"os"

	"github.com/amenzhinsky/iothub/iotmodule"
	iotmqtt "github.com/amenzhinsky/iothub/iotmodule/transport/mqtt"
)

func main() {
	c, err := iotmodule.NewFromConnectionString(
		iotmqtt.New(), os.Getenv("IOTHUB_MODULE_CONNECTION_STRING"), true,
	)
	if err != nil {
		log.Fatal(err)
	}

	// connect to the iothub
	if err = c.Connect(context.Background()); err != nil {
		log.Fatal(err)
	}

	// subscribe to events
	sub, err := c_module.SubscribeEvents(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	go printMsgs(sub)
	select {}
}

func printMsgs(sub *iotmodule.EventSub) {
	msgs := sub.C()
	err := sub.Err()
	for {
		if err != nil {
			fmt.Printf("Sub Error:\n%s\n\n", err)
		}
		msg := <-msgs
		fmt.Printf("Message:\n%s\n\n", msg.Payload)
	}
}
