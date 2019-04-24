package main

import (
	"context"
	"fmt"
	"log"

	"github.com/amenzhinsky/iothub/iotservice"
)

func main() {
	// IOTHUB_SERVICE_CONNECTION_STRING environment variable must be set
	c, err := iotservice.New()
	if err != nil {
		log.Fatal(err)
	}

	// subscribe to device-to-cloud events
	log.Fatal(c.SubscribeEvents(context.Background(), func(msg *iotservice.Event) error {
		fmt.Printf("%q sends %q", msg.ConnectionDeviceID, msg.Payload)
		return nil
	}))
}
