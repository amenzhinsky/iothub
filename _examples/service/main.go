package main

import (
	"context"
	"fmt"
	"log"

	"github.com/amenzhinsky/iothub/iotservice"
)

func main() {
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
