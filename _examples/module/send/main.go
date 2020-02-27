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

	// send a device-to-cloud message
	if err = c.SendEvent(context.Background(), []byte("hello")); err != nil {
		log.Fatal(err)
	}
}
