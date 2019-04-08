package main

import (
	"context"
	"log"
	"os"

	"github.com/amenzhinsky/iothub/iotdevice"
	iotmqtt "github.com/amenzhinsky/iothub/iotdevice/transport/mqtt"
)

func main() {
	c, err := iotdevice.New(
		iotdevice.WithTransport(iotmqtt.New()),
		iotdevice.WithConnectionString(os.Getenv("DEVICE_CONNECTION_STRING")),
	)
	if err != nil {
		log.Fatal(err)
	}

	// connect to the iothub
	if err = c.Connect(context.Background()); err != nil {
		log.Fatal(err)
	}

	// send a device-to-cloud message
	if err = c.SendEvent(context.Background(), []byte(`hello`),
		iotdevice.WithSendProperty("foo", "bar"),
		iotdevice.WithSendQoS(0), // 1 is the default value
	); err != nil {
		log.Fatal(err)
	}
}
