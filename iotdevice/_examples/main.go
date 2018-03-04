package main

import (
	"context"
	"log"
	"os"

	"github.com/amenzhinsky/golang-iothub/iotdevice"
	"github.com/amenzhinsky/golang-iothub/iotdevice/transport/mqtt"
)

func main() {
	t, err := mqtt.New()
	if err != nil {
		log.Fatal(err)
	}
	c, err := iotdevice.NewClient(
		iotdevice.WithTransport(t),
		iotdevice.WithConnectionString(os.Getenv("DEVICE_CONNECTION_STRING")),
	)
	if err != nil {
		log.Fatal(err)
	}

	// connect to the iothub
	if err = c.Connect(context.Background(), false); err != nil {
		log.Fatal(err)
	}

	// send a device-to-cloud message
	if err = c.SendEvent(context.Background(), []byte(`hello`),
		iotdevice.WithSendProperty("a", "1"),
		iotdevice.WithSendProperty("b", "2"),
	); err != nil {
		log.Fatal(err)
	}
}
