package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/amenzhinsky/golang-iothub/common"
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

	// interrupt all recepients when the function returns
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err = c.Connect(ctx, false); err != nil {
		log.Fatal(err)
	}

	// send a device-to-cloud message
	if err = c.SendEvent(ctx, &common.Message{
		Payload:    []byte(`hello world`),
		Properties: map[string]string{"foo": "bar"},
	}); err != nil {
		log.Fatal(err)
	}

	// need enough buffer space to avoid channel blocking on send
	errc := make(chan error, 2)

	// register "sum" direct method that sums "a" and "b" arguments and returns the result
	go func() {
		if err := c.SubscribeEvents(ctx, func(msg *common.Message) {
			fmt.Printf("new message: %s\n", msg.Payload)
		}); err != nil {
			errc <- err
		}
	}()

	// subscribe to cloud-to-device events
	go func() {
		if err := c.HandleMethod(ctx, "sum", func(v map[string]interface{}) (map[string]interface{}, error) {
			a, ok := v["a"].(float64) // default type for JSON numbers
			if !ok {
				return nil, fmt.Errorf("malformed 'a' argument")
			}
			b, ok := v["b"].(float64)
			if !ok {
				return nil, fmt.Errorf("malformed 'b' argument")
			}
			return map[string]interface{}{
				"result": a + b,
			}, nil
		}); err != nil {
			errc <- err
		}
	}()

	log.Fatal(<-errc)
}
