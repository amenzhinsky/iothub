# golang-iothub

This repository provides both SDK and command line tools for both device-to-cloud (`iotdevice`) and cloud-to-device (`iotservice`) functionality.

This project in the active development state and if you decided to use it anyway, please vendor the source code.

Some of features are missing see [TODO](https://github.com/amenzhinsky/golang-iothub#todo).

## Example

```go
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
```

For working examples, not only `iotdevice` lib, see `cmd` directory.

## CLI

There are two command line utilities: `iothub-device` and `iothub-sevice`. First is for using it on a IoT device and the second for managing and interacting with those devices. 

You can perform operations like publishing, subscribing events, registering and invoking direct method, subscribing to event feedback, etc. straight from the command line.

`iothub-service` is a [iothub-explorer](https://github.com/Azure/iothub-explorer) replacement that can be distributed as a single binary instead of need to install nodejs and add dependency hell that it brings.

See `-help` for more details.

## Testing

To enable end-to-end testing in the `tests` directory you need to set the following environment variables (hope these names are descriptive):

```
TEST_HOSTNAME
TEST_DEVICE_CONNECTION_STRING
TEST_DISABLED_DEVICE_CONNECTION_STRING
TEST_SERVICE_CONNECTION_STRING
TEST_X509_DEVICE
```

On the cloud side you need to create:

1. access policy (service connect perm)
1. disabled device (symmetric key)
1. enabled device (symmetric key)
1. enabled device (x509 self signed `443ABB6DEA8F93D5987D31D2607BE2931217752C`)

## TODO

1. Stabilize API.
1. Files uploading.
1. Batch sending.
1. HTTP transport.
1. AMQP transport.
1. AMQP-WS transport.
1. Complete set of subcommands for iothub-device and iothub-service.
1. Add missing iotservice functionality.
1. Grammar check.
1. Automated testing.
