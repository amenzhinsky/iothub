# iothub

Azure IoT Hub SDK for Golang, provides both device-to-cloud ([`iotdevice`](iotdevice)) and cloud-to-device ([`iotservice`](iotservice)) packages for end-to-end communication.

This project is in active development state and if you decided to use it anyway, please vendor the source code. API is subject to change until `v1.0.0`.

Only **MQTT** is available for device-to-cloud communication at the moment.

## Examples

Send a message from an IoT device:

```go
package main

import (
	"context"
	"log"

	"github.com/amenzhinsky/iothub/iotdevice"
	iotmqtt "github.com/amenzhinsky/iothub/iotdevice/transport/mqtt"
)

func main() {
	// IOTHUB_DEVICE_CONNECTION_STRING environment variable must be set
	c, err := iotdevice.New(
		iotdevice.WithTransport(iotmqtt.New()),
	)
	if err != nil {
		log.Fatal(err)
	}

	// connect to the iothub
	if err = c.Connect(context.Background()); err != nil {
		log.Fatal(err)
	}

	// send a device-to-cloud message
	if err = c.SendEvent(context.Background(), []byte(`hello`)); err != nil {
		log.Fatal(err)
	}
}
```

Receive and print messages from IoT devices in a backend application:

```go
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
```

## Environment Variables

The following environment variables are used by the library unless corresponding options are set explicitly:

- `IOTHUB_DEVICE_CONNECTION_STRING` a device connection string.
- `IOTHUB_SERVICE_CONNECTION_STRING` a shared access policy connection string.
- `IOTHUB_DEVICE_LOG_LEVEL` controls `iotdevice` log level.
- `IOTHUB_SERVICE_LOG_LEVEL` controls `iotservice` log level.

Valid log level values are: `error`, `warn`, `info` and `debug`, default is `warn`.

## CLI

The project provides two command line utilities: `iothub-device` and `iothub-sevice`. First is for using it on IoT devices and the second manages and interacts with them. 

You can perform operations like publishing, subscribing to events and feedback, registering and invoking direct methods and so on straight from the command line.

`iothub-service` is a [iothub-explorer](https://github.com/Azure/iothub-explorer) replacement that can be distributed as a single binary opposed to a typical nodejs app.

See `-help` for more details.

## Testing

To enable end-to-end testing in the `tests` directory you need to provide `TEST_IOTHUB_SERVICE_CONNECTION_STRING` which is a shared access policy connection string with all permissions.

`TEST_EVENTHUB_CONNECTION_STRING` is required for `eventhub` package testing.

## TODO

1. Schedule twin updates and device method calls.
1. HTTP transport (files uploading).
1. AMQP transport (batch sending, WS).

## Contributing

All contributions are welcome.
