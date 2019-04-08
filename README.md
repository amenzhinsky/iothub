# iothub SDK for Golang

This repository provides Azure IoT Hub SDK for golang and command line tools for device-to-cloud (`iotdevice`) and cloud-to-device (`iotservice`) functionality.

This project in active development state and if you decided to use it anyway, please vendor the source code. API is subject to change until `v0.1.0`. 

Only **MQTT** is available for device-to-cloud communication at the moment.

See [TODO](https://github.com/amenzhinsky/iothub#todo) list to learn what is missing in the current implementation.

## Examples

Send a message from an IoT device:

```go
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
	); err != nil {
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
	"os"

	"github.com/amenzhinsky/iothub/common"
	"github.com/amenzhinsky/iothub/iotservice"
)

func main() {
	c, err := iotservice.New(
		iotservice.WithConnectionString(os.Getenv("SERVICE_CONNECTION_STRING")),
	)
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(c.SubscribeEvents(context.Background(), func(msg *common.Message) {
		fmt.Printf("%q sends %q", msg.ConnectionDeviceID, msg.Payload)
	}))
}
```

## CLI

The project provides two command line utilities: `iothub-device` and `iothub-sevice`. First is for using it on IoT devices and the second manages and interacts with them. 

You can perform operations like publishing, subscribing to events and feedback, registering and invoking direct methods and so on straight from the command line.

`iothub-service` is a [iothub-explorer](https://github.com/Azure/iothub-explorer) replacement that can be distributed as a single binary opposed to typical nodejs app.

See `-help` for more details.

## Testing

To enable end-to-end testing in the `tests` directory you need to provide `TEST_SERVICE_CONNECTION_STRING` which is a shared access policy connection string.

## TODO

1. Stabilize API.
1. HTTP transport (files uploading).
1. AMQP transport (batch sending, WS).
1. Grammar check plus better documentation.
1. Rework Subscribe* functions.

## Contributing

All contributions are welcome.
