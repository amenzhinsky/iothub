# iothub

This repository provides Azure IoT Hub SDK for golang and command line tools for device-to-cloud (`iotdevice`) and cloud-to-device (`iotservice`) functionality.

This project in the active development state and if you decided to use it anyway, please vendor the source code.

Some features are missing, see [TODO](https://github.com/goautomotive/iothub#todo).

## Examples

Send a message from an IoT device:

```go
package main

import (
	"context"
	"log"
	"os"

	"github.com/goautomotive/iothub/iotdevice"
	"github.com/goautomotive/iothub/iotdevice/transport/mqtt"
)

func main() {
	c, err := iotdevice.NewClient(
		iotdevice.WithTransport(mqtt.New()),
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

	"github.com/goautomotive/iothub/common"
	"github.com/goautomotive/iothub/iotservice"
)

func main() {
	c, err := iotservice.NewClient(
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

There are two command line utilities: `iothub-device` and `iothub-sevice`. First is for using it on IoT devices and the second manages and interacts with them. 

You can perform operations like publishing, subscribing to events and feedback, registering and invoking direct method, etc. straight from the command line.

`iothub-service` is a [iothub-explorer](https://github.com/Azure/iothub-explorer) replacement that can be distributed as a single binary opposed to typical nodejs app.

See `-help` for more details.

## Testing

To enable end-to-end testing in the `tests` directory you need to provide `TEST_SERVICE_CONNECTION_STRING` which is a shared access policy connection string.

## TODO

1. Stabilize API.
1. Files uploading.
1. Batch sending.
1. HTTP transport.
1. AMQP transport.
1. AMQP-WS transport.
1. Retry policies.
1. Grammar check.
1. Rework debugging logs.
1. Make iotservice.Subscribe* functions non-blocking.
