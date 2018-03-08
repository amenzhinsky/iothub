# golang-iothub

This repository provides SDK for golang and command line tools for both device-to-cloud (`iotdevice`) and cloud-to-device (`iotservice`) functionality.

This project in the active development state and if you decided to use it anyway, please vendor the source code.

Some features are missing, see [TODO](https://github.com/amenzhinsky/golang-iothub#todo).

## Examples

Send a message from an IoT device:

```go
package main

import (
	"context"
	"log"
	"os"

	"github.com/amenzhinsky/golang-iothub/iotdevice"
	"github.com/amenzhinsky/golang-iothub/iotdevice/transport/mqtt"
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

	"github.com/amenzhinsky/golang-iothub/common"
	"github.com/amenzhinsky/golang-iothub/iotservice"
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

There are two command line utilities: `iothub-device` and `iothub-sevice`. First is for using it on a IoT device and the second for managing and interacting with those devices. 

You can perform operations like publishing, subscribing events, registering and invoking direct method, subscribing to event feedback, etc. straight from the command line.

`iothub-service` is a [iothub-explorer](https://github.com/Azure/iothub-explorer) replacement that can be distributed as a single binary instead of need to install nodejs and add dependency hell that it brings.

See `-help` for more details.

## Testing

To enable end-to-end testing in the `tests` directory you need to set the following environment variables:

| Name | Description |
| --- | --- |
| `TEST_HOSTNAME`                 | *.azure-devices.net |
| `TEST_DEVICE_CONNECTION_STRING` | enabled device connection string |
| `TEST_DISABLED_DEVICE_CONNECTION_STRING` | disabled device connection string |
| `TEST_SERVICE_CONNECTION_STRING` | access policy with `service connect` permission |
| `TEST_X509_DEVICE` | enabled device with x509 self signed certificate, thumbprint:  [443ABB6DEA8F93D5987D31D2607BE2931217752C](https://github.com/amenzhinsky/golang-iothub/blob/master/tests/testdata/device.crt) |

## TODO

1. Stabilize API.
1. Files uploading.
1. Batch sending.
1. HTTP transport.
1. AMQP transport.
1. AMQP-WS transport.
1. Retry policies.
1. Complete list of subcommands and api call for iothub-device and iothub-service.
1. Grammar check.
1. Automated testing, manual now.
1. Simplify testing by using only one access policy to create required devices.
1. Rework debugging logs.
