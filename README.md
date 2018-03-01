# golang-iothub

This repository provides both SDK and command line tools for device-to-cloud (`iotdevice`) and cloud-to-device (`iotservice`) functionality.

Right now only MQTT transport is implemented in `iotdevice` lib.

Many utility functions are missing in `iotservice` lib, e.g. managing devices, but pub/sub, directs methods and twin devices are already there.

This project in pre-alpha state if you decided to use it anyway, please vendor the source code.

## Example

```go
c, err := iotdevice.New(
		iotdevice.WithConnectionString(os.Getenv("DEVICE_CONNECTION_STRING")),

	// for x509 authentication use this options:
	// iotdevice.WithDeviceID(os.Getenv("DEVICE_ID")),
	// iotdevice.WithHostname(os.Getenv("HOSTNAME"),
	// iotdevice.WithX509FromFile(os.Getenv("TLS_CERT_FILE"), os.Getenv("TLS_KEY_FILE")),
	)
	if err != nil {
		return err
	}

	// interrupt all recepients when the function returns
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// publish a device-to-cloud event
	if err = c.PublishEvent(ctx, &iotdevice.Event{
		Payload:    []byte(`hello world`),
		Properties: map[string]string{"foo": "bar"},
	}); err != nil {
		return err
	}

	// need enough buffer space to avoid channel blocking on send
	errc := make(chan error, 2)

	// register "sum" direct method that sums "a" and "b" arguments and returns the result
	go func() {
		if err := c.SubscribeEvents(ctx, func(event *iotdevice.Event) {
			fmt.Printf("new message: %s\n", event.Payload)
		}); err != nil {
			errc <- err
		}
	}()

	// subscribe to cloud-to-device events
	go func() {
		if err := c.HandleMethod(ctx, "sum",  func(v map[string]interface{}) (map[string]interface{}, error) {
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

	return <-errc
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

1. HTTP transport.
1. Finalize AMQP transport.
1. AMQP-WS transport.
1. Complete set of subcommands for iothub-device and iothub-service.
1. Add missing iotservice functionality.
1. Grammar check.
