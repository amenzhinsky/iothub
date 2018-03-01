# golang-iothub-sdk

This repository provides both library and command line tools for device-to-cloud (`device`) and cloud-to-device (`service`) functionality.

It's in alpha state if you decided to use it, please vendor the source code.

## Example

TODO

## CLI

There are two command line utilities: `iothub-device` and `iothub-sevice` for interacting from the device as well as cloud prospective. 

Yoy can perform operations like publishing, subscribing events, registering and invoking direct method, subscribing to event feedback, etc. straight from the command line.

`iothub-service` is a [iothub-explorer](https://github.com/Azure/iothub-explorer) replacement that can be distributed as a single binary instead of need to install nodejs and add dependency hell that it brings.

See `-help` for more details.

## Testing

To enable end-to-end testing in the `tests` directory you need to set the following environment variables (hope these names are descriptive):

```
TEST_DEVICE_CONNECTION_STRING
TEST_SERVICE_CONNECTION_STRING
TEST_X509_DEVICE
TEST_X509_HOSTNAME
```

On the cloud side you need to create an access policy, a device with symmetric key and a device with x509 authentication (key thumbprint `443ABB6DEA8F93D5987D31D2607BE2931217752C`).

## TODO

1. HTTP transport.
1. AMQP and AMQP-WS transport.
1. Complete set of subcommands for iothub-device and iothub-service.
