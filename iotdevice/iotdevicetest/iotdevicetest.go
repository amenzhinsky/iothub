// Package iotdevicetest provides utilities for iotdevice testing.
package iotdevicetest

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/amenzhinsky/iothub/iotservice"
)

var testRunID = GenerateRandomID()

func GenerateRandomID() string {
	seed := rand.NewSource(time.Now().UnixNano())
	r := rand.New(seed)
	return strconv.FormatInt(r.Int63(), 10)
}

// NewServiceClient creates a service client based on the `TEST_IOTHUB_SERVICE_CONNECTION_STRING` connection string.
func NewServiceClient(t *testing.T) *iotservice.Client {
	t.Helper()
	cs := os.Getenv("TEST_IOTHUB_SERVICE_CONNECTION_STRING")
	if cs == "" {
		t.Fatal("$TEST_IOTHUB_SERVICE_CONNECTION_STRING is empty")
	}
	c, err := iotservice.NewFromConnectionString(cs)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// NewDevice creates a new test device and deletes it when the tests are done.
func NewDevice(t *testing.T, c *iotservice.Client) *iotservice.Device {
	t.Helper()

	device := &iotservice.Device{
		DeviceID: "test-device-" + testRunID,
	}
	device, err := c.CreateDevice(context.Background(), device)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		device.ETag = ""
		if err := c.DeleteDevice(context.Background(), device); err != nil {
			t.Fatal(err)
		}
	})
	return device
}

// NewModule creates a new test module, sets up SAS authentication and deletes it when the tests are done.
func NewModule(t *testing.T, c *iotservice.Client, deviceID string) *iotservice.Module {
	module := &iotservice.Module{
		DeviceID:  deviceID,
		ModuleID:  "test-module-" + testRunID,
		ManagedBy: "admin",
	}
	module, err := c.CreateModule(context.Background(), module)
	if err != nil {
		t.Fatal(err)
	}
	module.Authentication.Type = iotservice.AuthSAS
	module, err = c.UpdateModule(context.Background(), module)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		module.ETag = ""
		if err := c.DeleteModule(context.Background(), module); err != nil {
			t.Fatal(err)
		}
	})
	return module
}
