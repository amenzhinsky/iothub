package iotdevice

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/amenzhinsky/iothub/iotdevice/transport/mqtt"
	"github.com/amenzhinsky/iothub/iotservice"
)

var testRunID = strconv.Itoa(int(time.Now().Unix()))

func newServiceClient(t *testing.T) *iotservice.Client {
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

func newDevice(t *testing.T, c *iotservice.Client) *iotservice.Device {
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

func newModule(t *testing.T, c *iotservice.Client, deviceID string) *iotservice.Module {
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

func newModuleClient(t *testing.T, sc *iotservice.Client) *ModuleClient {
	t.Helper()

	device := newDevice(t, sc)
	module := newModule(t, sc, device.DeviceID)

	mcs, err := sc.ModuleConnectionString(module, false)
	if err != nil {
		t.Fatal(err)
	}

	creds, err := ParseModuleConnectionString(mcs)
	if err != nil {
		t.Fatal(err)
	}
	tr := mqtt.NewModuleTransport()
	c, err := NewModule(tr, creds)
	if err != nil {
		t.Fatal(err)
	}

	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	return c
}

func TestSubscribeTwinUpdates(t *testing.T) {
	sc := newServiceClient(t)
	mc := newModuleClient(t, sc)

	twinStateSub, err := mc.SubscribeTwinUpdates(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	twin, err := sc.UpdateModuleTwin(context.Background(), &iotservice.ModuleTwin{
		DeviceID: mc.creds.GetDeviceID(),
		ModuleID: mc.creds.GetModuleID(),
		Properties: &iotservice.Properties{
			Desired: map[string]interface{}{
				"hw": "1.12",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	stateChan := twinStateSub.C()
	select {
	case ret := <-stateChan:
		for k, v := range twin.Properties.Desired {
			if k == "$metadata" {
				continue
			}
			retVal, ok := ret[k]
			if !ok {
				t.Errorf("twin desired property %s not received", k)
			}
			if retVal != v {
				t.Errorf("twin desired property %s = %s, want %s", k, retVal, v)
			}
		}
	case <-time.After(1 * time.Second):
		t.Error("twin update not received")
	}
}
