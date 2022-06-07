package http

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/amenzhinsky/iothub/iotdevice"
	"github.com/amenzhinsky/iothub/iotservice"
)

var testRunID = strconv.Itoa(int(time.Now().Unix()))

func newClient(t *testing.T) *iotservice.Client {
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

func newTransport(t *testing.T) *Transport {
	t.Helper()

	sc := newClient(t)
	device := newDevice(t, sc)

	dcs, err := sc.DeviceConnectionString(device, false)
	if err != nil {
		t.Fatal(err)
	}

	creds, err := iotdevice.ParseConnectionString(dcs)
	if err != nil {
		t.Fatal(err)
	}
	tr := New()
	if err := tr.Connect(context.Background(), creds); err != nil {
		t.Fatal(err)
	}

	return tr
}

func newModule(t *testing.T, tr *Transport) *iotservice.Module {
	module := &iotservice.Module{
		DeviceID:  tr.creds.GetDeviceID(),
		ModuleID:  "test-module-" + testRunID,
		ManagedBy: "admin",
	}
	module, err := tr.CreateModule(context.Background(), module)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		module.ETag = ""
		if err := tr.DeleteModule(context.Background(), module); err != nil {
			t.Fatal(err)
		}
	})
	return module
}

func TestListModules(t *testing.T) {
	tr := newTransport(t)
	module := newModule(t, tr)
	modules, err := tr.ListModules(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(modules) != 1 {
		t.Errorf("module count = %d, want 1", len(modules))
	}

	if modules[0].ModuleID != module.ModuleID {
		t.Errorf("moduleID = %s, want %s", modules[0].ModuleID, module.ModuleID)
	}
}

func TestGetModule(t *testing.T) {
	tr := newTransport(t)
	module := newModule(t, tr)
	if _, err := tr.GetModule(
		context.Background(), module.ModuleID,
	); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateModule(t *testing.T) {
	tr := newTransport(t)
	module := newModule(t, tr)
	module.Authentication.Type = iotservice.AuthSAS
	updatedModule, err := tr.UpdateModule(context.Background(), module)
	if err != nil {
		t.Fatal(err)
	}

	if updatedModule.Authentication.Type != iotservice.AuthSAS {
		t.Errorf("authentication type = `%s`, want `%s`", updatedModule.Authentication.Type, iotservice.AuthSAS)
	}
}
