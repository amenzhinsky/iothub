package iotdevice

import (
	"context"
	"testing"

	"github.com/amenzhinsky/iothub/iotdevice/iotdevicetest"
	"github.com/amenzhinsky/iothub/iotdevice/transport/http"
	"github.com/amenzhinsky/iothub/iotservice"
)

func newDeviceClient(t *testing.T) *Client {
	t.Helper()
	sc := iotdevicetest.NewServiceClient(t)
	device := iotdevicetest.NewDevice(t, sc)

	dcs, err := sc.DeviceConnectionString(device, false)
	if err != nil {
		t.Fatal(err)
	}

	dc, err := NewFromConnectionString(http.New(), dcs)
	if err != nil {
		t.Fatal(err)
	}

	if err := dc.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	return dc
}

// newModule creates a module using the device client
func newModule(t *testing.T, c *Client) *iotservice.Module {
	testRunID := iotdevicetest.GenerateRandomID()
	module := &iotservice.Module{
		DeviceID:  c.DeviceID(),
		ModuleID:  "test-module-" + testRunID,
		ManagedBy: "admin",
	}
	module, err := c.CreateModule(context.Background(), module)
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

func TestListModules(t *testing.T) {
	c := newDeviceClient(t)
	module := newModule(t, c)
	modules, err := c.ListModules(context.Background())
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
	c := newDeviceClient(t)
	module := newModule(t, c)
	if _, err := c.GetModule(
		context.Background(), module.ModuleID,
	); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateModule(t *testing.T) {
	c := newDeviceClient(t)
	module := newModule(t, c)
	module.Authentication.Type = iotservice.AuthSAS
	updatedModule, err := c.UpdateModule(context.Background(), module)
	if err != nil {
		t.Fatal(err)
	}

	if updatedModule.Authentication.Type != iotservice.AuthSAS {
		t.Errorf("authentication type = `%s`, want `%s`", updatedModule.Authentication.Type, iotservice.AuthSAS)
	}
}
