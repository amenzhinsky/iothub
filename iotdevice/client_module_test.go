package iotdevice

import (
	"context"
	"testing"
	"time"

	"github.com/amenzhinsky/iothub/iotdevice/iotdevicetest"
	"github.com/amenzhinsky/iothub/iotdevice/transport/mqtt"
	"github.com/amenzhinsky/iothub/iotservice"
)

func newModuleClient(t *testing.T, sc *iotservice.Client) *ModuleClient {
	t.Helper()

	device := iotdevicetest.NewDevice(t, sc)
	module := iotdevicetest.NewModule(t, sc, device.DeviceID)

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
	sc := iotdevicetest.NewServiceClient(t)
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
