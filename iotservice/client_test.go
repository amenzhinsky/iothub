package iotservice

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/amenzhinsky/iothub/credentials"
)

func TestListDevices(t *testing.T) {
	client := newClient(t)
	device := newDevice(t, client)
	devices, err := client.ListDevices(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, dev := range devices {
		if dev.DeviceID == device.DeviceID {
			return
		}
	}
	t.Fatal("device not found", device)
}

func TestGetDevice(t *testing.T) {
	client := newClient(t)
	device := newDevice(t, client)
	if _, err := client.GetDevice(context.Background(), device.DeviceID); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateDevice(t *testing.T) {
	client := newClient(t)
	device := newDevice(t, client)
	device.Status = Disabled
	dev, err := client.UpdateDevice(context.Background(), device)
	if err != nil {
		t.Fatal(err)
	}
	if dev.Status != device.Status {
		t.Fatal("device is not updated")
	}
}

func TestDeleteDevice(t *testing.T) {
	client := newClient(t)
	device := newDevice(t, client)
	if err := client.DeleteDevice(context.Background(), device); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetDevice(context.Background(), device.DeviceID); err == nil {
		t.Fatal("device found but should be removed")
	}
}

func TestDeviceConnectionString(t *testing.T) {
	client := newClient(t)
	device := newDevice(t, client)
	cs, err := client.DeviceConnectionString(device, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := credentials.ParseConnectionString(cs); err != nil {
		t.Fatal(err)
	}
}

func TestDeviceSAS(t *testing.T) {
	client := newClient(t)
	device := newDevice(t, client)
	sas, err := client.DeviceSAS(device, time.Hour, false)
	if err != nil {
		t.Fatal(err)
	}
	if sas == "" {
		t.Fatal("empty sas token")
	}
}

func TestGetDeviceTwin(t *testing.T) {
	client := newClient(t)
	device := newDevice(t, client)
	_, err := client.GetDeviceTwin(context.Background(), device.DeviceID)
	if err != nil {
		t.Fatal(err)
	}
}

func TestUpdateDeviceTwin(t *testing.T) {
	client := newClient(t)
	device := newDevice(t, client)
	twin, err := client.UpdateDeviceTwin(context.Background(), &Twin{
		DeviceID: device.DeviceID,
		Properties: &Properties{
			Desired: map[string]interface{}{
				"hw": "1.11",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if twin.Properties.Desired["hw"] != "1.11" {
		t.Fatal("twin not updated")
	}
}

func TestModuleConnectionString(t *testing.T) {
	client := newClient(t)
	_, module := newDeviceAndModule(t, client)
	cs, err := client.ModuleConnectionString(module, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := credentials.ParseConnectionString(cs); err != nil {
		t.Fatal(err)
	}
}

func TestListModules(t *testing.T) {
	client := newClient(t)
	device, module := newDeviceAndModule(t, client)
	modules, err := client.ListModules(context.Background(), device.DeviceID)
	if err != nil {
		t.Fatal(err)
	}
	for _, mod := range modules {
		if mod.DeviceID == device.DeviceID && mod.ModuleID == module.ModuleID {
			return
		}
	}
	t.Fatal("module not found", device)
}

func TestGetModule(t *testing.T) {
	client := newClient(t)
	device, module := newDeviceAndModule(t, client)
	if _, err := client.GetModule(
		context.Background(), device.DeviceID, module.ModuleID,
	); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteModule(t *testing.T) {
	client := newClient(t)
	device, module := newDeviceAndModule(t, client)
	if err := client.DeleteModule(context.Background(), module); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetModule(
		context.Background(), device.DeviceID, module.ModuleID,
	); err == nil {
		t.Fatal("module is not deleted")
	}
}

func TestGetModuleTwin(t *testing.T) {
	client := newClient(t)
	device, module := newDeviceAndModule(t, client)
	if _, err := client.GetModuleTwin(
		context.Background(), device.DeviceID, module.ModuleID,
	); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateModuleTwin(t *testing.T) {
	client := newClient(t)
	device, module := newDeviceAndModule(t, client)
	twin, err := client.UpdateModuleTwin(context.Background(), &ModuleTwin{
		DeviceID: device.DeviceID,
		ModuleID: module.ModuleID,
		Properties: &Properties{
			Desired: map[string]interface{}{
				"hw": "1.12",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if twin.Properties.Desired["hw"] != "1.12" {
		t.Fatal("twin not updated")
	}
}

func TestListConfigurations(t *testing.T) {
	client := newClient(t)
	config := newConfiguration(t, client)
	configs, err := client.ListConfigurations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, cfg := range configs {
		if cfg.ID == config.ID {
			return
		}
	}
	t.Fatal("configuration not found in the list")
}

func TestGetConfiguration(t *testing.T) {
	client := newClient(t)
	config := newConfiguration(t, client)
	if _, err := client.GetConfiguration(context.Background(), config.ID); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateConfiguration(t *testing.T) {
	client := newClient(t)
	config := newConfiguration(t, client)
	config.Labels = map[string]string{
		"foo": "bar",
	}
	if _, err := client.UpdateConfiguration(context.Background(), config); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteConfiguration(t *testing.T) {
	client := newClient(t)
	config := newConfiguration(t, client)
	if err := client.DeleteConfiguration(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetConfiguration(context.Background(), config.ID); err == nil {
		t.Fatal("configuration is not deleted")
	}
}

func TestStats(t *testing.T) {
	client := newClient(t)
	if _, err := client.Stats(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestQuery(t *testing.T) {
	client := newClient(t)
	device := newDevice(t, client)

	var found bool
	if err := client.QueryDevices(
		context.Background(),
		&Query{
			Query:    "select deviceId from devices",
			PageSize: 1,
		},
		func(v map[string]interface{}) error {
			if v["deviceId"].(string) == device.DeviceID {
				found = true
			}
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("requested device not found")
	}
}

func newClient(t *testing.T) *Client {
	t.Helper()
	cs := os.Getenv("TEST_IOTHUB_SERVICE_CONNECTION_STRING")
	if cs == "" {
		t.Fatal("$TEST_IOTHUB_SERVICE_CONNECTION_STRING is empty")
	}
	c, err := New(WithConnectionString(cs))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func newDevice(t *testing.T, c *Client) *Device {
	t.Helper()
	device := &Device{
		DeviceID: "golang-iothub-test",
	}
	_ = c.DeleteDevice(context.Background(), device)
	device, err := c.CreateDevice(context.Background(), device)
	if err != nil {
		t.Fatal(err)
	}
	return device
}

func newDeviceAndModule(t *testing.T, c *Client) (*Device, *Module) {
	t.Helper()
	device := newDevice(t, c)
	module := &Module{
		DeviceID:  device.DeviceID,
		ModuleID:  "golang-iothub-test",
		ManagedBy: "admin",
	}
	_ = c.DeleteModule(context.Background(), module)
	module, err := c.CreateModule(context.Background(), module)
	if err != nil {
		t.Fatal(err)
	}
	return device, module
}

func newConfiguration(t *testing.T, c *Client) *Configuration {
	t.Helper()
	config := &Configuration{
		ID:              "golang-iothub-test",
		Priority:        10,
		SchemaVersion:   "1.0",
		TargetCondition: "deviceId='golang-iothub-test'",
		Labels: map[string]string{
			"test": "test",
		},
		Content: &ConfigurationContent{
			DeviceContent: map[string]interface{}{
				"properties.desired.testconf": 1.12,
			},
		},
		Metrics: &ConfigurationMetrics{
			Queries: map[string]string{
				"Total": "select deviceId from devices",
			},
		},
	}
	_ = c.DeleteConfiguration(context.Background(), config)
	config, err := c.CreateConfiguration(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	return config
}
