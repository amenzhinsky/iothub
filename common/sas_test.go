package common

import (
	"testing"
	"time"
)

func TestParseConnectionString(t *testing.T) {
	t.Parallel()

	for s, w := range map[string]*Credentials{
		"HostName=test.azure-devices.net;DeviceId=devnull;SharedAccessKey=c2VjcmV0": {
			HostName:            "test.azure-devices.net",
			DeviceID:            "devnull",
			SharedAccessKey:     "c2VjcmV0",
			SharedAccessKeyName: "",
		},
		"HostName=test.azure-devices.net;SharedAccessKeyName=device;SharedAccessKey=c2VjcmV0": {
			HostName:            "test.azure-devices.net",
			DeviceID:            "",
			SharedAccessKey:     "c2VjcmV0",
			SharedAccessKeyName: "device",
		},
	} {
		g, err := ParseConnectionString(s)
		if err != nil {
			t.Fatal(err)
		}
		if *g != *w {
			t.Errorf("ParseConnectionString(%q) = %v, want %v", s, g, w)
		}
	}
}

func TestCredentials_SAS(t *testing.T) {
	t.Parallel()

	c, err := ParseConnectionString("HostName=test.azure-devices.net;DeviceId=devnull;SharedAccessKey=c2VjcmV0")
	if err != nil {
		t.Fatal(err)
	}
	c.now = time.Date(2017, 1, 1, 1, 1, 1, 0, time.UTC)

	g, err := c.SAS(c.HostName+"/devices/test", time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	w := "SharedAccessSignature sr=test.azure-devices.net%2Fdevices%2Ftest&sig=IMr3Y5GKbdixQSt96QgIEymAURnu3qzLvEHhGHPLxrU%3D&se=1483236061&skn="
	if g != w {
		t.Errorf("SAS(time.Hour) = %q, want %q", g, w)
	}
}
