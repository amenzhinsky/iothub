package credentials

import (
	"testing"
	"time"
)

func TestParseConnectionString(t *testing.T) {
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

func TestCredentials_GenerateToken(t *testing.T) {
	c, err := ParseConnectionString("HostName=test.azure-devices.net;DeviceId=devnull;SharedAccessKey=c2VjcmV0")
	if err != nil {
		t.Fatal(err)
	}

	g, err := c.GenerateToken(c.HostName+"/devices/test",
		WithDuration(time.Hour),
		WithCurrentTime(time.Date(2017, 1, 1, 1, 1, 1, 0, time.UTC)),
	)
	if err != nil {
		t.Fatal(err)
	}

	w := "SharedAccessSignature sr=test.azure-devices.net%2Fdevices%2Ftest&sig=IMr3Y5GKbdixQSt96QgIEymAURnu3qzLvEHhGHPLxrU%3D&se=1483236061&skn="
	if g != w {
		t.Errorf("GenerateToken(time.Hour) = %q, want %q", g, w)
	}
}
