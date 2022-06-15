package common

import (
	"crypto/tls"
	"net/http"
	"testing"
)

func TestRootCAs(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "https://portal.azure.com", nil)
	if err != nil {
		t.Fatal(err)
	}

	c := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			ServerName: "portal.azure.com",
			RootCAs:    RootCAs(),
		},
	}}
	res, err := c.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
}

func TestNewRootCAs(t *testing.T) {
	// See validation steps here:
	// https://techcommunity.microsoft.com/t5/internet-of-things-blog/azure-iot-tls-critical-changes-are-almost-here-and-why-you/ba-p/2393169

	r, err := http.NewRequest(http.MethodGet, "https://g2cert.azure-devices.net", nil)
	if err != nil {
		t.Fatal(err)
	}

	c := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			ServerName: "g2cert.azure-devices.net",
			RootCAs:    RootCAs(),
		},
	}}
	res, err := c.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
}
