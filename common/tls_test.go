package common

import (
	"net/http"
	"testing"
)

func TestTLSConfig(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "https://portal.azure.com", nil)
	if err != nil {
		t.Fatal(err)
	}

	c := &http.Client{Transport: &http.Transport{
		TLSClientConfig: TLSConfig("portal.azure.com"),
	}}
	if _, err := c.Do(r); err != nil {
		t.Fatal(err)
	}
}
