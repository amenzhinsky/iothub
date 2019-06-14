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
	res, err := c.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
}
