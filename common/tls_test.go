package common

import (
	"crypto/tls"
	"net/http"
	"testing"
)

func TestTLSConfig(t *testing.T) {
	t.Parallel()

	r, err := http.NewRequest(http.MethodGet, "https://portal.azure.com", nil)
	if err != nil {
		t.Fatal(err)
	}

	c := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs: RootCAs(),
		},
	}}
	if _, err := c.Do(r); err != nil {
		t.Fatal(err)
	}
}
