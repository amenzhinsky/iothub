package common

import (
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
)

// DigiCert Baltimore Root (sha1 fingerprint=d4de20d05e66fc53fe1a50882c78db2852cae474) - remove post migration circa early 2023
// Microsoft RSA TLS CA 01 (sha1 fingerprint=703d7a8f0ebf55aaa59f98eaf4a206004eb2516a)
// Microsoft RSA TLS CA 02 (sha1 fingerprint=b0c2d2d13cdd56cdaa6ab6e2c04440be4a429c75)
// Microsoft Azure TLS Issuing CA 01 (sha1 fingerprint=2f2877c5d778c31e0f29c7e371df5471bd673173)
// Microsoft Azure TLS Issuing CA 02 (sha1 fingerprint=e7eea674ca718e3befd90858e09f8372ad0ae2aa)
// Microsoft Azure TLS Issuing CA 05 (sha1 fingerprint=6c3af02e7f269aa73afd0eff2a88a4a1f04ed1e5)
// Microsoft Azure TLS Issuing CA 06 (sha1 fingerprint=30e01761ab97e59a06b41ef20af6f2de7ef4f7b0)
// DigiCert Global Root G2 (sha1 fingerprint=df3c24f9bfd666761b268073fe06d1cc8d4f82a4)
// Microsoft RSA Root Certificate Authority 2017 (sha1 fingerprint=73a5e64a3bff8316ff0edccc618a906e4eae4d74)
var caCerts = []byte(`-----BEGIN CERTIFICATE-----
MIIDrzCCApegAwIBAgIQCDvgVpBCRrGhdWrJWZHHSjANBgkqhkiG9w0BAQUFADBh
MQswCQYDVQQGEwJVUzEVMBMGA1UEChMMRGlnaUNlcnQgSW5jMRkwFwYDVQQLExB3
d3cuZGlnaWNlcnQuY29tMSAwHgYDVQQDExdEaWdpQ2VydCBHbG9iYWwgUm9vdCBD
QTAeFw0wNjExMTAwMDAwMDBaFw0zMTExMTAwMDAwMDBaMGExCzAJBgNVBAYTAlVT
MRUwEwYDVQQKEwxEaWdpQ2VydCBJbmMxGTAXBgNVBAsTEHd3dy5kaWdpY2VydC5j
b20xIDAeBgNVBAMTF0RpZ2lDZXJ0IEdsb2JhbCBSb290IENBMIIBIjANBgkqhkiG
9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4jvhEXLeqKTTo1eqUKKPC3eQyaKl7hLOllsB
CSDMAZOnTjC3U/dDxGkAV53ijSLdhwZAAIEJzs4bg7/fzTtxRuLWZscFs3YnFo97
nh6Vfe63SKMI2tavegw5BmV/Sl0fvBf4q77uKNd0f3p4mVmFaG5cIzJLv07A6Fpt
43C/dxC//AH2hdmoRBBYMql1GNXRor5H4idq9Joz+EkIYIvUX7Q6hL+hqkpMfT7P
T19sdl6gSzeRntwi5m3OFBqOasv+zbMUZBfHWymeMr/y7vrTC0LUq7dBMtoM1O/4
gdW7jVg/tRvoSSiicNoxBN33shbyTApOB6jtSj1etX+jkMOvJwIDAQABo2MwYTAO
BgNVHQ8BAf8EBAMCAYYwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUA95QNVbR
TLtm8KPiGxvDl7I90VUwHwYDVR0jBBgwFoAUA95QNVbRTLtm8KPiGxvDl7I90VUw
DQYJKoZIhvcNAQEFBQADggEBAMucN6pIExIK+t1EnE9SsPTfrgT1eXkIoyQY/Esr
hMAtudXH/vTBH1jLuG2cenTnmCmrEbXjcKChzUyImZOMkXDiqw8cvpOp/2PV5Adg
06O/nVsJ8dWO41P0jmP6P6fbtGbfYmbW0W5BjfIttep3Sp+dWOIrWcBAI+0tKIJF
PnlUkiaY4IBIqDfv8NZ5YBberOgOzW6sRBc4L0na4UU+Krk2U886UAb3LujEV0ls
YSEY1QSteDwsOoBrp+uvFRTp2InBuThs4pFsiv9kuXclVzDAGySj4dzp30d8tbQk
CAUw7C29C79Fv1C5qfPrmAESrciIxpg0X40KPMbp1ZWVbd4=
-----END CERTIFICATE-----
`)

// RootCAs root CA certificates pool for connecting to the cloud.
func RootCAs() *x509.CertPool {
	p := x509.NewCertPool()
	if ok := p.AppendCertsFromPEM(caCerts); !ok {
		panic("tls: unable to append certificates")
	}
	return p
}

// TrustBundleResponse aids parsing the response from the edge.
type TrustBundleResponse struct {
	Certificate string `json:"certificate"`
}

// TrustBundle root CA certificates pool for connecting to EdgeHub Gateway.
func TrustBundle(workloadURI string) (*x509.CertPool, error) {
	tbr := TrustBundleResponse{}
	var err error

	// catch unix domain sockets URIs
	if strings.Contains(workloadURI, "unix://") {
		addr, err := net.ResolveUnixAddr("unix", strings.TrimPrefix(workloadURI, "unix://"))
		if err != nil {
			fmt.Printf("Failed to resolve: %v\n", err)
			return nil, err
		}

		setSharedUnixHTTPClient(addr.Name)
		response, err := sharedUnixHTTPClient.Get("http://iotedge" + "/trust-bundle?api-version=2019-11-05")
		if err != nil {
			return nil, fmt.Errorf("tls: unable to append certificates: %w", err)
		}
		defer response.Body.Close()

		body, err := io.ReadAll(response.Body)
		if err != nil {
			return nil, fmt.Errorf("tls: unable to append certificates: %w", err)
		}

		err = json.Unmarshal(body, &tbr)
		if err != nil {
			return nil, fmt.Errorf("tls: unable to append certificates: %w", err)
		}
	} else {
		// format uri string
		uri := fmt.Sprintf("%strust-bundle?api-version=2019-11-05", workloadURI)

		// get http response and handle error
		resp, err := http.Get(uri)
		if err != nil {
			return nil, fmt.Errorf("tls: unable to append certificates: %w", err)
		}
		defer resp.Body.Close()

		// read response
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("tls: unable to append certificates: %w", err)
		}

		err = json.Unmarshal(body, &tbr)
		if err != nil {
			return nil, fmt.Errorf("tls: unable to append certificates: %w", err)
		}
	}

	p := x509.NewCertPool()
	if ok := p.AppendCertsFromPEM([]byte(tbr.Certificate)); !ok {
		err = fmt.Errorf("tls: unable to append certificates: count not append certs to pool")
	}

	return p, err
}
