// This is a working example to provision a device via the HTTP API
//
// Example run (expects you have a x509 certificate and private key for your device):
// go run provision.go -cert device-cert.pem -pkey private-key.pem -scopeID "0ne000XXXXX" -deviceID "mydevice-1"
//
package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"time"

	"go.uber.org/zap"
)

const (
	DpsProvisioningUrl        string = "https://global.azure-devices-provisioning.net"
	DpsProvisioningApiVersion string = "api-version=2019-03-31"
)

type ProvClient struct {
	c        http.Client
	scopeId  string
	deviceId string
	log      *zap.SugaredLogger
}

func NewProvClient(slog *zap.SugaredLogger, scopeID string, deviceId string, certFile string, privKeyFile string) (*ProvClient, error) {
	cert, err := tls.LoadX509KeyPair(certFile, privKeyFile)
	if err != nil {
		return nil, err
	}
	p := new(ProvClient)
	p.log = slog.Named("ProvClient")
	p.deviceId = deviceId
	p.scopeId = scopeID
	p.c = http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				Renegotiation: tls.RenegotiateFreelyAsClient,
				Certificates:  []tls.Certificate{cert},
			},
		},
	}
	return p, nil
}

type ProvReplyJSON struct {
	OpId   string `json:"operationId"`
	Status string `json:"status"`
}

type ProvStatusReply struct {
	OpId     string `json:"operationId"`
	Status   string `json:"status"`
	RegState struct {
		X509 struct {
			EnrollmentGroupId string `json:"enrollmentGroupId"`
		} `json:"x509"`
		RegId                  string `json:"registrationId"`
		CreatedDateTimeUtc     string `json:"createdDateTimeUtc"`
		AssignedHub            string `json:"assignedHub"`
		DeviceId               string `json:"deviceId"`
		Status                 string `json:"status"`
		Substatus              string `json:"substatus"`
		LastUpdatedDateTimeUtc string `json:"lastUpdatedDateTimeUtc"`
	} `json:"registrationState"`
}

func provisioningRequestBody(deviceId string) io.Reader {
	type reqBody struct {
		RegId string `json:"registrationId"`
	}

	// Create JSON for request body
	reqJSON, _ := json.Marshal(reqBody{
		RegId: deviceId,
	})

	return bytes.NewBuffer(reqJSON)
}

func (p *ProvClient) SendProvisioningRequest() (ProvReplyJSON, error) {
	url := fmt.Sprintf("%s/%s/registrations/%s/register?%s", DpsProvisioningUrl, p.scopeId, p.deviceId, DpsProvisioningApiVersion)
	p.log.Debugw("Provisioning Request", "url", url)

	var reply ProvReplyJSON

	// Create HTTP POST request
	req, err := http.NewRequest("PUT", url, provisioningRequestBody(p.deviceId))
	if err != nil {
		return reply, err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Encoding", "utf-8")

	reqDump, _ := httputil.DumpRequest(req, true)
	p.log.Infow("Request", "dump", string(reqDump))

	// Send request
	resp, err := p.c.Do(req)
	if err != nil {
		return reply, err
	}
	fmt.Printf("\n%+v\n", p.c)
	p.log.Debugw("Reply", "error", err, "status", resp.Status)
	if resp.StatusCode != 202 {
		return reply, fmt.Errorf("Got non 202 return code")
	}

	// Read response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return reply, err
	}

	json.Unmarshal(body, &reply)

	return reply, err
}

func (p *ProvClient) ProvisioningStatus(operationId string) (ProvStatusReply, error) {
	url := fmt.Sprintf("%s/%s/registrations/%s/operations/%s?%s", DpsProvisioningUrl, p.scopeId, p.deviceId, operationId, DpsProvisioningApiVersion)
	p.log.Debugw("Provisioning Status", "url", url)

	var reply ProvStatusReply

	// Create HTTP GET request
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return reply, err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Encoding", "utf-8")

	// Send request
	resp, err := p.c.Do(req)
	if err != nil {
		return reply, err
	}
	if resp.StatusCode != 200 {
		return reply, fmt.Errorf("Got non 200 return code")
	}

	// Read response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return reply, err
	}

	json.Unmarshal(body, &reply)

	return reply, err
}

func main() {
	var (
		optScopeID    string
		optDeviceID   string
		optCert       string
		optPrivateKey string
	)

	flag.StringVar(&optScopeID, "scopeID", "", "Scope ID")
	flag.StringVar(&optDeviceID, "deviceID", "", "Device ID in the cloud")
	flag.StringVar(&optCert, "cert", "cert.pem", "x509 Certificate")
	flag.StringVar(&optPrivateKey, "pkey", "private-key.pem", "Private Key")
	flag.Parse()

	slog := zap.NewExample().Sugar()
	defer slog.Sync()

	// Create client
	client, _ := NewProvClient(slog, optScopeID, optDeviceID, optCert, optPrivateKey)
	// Send provisioning request to DPS
	rep, _ := client.SendProvisioningRequest()
	// Check on provisioning status, give up after 10 seconds
	var count uint
	for count < 10 {
		if status, err := client.ProvisioningStatus(rep.OpId); err != nil {
			slog.Errorw("Failed to get provisioning status", "error", err)
		} else {
			slog.Infow("Provisioning Status", "reply", status)
			if status.Status == "assigned" {
				slog.Info("Success")
				break
			}
		}
		count++
		time.Sleep(time.Second)
	}
}
