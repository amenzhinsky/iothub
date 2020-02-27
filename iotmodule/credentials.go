package iotmodule

import (
	"crypto/tls"
	"errors"
	"time"

	"github.com/amenzhinsky/iothub/common"
)

type X509Credentials struct {
	HostName    string
	DeviceID    string
	Certificate *tls.Certificate
}

func (c *X509Credentials) GetDeviceID() string {
	return c.DeviceID
}

func (c *X509Credentials) GetHostName() string {
	return c.HostName
}

func (c *X509Credentials) GetModuleID() string {
	return ""
}

func (c *X509Credentials) GetGenerationID() string {
	return ""
}

func (c *X509Credentials) GetGateway() string {
	return ""
}

func (c *X509Credentials) UseEdgeGateway() bool {
	return false
}

func (c *X509Credentials) GetCertificate() *tls.Certificate {
	return c.Certificate
}

func (c *X509Credentials) GetBroker() string {
	output := c.GetHostName()
	gw := c.GetGateway()
	if len(gw) > 0 {
		output = gw
	}
	return output
}

func (c *X509Credentials) GetSAK() string {
	return ""
}

func (c *X509Credentials) Token(
	resource string, lifetime time.Duration,
) (*common.SharedAccessSignature, error) {
	return nil, errors.New("cannot generate SAS tokens with x509 credentials")
}

type SharedAccessKeyCredentials struct {
	DeviceID     string
	ModuleID     string
	Gateway      string
	GenerationID string
	EdgeGateway  bool
	common.SharedAccessKey
}

func (c *SharedAccessKeyCredentials) GetDeviceID() string {
	return c.DeviceID
}

func (c *SharedAccessKeyCredentials) GetModuleID() string {
	return c.ModuleID
}

func (c *SharedAccessKeyCredentials) GetGenerationID() string {
	return c.GenerationID
}

func (c *SharedAccessKeyCredentials) GetGateway() string {
	return c.Gateway
}

func (c *SharedAccessKeyCredentials) UseEdgeGateway() bool {
	return c.EdgeGateway
}

func (c *SharedAccessKeyCredentials) GetHostName() string {
	return c.SharedAccessKey.HostName
}

func (c *SharedAccessKeyCredentials) GetBroker() string {
	output := c.GetHostName()
	gw := c.GetGateway()
	usegw := c.UseEdgeGateway()
	if usegw && len(gw) > 0 {
		output = gw
	}
	return output
}

func (c *SharedAccessKeyCredentials) GetCertificate() *tls.Certificate {
	return nil
}

func (c *SharedAccessKeyCredentials) GetSAK() string {
	return c.SharedAccessKey.SharedAccessKey
}
