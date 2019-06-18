package iotdevice

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

func (c *X509Credentials) GetCertificate() *tls.Certificate {
	return c.Certificate
}

func (c *X509Credentials) Token(
	resource string, lifetime time.Duration,
) (*common.SharedAccessSignature, error) {
	return nil, errors.New("cannot generate SAS tokens with x509 credentials")
}

type SharedAccessKeyCredentials struct {
	DeviceID string
	common.SharedAccessKey
}

func (c *SharedAccessKeyCredentials) GetDeviceID() string {
	return c.DeviceID
}

func (c *SharedAccessKeyCredentials) GetHostName() string {
	return c.SharedAccessKey.HostName
}

func (c *SharedAccessKeyCredentials) GetCertificate() *tls.Certificate {
	return nil
}
