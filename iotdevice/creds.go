package iotdevice

import (
	"context"
	"crypto/tls"
	"errors"
	"time"

	"github.com/goautomotive/iothub/common"
	"github.com/goautomotive/iothub/iotdevice/transport"
)

func NewSASCredentials(cs string) (transport.Credentials, error) {
	creds, err := common.ParseConnectionString(cs)
	if err != nil {
		return nil, err
	}
	return &sasCreds{creds: creds}, nil
}

type sasCreds struct {
	creds *common.Credentials
}

func (c *sasCreds) DeviceID() string {
	return c.creds.DeviceID
}

func (c *sasCreds) Hostname() string {
	return c.creds.HostName
}

func (c *sasCreds) IsSAS() bool {
	return true
}

func (c *sasCreds) TLSConfig() *tls.Config {
	return &tls.Config{
		ServerName: c.creds.HostName,
		RootCAs:    common.RootCAs(),
	}
}

func (c *sasCreds) Token(ctx context.Context, uri string, d time.Duration) (string, error) {
	return c.creds.SAS(uri, d)
}

func NewX509Credentials(deviceID, hostname string, crt *tls.Certificate) (transport.Credentials, error) {
	return &x509Creds{
		deviceID:    deviceID,
		hostname:    hostname,
		certificate: crt,
	}, nil
}

type x509Creds struct {
	deviceID    string
	hostname    string
	certificate *tls.Certificate
}

func (c *x509Creds) DeviceID() string {
	return c.deviceID
}

func (c *x509Creds) Hostname() string {
	return c.hostname
}

func (c *x509Creds) IsSAS() bool {
	return false
}

func (c *x509Creds) TLSConfig() *tls.Config {
	return &tls.Config{
		ServerName:   c.hostname,
		Certificates: []tls.Certificate{*c.certificate},
		RootCAs:      common.RootCAs(),
	}
}

func (c *x509Creds) Token(ctx context.Context, uri string, d time.Duration) (string, error) {
	return "", errors.New("not supported")
}
