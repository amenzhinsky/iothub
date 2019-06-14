package credentials

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ParseConnectionString parses the given string into a Credentials struct.
// If you use a shared access policy DeviceId is needed to be added manually.
func ParseConnectionString(cs string) (*Credentials, error) {
	chunks := strings.Split(cs, ";")
	if len(chunks) != 3 && len(chunks) != 4 {
		return nil, errors.New("malformed connection string")
	}

	m := &Credentials{}
	for _, chunk := range chunks {
		c := strings.SplitN(chunk, "=", 2)
		if len(c) != 2 {
			return nil, errors.New("malformed connection string")
		}

		switch c[0] {
		case "HostName":
			m.HostName = c[1]
		case "DeviceId":
			m.DeviceID = c[1]
		case "ModuleId":
			m.ModuleID = c[1]
		case "SharedAccessKey":
			m.SharedAccessKey = c[1]
		case "SharedAccessKeyName":
			m.SharedAccessKeyName = c[1]
		}
	}
	return m, nil
}

// Credentials is a IoT Hub authorization entity.
type Credentials struct {
	HostName            string
	DeviceID            string
	ModuleID            string
	SharedAccessKey     string
	SharedAccessKeyName string
}

type options struct {
	duration time.Duration
	time     time.Time
}

// TokenOption is token generation option.
type TokenOption func(opts *options)

// WithDuration sets token duration.
func WithDuration(d time.Duration) TokenOption {
	return func(opts *options) {
		opts.duration = d
	}
}

// WithCurrentTime overrides current time clock.
func WithCurrentTime(t time.Time) TokenOption {
	return func(opts *options) {
		opts.time = t
	}
}

// GenerateToken generates a SAS token for the given uri.
//
// Default token duration is one hour.
func (c *Credentials) GenerateToken(uri string, opts ...TokenOption) (string, error) {
	if uri == "" {
		return "", errors.New("uri is blank")
	}
	if c.SharedAccessKey == "" {
		return "", errors.New("SharedAccessKey is blank")
	}

	topts := &options{
		duration: time.Hour,
		time:     time.Now(),
	}
	for _, opt := range opts {
		opt(topts)
	}

	sr := url.QueryEscape(uri)
	se := topts.time.Add(topts.duration).Unix()

	b, err := base64.StdEncoding.DecodeString(c.SharedAccessKey)
	if err != nil {
		return "", err
	}

	// generate signature from uri and expiration time.
	e := fmt.Sprintf("%s\n%d", sr, se)
	h := hmac.New(sha256.New, b)
	if _, err = h.Write([]byte(e)); err != nil {
		return "", err
	}

	return "SharedAccessSignature " +
		"sr=" + sr +
		"&sig=" + url.QueryEscape(base64.StdEncoding.EncodeToString(h.Sum(nil))) +
		"&se=" + url.QueryEscape(strconv.FormatInt(se, 10)) +
		"&skn=" + url.QueryEscape(c.SharedAccessKeyName), nil
}
