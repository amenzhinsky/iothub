package common

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
		switch c[0] {
		case "HostName":
			m.HostName = c[1]
		case "DeviceId":
			m.DeviceID = c[1]
		case "SharedAccessKey":
			m.SharedAccessKey = c[1]
		case "SharedAccessKeyName":
			m.SharedAccessKeyName = c[1]
		}
	}
	return m, nil
}

// Credentials contains all the required credentials
// to access iothub from a device's prospective.
type Credentials struct {
	HostName            string
	DeviceID            string
	SharedAccessKey     string
	SharedAccessKeyName string

	// needed for testing
	now time.Time
}

// SAS generates an access token for the given uri and duration.
func (c *Credentials) SAS(uri string, duration time.Duration) (string, error) {
	if uri == "" {
		return "", errors.New("uri is blank")
	}
	if duration == 0 {
		return "", errors.New("duration is zero")
	}
	if c.SharedAccessKey == "" {
		return "", errors.New("SharedAccessKey is blank")
	}

	sr := url.QueryEscape(uri)
	ts := time.Now()
	if !c.now.IsZero() {
		ts = c.now
	}
	se := ts.Add(duration).Unix()

	b, err := base64.StdEncoding.DecodeString(c.SharedAccessKey)
	if err != nil {
		return "", err
	}

	// generate signature from uri and expiration time.
	e := fmt.Sprintf("%s\n%d", sr, se)
	h := hmac.New(sha256.New, b)
	_, err = h.Write([]byte(e))
	if err != nil {
		return "", err
	}

	return "SharedAccessSignature " +
		"sr=" + sr +
		"&sig=" + url.QueryEscape(base64.StdEncoding.EncodeToString(h.Sum(nil))) +
		"&se=" + url.QueryEscape(strconv.FormatInt(se, 10)) +
		"&skn=" + url.QueryEscape(c.SharedAccessKeyName), nil
}
