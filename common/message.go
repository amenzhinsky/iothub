package common

import (
	"bytes"
	"fmt"
	"sort"
	"time"
	"unicode"
)

// Message is a common message format for all device-facing protocols.
// This message format is used for both device-to-cloud and cloud-to-device messages.
// See: https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-messages-construct
type Message struct {
	// MessageID is a user-settable identifier for the message used for request-reply patterns.
	MessageID string `json:"MessageId,omitempty"`

	// To is a destination specified in cloud-to-device messages.
	To string `json:"To,omitempty"`

	// ExpiryTime is time of message expiration.
	ExpiryTime time.Time `json:"ExpiryTimeUtc,omitempty"`

	// EnqueuedTime is time the Cloud-to-Device message was received by IoT Hub.
	EnqueuedTime time.Time `json:"EnqueuedTime,omitempty"`

	// CorrelationID is a string property in a response message that typically
	// contains the MessageId of the request, in request-reply patterns.
	CorrelationID string `json:"CorrelationId,omitempty"`

	// UserID is an ID used to specify the origin of messages.
	UserID string `json:"UserId,omitempty"`

	// ConnectionDeviceID is an ID set by IoT Hub on device-to-cloud messages.
	// It contains the deviceId of the device that sent the message.
	ConnectionDeviceID string `json:"ConnectionDeviceId,omitempty"`

	// ConnectionDeviceGenerationID is an ID set by IoT Hub on device-to-cloud messages.
	// It contains the generationId (as per Device identity properties)
	// of the device that sent the message.
	ConnectionDeviceGenerationID string `json:"ConnectionDeviceGenerationId,omitempty"`

	// ConnectionAuthMethod is an authentication method set by IoT Hub on
	// device-to-cloud messages. This property contains information about
	// the authentication method used to authenticate the device sending the message.
	ConnectionAuthMethod string `json:"ConnectionAuthMethod,omitempty"`

	// MessageSource determines a device-to-cloud message transport.
	MessageSource string `json:"MessageSource,omitempty"`

	// Payload is arbitrary data.
	Payload []byte

	// Properties are custom message properties (property bags).
	Properties map[string]string `json:"Properties,omitempty"`

	// TransportOptions transport specific options.
	TransportOptions map[string]interface{}
}

// Inspect is a human-readable message format.
func (msg *Message) Inspect() string {
	b := &bytes.Buffer{} // TODO: strings.Builder
	b.WriteString("--- PAYLOAD -------------\n")
	if len(msg.Payload) > 0 {
		b.WriteString(fmtPayload(msg.Payload))
	} else {
		b.WriteString("[empty]")
	}
	b.WriteString("\n--- PROPERTIES ----------\n")
	if len(msg.Properties) > 0 {
		b.WriteString(fmtProps(msg.Properties))
	} else {
		b.WriteString("[empty]")
	}
	b.WriteString("\n--- METADATA ------------\n")

	l := msg.mlen()
	f := func(k, v string) string {
		return fmt.Sprintf("%-"+fmt.Sprint(l)+"s : %s\n", k, v)
	}

	if msg.MessageID != "" {
		b.WriteString(f("MessageID", msg.MessageID))
	}
	if msg.To != "" {
		b.WriteString(f("To", msg.To))
	}
	if !msg.ExpiryTime.IsZero() {
		b.WriteString(f("ExpiryTime", msg.ExpiryTime.String()))
	}
	if !msg.EnqueuedTime.IsZero() {
		b.WriteString(f("EnqueuedTime", msg.EnqueuedTime.String()))
	}
	if msg.CorrelationID != "" {
		b.WriteString(f("CorrelationID", msg.CorrelationID))
	}
	if msg.UserID != "" {
		b.WriteString(f("UserID", msg.UserID))
	}
	if msg.ConnectionDeviceID != "" {
		b.WriteString(f("ConnectionDeviceID", msg.ConnectionDeviceID))
	}
	if msg.ConnectionDeviceGenerationID != "" {
		b.WriteString(f("ConnectionDeviceGenerationID", msg.ConnectionDeviceGenerationID))
	}
	if msg.ConnectionAuthMethod != "" {
		b.WriteString(f("ConnectionAuthMethod", msg.ConnectionAuthMethod))
	}
	if msg.MessageSource != "" {
		b.WriteString(f("MessageSource", msg.MessageSource))
	}
	b.WriteString("=========================")

	return b.String()
}

func (msg *Message) mlen() int {
	lenIfBigger := func(c int, s string, isZero bool) int {
		if isZero {
			return c
		}
		if len(s) > c {
			c = len(s)
		}
		return c
	}

	l := 0
	l = lenIfBigger(l, "MessageID", msg.MessageID == "")
	l = lenIfBigger(l, "To", msg.To == "")
	l = lenIfBigger(l, "ExpiryTime", msg.ExpiryTime.IsZero())
	l = lenIfBigger(l, "EnqueuedTime", msg.EnqueuedTime.IsZero())
	l = lenIfBigger(l, "CorrelationID", msg.CorrelationID == "")
	l = lenIfBigger(l, "UserID", msg.UserID == "")
	l = lenIfBigger(l, "ConnectionDeviceID", msg.ConnectionDeviceID == "")
	l = lenIfBigger(l, "ConnectionDeviceGenerationID", msg.ConnectionDeviceGenerationID == "")
	l = lenIfBigger(l, "ConnectionAuthMethod", msg.ConnectionAuthMethod == "")
	l = lenIfBigger(l, "MessageSource", msg.MessageSource == "")

	return l
}

func fmtPayload(b []byte) string {
	for _, r := range string(b) {
		if !unicode.IsPrint(r) {
			return fmt.Sprintf("[% x]", string(b))
		}
	}
	return string(b)
}

func fmtProps(m map[string]string) string {
	p := 0
	b := &bytes.Buffer{} // TODO: strings.Builder
	o := make([]string, 0, len(m))
	for k := range m {
		if p < len(k) {
			p = len(k)
		}
		o = append(o, k)
	}
	sort.Strings(o)
	for i, k := range o {
		if i != 0 {
			b.WriteByte('\n')
		}
		b.WriteString(fmt.Sprintf("%-"+fmt.Sprint(p)+"s : %s", k, fmtPayload([]byte(m[k]))))
	}
	return b.String()
}
