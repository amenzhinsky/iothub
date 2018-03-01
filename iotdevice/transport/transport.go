package transport

import (
	"context"
	"crypto/tls"
)

// Transport interface.
type Transport interface {
	Connect(
		ctx context.Context, tlsConfig *tls.Config, deviceID string, auth AuthFunc,
	) (c2ds chan *Event, dmis chan *Invocation, tscs chan *TwinState, err error)

	IsNetworkError(err error) bool
	PublishEvent(ctx context.Context, event *Event) error
	RespondDirectMethod(ctx context.Context, rid string, code int, payload []byte) error
	RetrieveTwinProperties(ctx context.Context) (payload []byte, err error)
	UpdateTwinProperties(ctx context.Context, payload []byte) (version int, err error)
	Close() error
}

// Event can be both D2C or C2D event message.
type Event struct {
	DeviceID   string
	Payload    []byte
	Properties map[string]string
	Err        error
}

// TwinState is desired twin state update message.
type TwinState struct {
	Payload []byte
	Err     error
}

// Invocation is a direct method invocation message.
type Invocation struct {
	RID     string
	Method  string
	Payload []byte
	Err     error
}

// AuthFunc is used to obtain hostname and sas token before authenticating.
type AuthFunc func(ctx context.Context, path string) (hostname string, token string, err error)
