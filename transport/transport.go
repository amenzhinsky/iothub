package transport

import (
	"context"
)

// Transport interface.
type Transport interface {
	Connect(ctx context.Context, deviceID string, sasFunc AuthFunc) error
	IsNetworkError(err error) bool
	PublishEvent(ctx context.Context, event *Event) error
	C2D() chan *Event
	DMI() chan *Call
	DSC() chan []byte
	RespondDirectMethod(ctx context.Context, rid string, code int, payload []byte) error
	RetrieveTwinProperties(ctx context.Context) (payload []byte, err error)
	UpdateTwinProperties(ctx context.Context, payload []byte) (version int, err error)
	Close() error
}

// Event can be both D2C or C2D event.
type Event struct {
	DeviceID   string
	Payload    []byte
	Properties map[string]string
}

// Call is a direct method invocation.
type Call struct {
	RID     string
	Method  string
	Payload []byte
}

// AuthFunc is used to obtain hostname and sas token before authenticating.
type AuthFunc func(ctx context.Context) (hostname string, sas string, err error)
