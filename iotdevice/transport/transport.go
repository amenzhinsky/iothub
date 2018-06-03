package transport

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/goautomotive/iothub/common"
)

// Transport interface.
type Transport interface {
	Connect(ctx context.Context, creds Credentials) error
	Send(ctx context.Context, msg *common.Message) error
	RegisterDirectMethods(ctx context.Context, mux MethodDispatcher) error
	SubscribeEvents(ctx context.Context, mux MessageDispatcher) error
	SubscribeTwinUpdates(ctx context.Context, mux TwinStateDispatcher) error
	RetrieveTwinProperties(ctx context.Context) (payload []byte, err error)
	UpdateTwinProperties(ctx context.Context, payload []byte) (version int, err error)
	Close() error
}

// MessageDispatcher handles incoming messages.
type MessageDispatcher interface {
	Dispatch(msg *common.Message)
}

// TwinStateDispatcher handles twin state updates.
type TwinStateDispatcher interface {
	Dispatch(b []byte)
}

// MethodDispatcher handles direct method calls.
type MethodDispatcher interface {
	Dispatch(methodName string, b []byte) (rc int, data []byte, err error)
}

// Credentials is connection credentials needed for x509 or sas authentication.
type Credentials interface {
	DeviceID() string
	Hostname() string
	TLSConfig() *tls.Config
	IsSAS() bool
	Token(ctx context.Context, uri string, d time.Duration) (string, error)
}
