package transport

import (
	"context"
	"crypto/tls"

	"github.com/amenzhinsky/golang-iothub/common"
)

// Transport interface.
type Transport interface {
	Connect(ctx context.Context, tlsConfig *tls.Config, deviceID string, auth AuthFunc) error
	IsNetworkError(err error) bool
	Send(ctx context.Context, msg *common.Message) error
	RegisterDirectMethods(ctx context.Context, mux MethodDispatcher) error
	SubscribeEvents(ctx context.Context, mux MessageDispatcher) error
	SubscribeTwinUpdates(ctx context.Context, mux TwinStateDispatcher) error
	RetrieveTwinProperties(ctx context.Context) (payload []byte, err error)
	UpdateTwinProperties(ctx context.Context, payload []byte) (version int, err error)
	Close() error
}

type MethodDispatcher interface {
	Dispatch(methodName string, b []byte) (rc int, data []byte, err error)
}

type MessageDispatcher interface {
	Dispatch(msg *common.Message)
}

type TwinStateDispatcher interface {
	Dispatch(b []byte)
}

// AuthFunc is used to obtain hostname and sas token before authenticating.
type AuthFunc func(ctx context.Context, path string) (hostname string, token string, err error)
