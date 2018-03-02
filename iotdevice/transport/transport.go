package transport

import (
	"context"
	"crypto/tls"

	"github.com/amenzhinsky/golang-iothub/common"
)

// Transport interface.
type Transport interface {
	Connect(
		ctx context.Context, tlsConfig *tls.Config, deviceID string, auth AuthFunc,
	) (c2ds chan *Message, dmis chan *Invocation, tscs chan *TwinState, err error)

	IsNetworkError(err error) bool
	Send(ctx context.Context, deviceID string, msg *common.Message) error
	RespondDirectMethod(ctx context.Context, rid string, code int, payload []byte) error
	RetrieveTwinProperties(ctx context.Context) (payload []byte, err error)
	UpdateTwinProperties(ctx context.Context, payload []byte) (version int, err error)
	Close() error
}

// Message can be both D2C or C2D event message.
type Message struct {
	Msg *common.Message
	Err error
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
