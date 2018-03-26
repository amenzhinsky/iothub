package iotdevice

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"log"
	"os"
	"sync"

	"github.com/amenzhinsky/golang-iothub/common"
	"github.com/amenzhinsky/golang-iothub/iotdevice/transport"
)

// ClientOption is a client configuration option.
type ClientOption func(c *Client) error

// WithDebug enables or disables debug mode.
// By default it's activated by the DEBUG environment variable but can be overwritten.
func WithDebug(t bool) ClientOption {
	return func(c *Client) error {
		c.debug = t
		return nil
	}
}

// WithLogger changes default logger, default it an stdout logger.
func WithLogger(l *log.Logger) ClientOption {
	return func(c *Client) error {
		c.logger = l
		return nil
	}
}

// WithTransport changes default transport.
func WithTransport(tr transport.Transport) ClientOption {
	return func(c *Client) error {
		c.tr = tr
		return nil
	}
}

// WithCredentials sets custom authentication credentials, e.g. 3rd-party token provider.
func WithCredentials(creds transport.Credentials) ClientOption {
	if creds == nil {
		panic("creds is nil")
	}
	return func(c *Client) error {
		c.creds = creds
		return nil
	}
}

// WithConnectionString same as WithCredentials,
// but it parses the given connection string first.
func WithConnectionString(cs string) ClientOption {
	return func(c *Client) error {
		var err error
		c.creds, err = NewSASCredentials(cs)
		if err != nil {
			return err
		}
		return nil
	}
}

// WithX509FromCert enables x509 authentication.
func WithX509FromCert(deviceID, hostname string, crt *tls.Certificate) ClientOption {
	return func(c *Client) error {
		var err error
		c.creds, err = NewX509Credentials(deviceID, hostname, crt)
		if err != nil {
			return err
		}
		return nil
	}
}

// WithX509FromFile is same as `WithX509FromCert` but parses the given pem files first.
func WithX509FromFile(deviceID, hostname, certFile, keyFile string) ClientOption {
	return func(c *Client) error {
		crt, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return err
		}
		return WithX509FromCert(deviceID, hostname, &crt)(c)
	}
}

// errNotConnected is the initial connection state.
var errNotConnected = errors.New("not connected")

// NewClient returns new iothub client.
func NewClient(opts ...ClientOption) (*Client, error) {
	c := &Client{
		done:    make(chan struct{}),
		debug:   os.Getenv("DEBUG") != "",
		connErr: errNotConnected,
	}
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}
	if c.creds == nil {
		return nil, errors.New("credentials required")
	}
	if c.tr == nil {
		return nil, errors.New("transport required")
	}
	return c, nil
}

// Client is iothub device client.
type Client struct {
	creds transport.Credentials
	tr    transport.Transport

	logger *log.Logger
	debug  bool

	mu   sync.RWMutex
	done chan struct{}

	connCh  chan struct{}
	connMu  sync.RWMutex
	connErr error // nil means successfully connected

	cmMux messageMux
	dmMux methodMux
	tuMux stateMux
}

// MessageHandler handles cloud-to-device events.
type MessageHandler func(msg *common.Message)

// DirectMethodHandler handles direct method invocations.
type DirectMethodHandler func(p map[string]interface{}) (map[string]interface{}, error)

// TwinUpdateHandler handles twin desired state changes.
type TwinUpdateHandler func(state TwinState)

// DeviceID returns iothub device id.
func (c *Client) DeviceID() string {
	return c.creds.DeviceID()
}

type connection struct {
	ignoreNetErrors bool
}

// ConnOption is a connection option.
type ConnOption func(c *connection)

// WithConnIgnoreNetErrors when a network error occurs while connecting
// it's just ignored and connection reestablished until it succeeds.
func WithConnIgnoreNetErrors(ignore bool) ConnOption {
	return func(c *connection) {
		c.ignoreNetErrors = ignore
	}
}

// Connect connects to the iothub.
func (c *Client) Connect(ctx context.Context, opts ...ConnOption) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	conn := &connection{}
	for _, opt := range opts {
		opt(conn)
	}

Retry:
	c.connErr = c.tr.Connect(ctx, c.creds)
	if c.connErr != nil && conn.ignoreNetErrors && c.tr.IsNetworkError(c.connErr) {
		c.logf("couldn't connect, reconnecting")
		goto Retry
	}
	return c.connErr
}

// ConnectInBackground returns immediately connects in the background.
// Methods that require connection are blocked until it's established.
//
// When error is the connect loop is not a connection error stops
// the loop and it's propagated to all connection-dependent methods.
//
// It can be useful to check connection state using `ConnectionError`
// in a separate goroutine.
func (c *Client) ConnectInBackground(ctx context.Context, opts ...ConnOption) error {
	c.connMu.Lock()
	c.connCh = make(chan struct{})
	c.connMu.Unlock()
	go func() {
		if err := c.Connect(ctx, opts...); err != nil {
			c.logf("background connection error: %s", err)
		}
		c.connMu.Lock()
		close(c.connCh)
		c.connMu.Unlock()
	}()
	return nil
}

// ConnectionError blocks until the connection process is
// finished and returns its error, see `ConnectInBackground` method.
//
// Example:
// 	if err := c.ConnectInBackground(ctx); err != nil {
// 		return err
// 	}
//
// 	go func() {
// 		if err := c.ConnectionError(ctx); err != nil {
// 			fmt.Fprintf(os.Stderr, "connection error: %s\n", err)
// 			os.Exit(1)
// 		}
// 	}()
func (c *Client) ConnectionError(ctx context.Context) error {
	c.connMu.RLock()
	w := c.connCh
	c.connMu.RUnlock()

	// non-background connection
	if w == nil {
		return c.connErr
	}

	select {
	case <-w:
		return c.connErr
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return ErrClosed
	}
}

// SubscribeEvents subscribes to cloud-to-device events and blocks until ctx is canceled.
func (c *Client) SubscribeEvents(ctx context.Context, fn MessageHandler) error {
	if err := c.ConnectionError(ctx); err != nil {
		return err
	}
	c.cmMux.once(func() error {
		return c.tr.SubscribeEvents(ctx, &c.cmMux)
	})
	c.cmMux.add(fn)
	return nil
}

// UnsubscribeEvents unsubscribes the given handler from cloud-to-device events.
func (c *Client) UnsubscribeEvents(fn MessageHandler) {
	c.cmMux.remove(fn)
}

// RegisterMethod registers the given direct method handler,
// returns an error when method is already registered.
// If f returns an error and empty body its error string
// used as value of the error attribute in the result json.
func (c *Client) RegisterMethod(ctx context.Context, name string, fn DirectMethodHandler) error {
	if err := c.ConnectionError(ctx); err != nil {
		return err
	}
	if name == "" {
		return errors.New("name cannot be blank")
	}

	if err := c.dmMux.once(func() error {
		return c.tr.RegisterDirectMethods(ctx, &c.dmMux)
	}); err != nil {
		return err
	}
	return c.dmMux.handle(name, fn)
}

// UnregisterMethod unregisters the named method.
func (c *Client) UnregisterMethod(name string) {
	c.dmMux.remove(name)
}

// ErrClosed returned by methods when client closes.
var ErrClosed = errors.New("iotdevice: closed")

// TwinState is both desired and reported twin device's state.
type TwinState map[string]interface{}

// Version is state version.
func (s TwinState) Version() int {
	v, ok := s["$version"].(float64)
	if !ok {
		return 0
	}
	return int(v)
}

// RetrieveTwinState returns desired and reported twin device states.
func (c *Client) RetrieveTwinState(ctx context.Context) (desired TwinState, reported TwinState, err error) {
	if err := c.ConnectionError(ctx); err != nil {
		return nil, nil, err
	}
	b, err := c.tr.RetrieveTwinProperties(ctx)
	if err != nil {
		return nil, nil, err
	}
	var v struct {
		Desired  TwinState `json:"desired"`
		Reported TwinState `json:"reported"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, nil, err
	}
	return v.Desired, v.Reported, nil
}

// UpdateTwinState updates twin device's state and returns new version.
// To remove any attribute set its value to nil.
func (c *Client) UpdateTwinState(ctx context.Context, s TwinState) (int, error) {
	if err := c.ConnectionError(ctx); err != nil {
		return 0, err
	}
	b, err := json.Marshal(s)
	if err != nil {
		return 0, err
	}
	return c.tr.UpdateTwinProperties(ctx, b)
}

// SubscribeTwinUpdates registers fn as a desired state changes handler.
func (c *Client) SubscribeTwinUpdates(ctx context.Context, fn TwinUpdateHandler) error {
	if err := c.ConnectionError(ctx); err != nil {
		return err
	}
	if err := c.tuMux.once(func() error {
		return c.tr.SubscribeTwinUpdates(ctx, &c.tuMux)
	}); err != nil {
		return err
	}
	c.tuMux.add(fn)
	return nil
}

// UnsubscribeTwinUpdates unsubscribes the given handler from twin state updates.
func (c *Client) UnsubscribeTwinUpdates(fn TwinUpdateHandler) {
	c.tuMux.remove(fn)
}

// SendOption is a send event options.
type SendOption func(msg *common.Message) error

// WithSendQoS sets the quality of service (MQTT only).
// Only 0 and 1 values are supported, defaults to 1.
func WithSendQoS(qos int) SendOption {
	return func(msg *common.Message) error {
		if msg.TransportOptions == nil {
			msg.TransportOptions = map[string]interface{}{}
		}
		msg.TransportOptions["qos"] = qos
		return nil
	}
}

// WithSendMessageID sets message id.
func WithSendMessageID(mid string) SendOption {
	return func(msg *common.Message) error {
		msg.MessageID = mid
		return nil
	}
}

// WithSendCorrelationID sets message correlation id.
func WithSendCorrelationID(cid string) SendOption {
	return func(msg *common.Message) error {
		msg.CorrelationID = cid
		return nil
	}
}

// WithSendTo sets message destination.
func WithSendTo(to string) SendOption {
	return func(msg *common.Message) error {
		msg.To = to
		return nil
	}
}

// TODO: seems like has no effect.
//func WithSendUserID(uid string) SendOption {
//	return func(msg *common.Message) error {
//		msg.UserID = uid
//		return nil
//	}
//}

// TODO: cloud disconnects when using mqtt, for amqp has no effect
//func WithSendExpiryTime(t time.Time) SendOption {
//	return func(msg *common.Message) error {
//		msg.ExpiryTime = t
//		return nil
//	}
//}

// WithSendProperty sets a message option.
func WithSendProperty(k, v string) SendOption {
	return func(msg *common.Message) error {
		if msg.Properties == nil {
			msg.Properties = map[string]string{}
		}
		msg.Properties[k] = v
		return nil
	}
}

// WithSendProperties same as `WithSendProperty` but accepts map of keys and values.
func WithSendProperties(m map[string]string) SendOption {
	return func(msg *common.Message) error {
		if msg.Properties == nil {
			msg.Properties = map[string]string{}
		}
		for k, v := range m {
			msg.Properties[k] = v
		}
		return nil
	}
}

// SendEvent sends a device-to-cloud message.
// Panics when event is nil.
func (c *Client) SendEvent(ctx context.Context, payload []byte, opts ...SendOption) error {
	if err := c.ConnectionError(ctx); err != nil {
		return err
	}
	if payload == nil {
		return errors.New("payload is nil")
	}
	msg := &common.Message{Payload: payload}
	for _, opt := range opts {
		if err := opt(msg); err != nil {
			return err
		}
	}
	if err := c.tr.Send(ctx, msg); err != nil {
		return err
	}
	if c.debug {
		c.logf("device-to-cloud sent\n%v", msg)
	} else {
		c.logf("device-to-cloud sent")
	}
	return nil
}

func (c *Client) logf(format string, v ...interface{}) {
	if c.logger != nil {
		c.logger.Printf(format, v...)
	}
}

// Close closes transport connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.done:
		return nil
	default:
		close(c.done)
		return c.tr.Close()
	}
}
