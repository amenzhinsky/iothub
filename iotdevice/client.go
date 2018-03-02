package iotdevice

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

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

// WithDeviceID sets client device id.
func WithDeviceID(s string) ClientOption {
	return func(c *Client) error {
		c.deviceID = s
		return nil
	}
}

// WithConnectionString same as WithCredentials,
// but it parses the given connection string first.
func WithConnectionString(cs string) ClientOption {
	return func(c *Client) error {
		creds, err := common.ParseConnectionString(cs)
		if err != nil {
			return err
		}
		c.deviceID = creds.DeviceID
		c.authFunc = mkCredsAuthFunc(creds)
		return nil
	}
}

// WithCredentials uses the given credentials to obtain
// connection credentials by the authFunc and to set DeviceID.
func WithCredentials(creds *common.Credentials) ClientOption {
	return func(c *Client) error {
		c.deviceID = creds.DeviceID
		c.authFunc = mkCredsAuthFunc(creds)
		return nil
	}
}

func mkCredsAuthFunc(creds *common.Credentials) transport.AuthFunc {
	return func(_ context.Context, path string) (string, string, error) {
		token, err := creds.SAS(creds.HostName+path, time.Hour)
		if err != nil {
			return "", "", err
		}
		return creds.HostName, token, nil
	}
}

// WithAuthFunc sets AuthFunc useful when you're using a 3rd-party token service.
func WithAuthFunc(fn transport.AuthFunc) ClientOption {
	return func(c *Client) error {
		c.authFunc = fn
		return nil
	}
}

// WithHostname changes hostname required when using x509 authentication.
func WithHostname(hostname string) ClientOption {
	return func(c *Client) error {
		c.tls.ServerName = hostname
		return nil
	}
}

// WithX509FromCert uses the given TLS certificate for x509 authentication.
func WithX509FromCert(crt *tls.Certificate) ClientOption {
	return func(c *Client) error {
		c.tls.Certificates = []tls.Certificate{*crt}
		return nil
	}
}

// WithX509FromFile is same as `WithX509FromCert` but parses the given files first.
func WithX509FromFile(certFile, keyFile string) ClientOption {
	return func(c *Client) error {
		crt, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return err
		}
		c.tls.Certificates = []tls.Certificate{crt}
		return nil
	}
}

// errNotConnected is the initial connection state.
var errNotConnected = errors.New("not connected")

// NewClient returns new iothub client.
func NewClient(opts ...ClientOption) (*Client, error) {
	c := &Client{
		tls:     &tls.Config{RootCAs: common.RootCAs()},
		subs:    make([]chan *transport.Message, 0, 10),
		changes: make([]chan *transport.TwinState, 0, 10),
		methods: make(map[string]DirectMethodFunc, 10),
		done:    make(chan struct{}),
		debug:   os.Getenv("DEBUG") != "",
		connErr: errNotConnected,
	}

	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}
	if c.deviceID == "" {
		return nil, errors.New("device id is empty, consider using `WithDeviceID` option")
	}
	if c.tr == nil {
		return nil, errors.New("transport is nil, consider using `WithTransport` option")
	}
	return c, nil
}

// Client is iothub device client.
type Client struct {
	deviceID string
	authFunc transport.AuthFunc
	tls      *tls.Config

	logger *log.Logger
	debug  bool

	mu      sync.RWMutex
	subs    []chan *transport.Message
	changes []chan *transport.TwinState
	methods map[string]DirectMethodFunc
	done    chan struct{}

	connCh  chan struct{}
	connMu  sync.RWMutex
	connErr error // nil means successfully connected

	c2ds chan *transport.Message    // cloud-to-device events
	dmis chan *transport.Invocation // direct method invocations
	tscs chan *transport.TwinState  // twin state changes

	tr transport.Transport
}

// CloudToDeviceFunc handles cloud-to-device events.
type CloudToDeviceFunc func(msg *common.Message)

// DirectMethodFunc handles direct method invocations.
type DirectMethodFunc func(p map[string]interface{}) (map[string]interface{}, error)

// DesiredStateChangeFunc handler desired state changes.
type DesiredStateChangeFunc func(TwinState)

// DeviceID returns iothub device id.
func (c *Client) DeviceID() string {
	return c.deviceID
}

// Connect connects to iothub and subscribes to communication topics.
//
// If `ignoreNetErrors` is true it reconnects until connection is
// established or other kind of error encountered.
func (c *Client) Connect(ctx context.Context, ignoreNetErrors bool) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

Retry:
	c.c2ds, c.dmis, c.tscs, c.connErr = c.tr.Connect(
		ctx,
		c.tls.Clone(),
		c.deviceID,
		transport.AuthFunc(c.authFunc),
	)
	if ignoreNetErrors && c.tr.IsNetworkError(c.connErr) {
		c.logf("couldn't connect, reconnecting")
		goto Retry
	}
	if c.connErr == nil {
		go c.recv()
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
func (c *Client) ConnectInBackground(ctx context.Context, ignoreNetErrors bool) error {
	c.connMu.Lock()
	c.connCh = make(chan struct{})
	c.connMu.Unlock()
	go func() {
		if err := c.Connect(ctx, ignoreNetErrors); err != nil {
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
	case <-c.done:
		return errors.New("client is closed")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) recv() {
Loop:
	for {
		select {
		case ev, ok := <-c.c2ds:
			if !ok {
				break Loop
			}
			if ev.Err != nil {
				c.logf("cloud-to-device error: %s", ev.Err)
			} else {
				if c.debug {
					c.logf("cloud-to-device received\n%s", ev.Msg.Inspect())
				} else {
					c.logf("cloud-to-device received %s", ev.Msg.MessageID)
				}
			}

			c.mu.RLock()
			for _, w := range c.subs {
				select {
				case w <- ev:
				default:
					panic("c2d jam")
				}
			}
			c.mu.RUnlock()
		case call, ok := <-c.dmis:
			if !ok {
				break Loop
			}
			c.mu.RLock()
			for k, f := range c.methods {
				if k != call.Method {
					continue
				}
				c.mu.RUnlock()

				var v map[string]interface{}
				if err := json.Unmarshal(call.Payload, &v); err != nil {
					c.logf("error direct-method payload: %s", err)
					continue Loop
				}
				go c.handleDirectMethod(f, v, call.Method, call.RID)
				continue Loop
			}
			c.mu.RUnlock()
			c.logf("direct-method %q is missing", call.Method)
		case s, ok := <-c.tscs:
			if !ok {
				break Loop
			}

			// TODO: double json parsing here and all by all subscribers
			var v TwinState
			if err := json.Unmarshal(s.Payload, &v); err != nil {
				s.Err = err
			}

			if s.Err == nil {
				if c.debug {
					c.logf("twin-desired-state ver=%d:\n--------------\n%s\n--------------",
						v.Version(),
						s.Payload,
					)
				} else {
					c.logf("twin-desired-state ver=%d", v.Version())
				}
			} else {
				c.logf("twin-desired-state error: %s", s.Err)
			}

			c.mu.RLock()
			for _, w := range c.changes {
				select {
				case w <- s:
				default:
					panic("dsc jam")
				}
			}
			c.mu.RUnlock()
		case <-c.done:
			return
		}
	}

	// if the loop exits we consider client closed
	// TODO: add error describing what causes it
	c.mu.Lock()
	close(c.done)
	c.mu.Unlock()
}

func (c *Client) handleDirectMethod(f DirectMethodFunc, p map[string]interface{}, name, rid string) {
	if c.debug {
		c.logf("direct-method %q rid=%s\n---- body ----\n%s\n--------------", name, rid, p)
	} else {
		c.logf("direct-method %q rid=%s", name, rid)
	}
	code := 200
	body, err := f(p)
	if err != nil {
		code = 500
		if body == nil {
			body = map[string]interface{}{
				"error": err.Error(),
			}
		}
	}

	b, err := json.Marshal(body)
	if err != nil {
		c.logf("error marshalling payload: %s", err)
		return
	}

	if c.debug {
		c.logf("direct-method %q rid=%s code=%d\n---- body ----\n%s\n--------------", name, rid, code, body)
	} else {
		c.logf("direct-method %q rid=%s code=%d", name, rid, code)
	}
	if err = c.tr.RespondDirectMethod(context.Background(), rid, code, b); err != nil {
		c.logf("error sending direct-method %q result: %s", name, err)
	}
}

// SubscribeEvents subscribes to cloud-to-device events and blocks until ctx is canceled.
func (c *Client) SubscribeEvents(ctx context.Context, f CloudToDeviceFunc) error {
	if err := c.ConnectionError(ctx); err != nil {
		return err
	}

	w := make(chan *transport.Message, 1)
	c.mu.Lock()
	c.subs = append(c.subs, w)
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		for i, x := range c.subs {
			if w == x {
				c.subs = append(c.subs[:i], c.subs[i+1:]...)
				break
			}
		}
		c.mu.Unlock()
	}()

	for {
		select {
		case m := <-w:
			if m.Err != nil {
				return m.Err
			}
			go f(m.Msg)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// HandleMethod registers the given direct method handler,
// returns an error when method is already registered.
// If f returns an error and empty body its error string
// used as value of the error attribute in the result json.
// Blocks until context is done and deregisters it.
func (c *Client) HandleMethod(ctx context.Context, name string, f DirectMethodFunc) error {
	if err := c.ConnectionError(ctx); err != nil {
		return err
	}
	if name == "" {
		return errors.New("name cannot be blank")
	}
	c.mu.Lock()
	if _, ok := c.methods[name]; ok {
		c.mu.Unlock()
		return fmt.Errorf("method %q is already registered", name)
	}
	c.methods[name] = f
	c.mu.Unlock()
	c.logf("direct-method %q registered", name)
	<-ctx.Done()
	c.mu.Lock()
	delete(c.methods, name)
	c.mu.Unlock()
	c.logf("direct-method %q deregistered", name)
	return ctx.Err()
}

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

// SubscribeDesiredStateChanges watches twin device desired state changes until ctx canceled.
func (c *Client) SubscribeTwinStateChanges(ctx context.Context, f DesiredStateChangeFunc) error {
	if err := c.ConnectionError(ctx); err != nil {
		return err
	}

	w := make(chan *transport.TwinState, 1)
	c.mu.Lock()
	c.changes = append(c.changes, w)
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		for i, x := range c.changes {
			if w == x {
				c.changes = append(c.changes[:i], c.changes[i+1:]...)
				break
			}
		}
		c.mu.Unlock()
	}()

	for {
		select {
		case s := <-w:
			var v TwinState
			if err := json.Unmarshal(s.Payload, &v); err != nil {
				return err
			}
			go f(v)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// SendEvent sends a device-to-cloud message.
// Panics when event is nil.
func (c *Client) SendEvent(ctx context.Context, msg *common.Message) error {
	if err := c.ConnectionError(ctx); err != nil {
		return err
	}
	if msg == nil {
		panic("event is nil")
	}
	if msg.Payload == nil {
		return errors.New("payload is nil")
	}
	if err := c.tr.Send(ctx, c.deviceID, msg); err != nil {
		return err
	}
	if c.debug {
		c.logf("device-to-cloud sent\n%s", msg.Inspect())
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
	}
	return c.tr.Close()
}
