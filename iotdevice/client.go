package iotdevice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/amenzhinsky/iothub/credentials"
	"github.com/amenzhinsky/iothub/transport"
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
		creds, err := credentials.ParseConnectionString(cs)
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
func WithCredentials(creds *credentials.Credentials) ClientOption {
	return func(c *Client) error {
		c.deviceID = creds.DeviceID
		c.authFunc = mkCredsAuthFunc(creds)
		return nil
	}
}

func mkCredsAuthFunc(creds *credentials.Credentials) transport.AuthFunc {
	return func(context.Context) (string, string, error) {
		sas, err := creds.SAS(time.Hour)
		if err != nil {
			return "", "", err
		}
		return creds.HostName, sas, nil
	}
}

// WithAuthFunc sets AuthFunc useful when you're using a 3rd-party token service.
func WithAuthFunc(fn transport.AuthFunc) ClientOption {
	return func(c *Client) error {
		c.authFunc = fn
		return nil
	}
}

// errNotConnected is the initial connection state.
var errNotConnected = errors.New("not connected")

// New returns new iothub client parsing the given connection string.
func New(opts ...ClientOption) (*Client, error) {
	c := &Client{
		subs:    make([]CloudToDeviceFunc, 0),
		changes: make([]DesiredStateChangeFunc, 0),
		methods: make(map[string]DirectMethodFunc, 0),
		close:   make(chan struct{}),
		logger:  log.New(os.Stdout, "[iothub] ", 0),
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
	if c.authFunc == nil {
		return nil, errors.New("authentication func is nil, consider using `WithAuthFunc` option")
	}
	return c, nil
}

// Client is iothub device client.
type Client struct {
	deviceID string
	authFunc transport.AuthFunc

	logger *log.Logger
	debug  bool

	mu      sync.RWMutex
	subs    []CloudToDeviceFunc
	changes []DesiredStateChangeFunc
	methods map[string]DirectMethodFunc
	close   chan struct{}

	connCh  chan struct{}
	connMu  sync.RWMutex
	connErr error // nil means successfully connected

	tr transport.Transport
}

// CloudToDeviceFunc handles cloud-to-device events.
type CloudToDeviceFunc func(event *Event)

// DirectMethodFunc handles direct method invocations.
type DirectMethodFunc func(p map[string]interface{}) (map[string]interface{}, error)

// DesiredStateChangeFunc handler desired state changes.
type DesiredStateChangeFunc func(State)

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
	c.connErr = c.tr.Connect(ctx, c.deviceID, transport.AuthFunc(c.authFunc))
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
	select {
	case <-w:
		return c.connErr
	case <-c.close:
		return errors.New("closed")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) recv() {
	c2d := c.tr.C2D()
	dmi := c.tr.DMI()
	dsc := c.tr.DSC()

Loop:
	for {
		select {
		case ev, ok := <-c2d:
			if !ok {
				panic("c2d channel is closed unexpectedly")
			}
			if c.debug {
				c.logf("cloud-to-device %s\n---- body ----\n%s\n--------------", fmtprops(ev.Properties), ev.Payload)
			} else {
				c.logf("cloud-to-device %s", fmtprops(ev.Properties))
			}
			c.mu.RLock()
			if len(c.subs) == 0 {
				c.logf("cloud-to-device no subscribers")
			}
			for _, f := range c.subs {
				go f(&Event{
					Payload:    ev.Payload,
					Properties: ev.Properties,
				})
			}
			c.mu.RUnlock()
		case call, ok := <-dmi:
			if !ok {
				panic("dmi channel is closed unexpectedly")
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
		case b, ok := <-dsc:
			if !ok {
				panic("dsc channel is closed unexpectedly")
			}
			var v State
			if err := json.Unmarshal(b, &v); err != nil {
				c.logf("error parsing twin updates: %s", err)
				continue Loop
			}

			c.logf("desired state update:\n--------------\n%s\n--------------", b)
			c.mu.RLock()
			for _, f := range c.changes {
				go f(v)
			}
			c.mu.RUnlock()
		case <-c.close:
			return
		}
	}
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
	c.mu.Lock()
	c.subs = append(c.subs, f)
	c.mu.Unlock()
	<-ctx.Done()
	c.mu.Lock()
	for i, fn := range c.subs {
		if ptreq(f, fn) {
			c.subs = append(c.subs[:i], c.subs[i+1:]...)
			break
		}
	}
	c.mu.Unlock()
	return ctx.Err()
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

// State is both desired and reported twin device's state.
type State map[string]interface{}

// Version is state version.
func (s State) Version() int {
	v, ok := s["$version"].(float64)
	if !ok {
		return 0
	}
	return int(v)
}

// RetrieveState returns desired and reported twin device states.
func (c *Client) RetrieveState(ctx context.Context) (desired State, reported State, err error) {
	if err := c.ConnectionError(ctx); err != nil {
		return nil, nil, err
	}
	b, err := c.tr.RetrieveTwinProperties(ctx)
	if err != nil {
		return nil, nil, err
	}
	var v struct {
		Desired  State `json:"desired"`
		Reported State `json:"reported"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, nil, err
	}
	return v.Desired, v.Reported, nil
}

// UpdateTwin updates twin device's state and returns new version.
// To remove any attribute set its value to nil.
func (c *Client) UpdateTwin(ctx context.Context, s State) (int, error) {
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
func (c *Client) SubscribeTwinChanges(ctx context.Context, f DesiredStateChangeFunc) error {
	if err := c.ConnectionError(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	c.changes = append(c.changes, f)
	c.mu.Unlock()
	<-ctx.Done()
	c.mu.Lock()
	for i, fn := range c.changes {
		if ptreq(f, fn) {
			c.changes = append(c.changes[:i], c.changes[i+1:]...)
			break
		}
	}
	c.mu.Unlock()
	return ctx.Err()
}

// Event is a device-to-cloud event.
type Event struct {
	Payload    []byte
	Properties map[string]string
}

// Publish sends a device-to-cloud message.
// Panics when event is nil.
func (c *Client) Publish(ctx context.Context, event *Event) error {
	if err := c.ConnectionError(ctx); err != nil {
		return err
	}
	if event == nil {
		panic("event is nil")
	}
	if event.Payload == nil {
		return errors.New("payload is nil")
	}
	if err := c.tr.PublishEvent(ctx, &transport.Event{
		DeviceID:   c.deviceID,
		Payload:    event.Payload,
		Properties: event.Properties,
	}); err != nil {
		return err
	}
	if c.debug {
		c.logf("device-to-cloud %s\n---- body ----\n%s\n--------------", fmtprops(event.Properties), event.Payload)
	} else {
		c.logf("device-to-cloud %s", fmtprops(event.Properties))
	}
	return nil
}

func fmtprops(p map[string]string) string {
	u := url.Values{}
	for k, v := range p {
		u.Add(k, v)
	}
	return u.Encode()
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
	case <-c.close:
		return nil
	default:
		close(c.close)
		return c.tr.Close()
	}
}

// ptreq reports whether two functions are equal or not,
// because they cannot be compared natively for performance reasons,
// so we just compare pointer values.
func ptreq(v1, v2 interface{}) bool {
	return fmt.Sprintf("%p", v1) == fmt.Sprintf("%p", v2)
}
