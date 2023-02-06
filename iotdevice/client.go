package iotdevice

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/dangeroushobo/iothub/common"
	"github.com/dangeroushobo/iothub/iotdevice/transport"
	"github.com/dangeroushobo/iothub/iotservice"
	"github.com/dangeroushobo/iothub/logger"
)

// ClientOption is a client configuration option.
type ClientOption func(c *Client)

// WithLogger changes default logger, default it an stdout logger.
func WithLogger(l logger.Logger) ClientOption {
	return func(c *Client) {
		c.logger = l
	}
}

// NewFromConnectionString creates a device client based on the given connection string.
func NewFromConnectionString(
	transport transport.Transport, cs string, opts ...ClientOption,
) (*Client, error) {
	creds, err := ParseConnectionString(cs)
	if err != nil {
		return nil, err
	}
	return New(transport, creds, opts...)
}

func ParseConnectionString(cs string) (*SharedAccessKeyCredentials, error) {
	m, err := common.ParseConnectionString(cs, "DeviceId", "SharedAccessKey")
	if err != nil {
		return nil, err
	}
	return &SharedAccessKeyCredentials{
		DeviceID: m["DeviceId"],
		SharedAccessKey: common.SharedAccessKey{
			HostName:            m["HostName"],
			SharedAccessKeyName: m["SharedAccessKeyName"],
			SharedAccessKey:     m["SharedAccessKey"],
		},
	}, nil
}

func NewFromX509Cert(
	transport transport.Transport,
	deviceID, hostName string, crt *tls.Certificate,
	opts ...ClientOption,
) (*Client, error) {
	return New(transport, &X509Credentials{
		DeviceID:    deviceID,
		HostName:    hostName,
		Certificate: crt,
	}, opts...)
}

func NewFromX509FromFile(
	transport transport.Transport,
	deviceID, hostname, certFile, keyFile string,
	opts ...ClientOption,
) (*Client, error) {
	crt, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return NewFromX509Cert(transport, deviceID, hostname, &crt, opts...)
}

// New returns new iothub client.
func New(
	transport transport.Transport, creds transport.Credentials, opts ...ClientOption,
) (*Client, error) {
	c := &Client{
		tr:    transport,
		creds: creds,

		ready:  make(chan struct{}),
		done:   make(chan struct{}),
		logger: logger.NewFromString(os.Getenv("IOTHUB_DEVICE_LOG_LEVEL")),

		evMux: newEventsMux(),
		tsMux: newTwinStateMux(),
		dmMux: newMethodMux(),
	}

	for _, opt := range opts {
		opt(c)
	}

	// transport uses the same logger as the client
	c.tr.SetLogger(c.logger)
	return c, nil
}

// Client is iothub device client.
type Client struct {
	creds transport.Credentials
	tr    transport.Transport

	logger logger.Logger

	mu    sync.RWMutex
	ready chan struct{}
	done  chan struct{}

	evMux *eventsMux
	tsMux *twinStateMux
	dmMux *methodMux
}

// DirectMethodHandler handles direct method invocations.
type DirectMethodHandler func(payload map[string]interface{}) (
	code int, response map[string]interface{}, err error,
)

// UpdateCredentials updates the credentials for the client.
func (c *Client) UpdateCredentials(creds transport.Credentials) {
	c.creds = creds
}

// DeviceID returns iothub device id.
func (c *Client) DeviceID() string {
	return c.creds.GetDeviceID()
}

// Connect connects to the iothub all subsequent calls
// will block until this function finishes with no error so it's the client's
// responsibility to connect in the background by running it in a goroutine
// and control other method invocations or calls in a synchronous way.
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	select {
	case <-c.ready:
		c.mu.Unlock()
		return errors.New("already connected")
	default:
	}
	err := c.tr.Connect(ctx, c.creds)
	if err == nil {
		close(c.ready)
	}
	c.mu.Unlock()
	// TODO: c.err = err
	return err
}

// ErrClosed the client is already closed.
var ErrClosed = errors.New("closed")

func (c *Client) checkConnection(ctx context.Context) error {
	select {
	case <-c.ready:
		return nil
	case <-c.done:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SubscribeEvents subscribes to cloud-to-device events and returns a subscription struct.
func (c *Client) SubscribeEvents(ctx context.Context) (*EventSub, error) {
	if err := c.checkConnection(ctx); err != nil {
		return nil, err
	}
	if err := c.evMux.once(func() error {
		return c.tr.SubscribeEvents(ctx, c.evMux)
	}); err != nil {
		return nil, err
	}
	return c.evMux.sub(), nil
}

// UnsubscribeEvents makes the given subscription to stop receiving messages.
func (c *Client) UnsubscribeEvents(sub *EventSub) {
	c.evMux.unsub(sub)
}

// RegisterMethod registers the given direct method handler,
// returns an error when method is already registered.
// If fn returns an error and empty body its error string
// used as value of the error attribute in the result json.
func (c *Client) RegisterMethod(ctx context.Context, name string, fn DirectMethodHandler) error {
	if err := c.checkConnection(ctx); err != nil {
		return err
	}
	if name == "" {
		return errors.New("name cannot be blank")
	}
	if err := c.dmMux.once(func() error {
		return c.tr.RegisterDirectMethods(ctx, c.dmMux)
	}); err != nil {
		return err
	}
	return c.dmMux.handle(name, fn)
}

// UnregisterMethod unregisters the named method.
func (c *Client) UnregisterMethod(name string) {
	c.dmMux.remove(name)
}

// TwinState is both desired and reported twin device's state.
type TwinState []byte

// Version is state version.
func (s TwinState) Version() (int, int) {
	var v struct {
		Desired  map[string]any `json:"desired"`
		Reported map[string]any `json:"reported"`
	}
	json.Unmarshal(s, &v)
	d, _ := v.Desired["$version"].(float64)
	r, _ := v.Reported["$version"].(float64)
	return int(d), int(r)
}

// String returns the string representation of the TwinState.
func (s TwinState) String() string {
	return string(s)
}

// RetrieveTwinState returns desired and reported twin device states.
func (c *Client) RetrieveTwinState(ctx context.Context) (tw TwinState, err error) {
	if err := c.checkConnection(ctx); err != nil {
		return TwinState{}, err
	}
	b, err := c.tr.RetrieveTwinProperties(ctx)
	if err != nil {
		return TwinState{}, err
	}
	return TwinState(b), nil
}

// UpdateTwinState updates twin device's state and returns new version.
// To remove any attribute set its value to nil.
func (c *Client) UpdateTwinState(ctx context.Context, s TwinState) (int, error) {
	if err := c.checkConnection(ctx); err != nil {
		return 0, err
	}
	return c.tr.UpdateTwinProperties(ctx, []byte(s))
}

// SubscribeTwinUpdates registers fn as a desired state changes handler.
func (c *Client) SubscribeTwinUpdates(ctx context.Context) (*TwinStateSub, error) {
	if err := c.checkConnection(ctx); err != nil {
		return nil, err
	}
	if err := c.tsMux.once(func() error {
		return c.tr.SubscribeTwinUpdates(ctx, c.tsMux)
	}); err != nil {
		return nil, err
	}
	return c.tsMux.sub(), nil
}

// UnsubscribeTwinUpdates unsubscribes the given handler from twin state updates.
func (c *Client) UnsubscribeTwinUpdates(sub *TwinStateSub) {
	c.tsMux.unsub(sub)
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

func WithSendExpiryTime(t time.Time) SendOption {
	return func(msg *common.Message) error {
		msg.ExpiryTime = &t
		return nil
	}
}

func WithSendCreationTime(t time.Time) SendOption {
	return func(msg *common.Message) error {
		msg.EnqueuedTime = &t
		return nil
	}
}

// SendEvent sends a device-to-cloud message.
// Panics when event is nil.
func (c *Client) SendEvent(ctx context.Context, payload []byte, opts ...SendOption) error {
	if err := c.checkConnection(ctx); err != nil {
		return err
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
	c.logger.Debugf("device-to-cloud: %#v", msg)
	return nil
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
		c.evMux.close(ErrClosed)
		c.tsMux.close(ErrClosed)
		return c.tr.Close()
	}
}

func (c *Client) UploadFile(ctx context.Context, blobName string, file io.Reader, size int64) error {
	if err := c.checkConnection(ctx); err != nil {
		return err
	}

	correlationID, sas, err := c.tr.GetBlobSharedAccessSignature(ctx, blobName)
	if err != nil {
		return err
	}

	err = c.tr.UploadToBlob(ctx, sas, file, size)
	if err == nil {
		err = c.tr.NotifyUploadComplete(ctx, correlationID, true, http.StatusOK, "File uploaded successfully")
	} else {
		notifyErr := c.tr.NotifyUploadComplete(ctx, correlationID, false, http.StatusInternalServerError, "File upload failed")
		if notifyErr != nil {
			err = fmt.Errorf("failed to notify file upload: %v - %w", notifyErr, err)
		}
	}

	return err
}

// ListModules list all the registered modules on the device.
func (c *Client) ListModules(ctx context.Context) ([]*iotservice.Module, error) {
	if err := c.checkConnection(ctx); err != nil {
		return nil, err
	}

	return c.tr.ListModules(ctx)
}

// CreateModule Creates adds the given module to the registry.
func (c *Client) CreateModule(ctx context.Context, m *iotservice.Module) (*iotservice.Module, error) {
	if err := c.checkConnection(ctx); err != nil {
		return nil, err
	}

	return c.tr.CreateModule(ctx, m)
}

// GetModule retrieves the named module.
func (c *Client) GetModule(ctx context.Context, moduleID string) (*iotservice.Module, error) {
	if err := c.checkConnection(ctx); err != nil {
		return nil, err
	}

	return c.tr.GetModule(ctx, moduleID)
}

// UpdateModule updates the given module.
func (c *Client) UpdateModule(ctx context.Context, m *iotservice.Module) (*iotservice.Module, error) {
	if err := c.checkConnection(ctx); err != nil {
		return nil, err
	}

	return c.tr.UpdateModule(ctx, m)
}

// DeleteModule removes the named device module.
func (c *Client) DeleteModule(ctx context.Context, m *iotservice.Module) error {
	if err := c.checkConnection(ctx); err != nil {
		return err
	}

	return c.tr.DeleteModule(ctx, m)
}
