package iotservice

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/amenzhinsky/iothub/common"
	"github.com/amenzhinsky/iothub/credentials"
	"github.com/amenzhinsky/iothub/eventhub"
	"pack.ag/amqp"
)

// ClientOption is a client configuration option.
type ClientOption func(c *Client) error

// WithConnectionString parses the given connection string instead of using `WithCredentials`.
func WithConnectionString(cs string) ClientOption {
	return func(c *Client) error {
		creds, err := credentials.ParseConnectionString(cs)
		if err != nil {
			return err
		}
		c.creds = creds
		return nil
	}
}

// WithCredentials uses the given credentials to generate GenerateToken tokens.
func WithCredentials(creds *credentials.Credentials) ClientOption {
	return func(c *Client) error {
		c.creds = creds
		return nil
	}
}

// WithHTTPClient changes default http rest client.
func WithHTTPClient(client *http.Client) ClientOption {
	return func(c *Client) error {
		c.http = client
		return nil
	}
}

// WithLogger sets client logger.
func WithLogger(l common.Logger) ClientOption {
	return func(c *Client) error {
		c.logger = l
		return nil
	}
}

// WithTLSConfig sets TLS config that's used by REST HTTP and AMQP clients.
func WithTLSConfig(config *tls.Config) ClientOption {
	return func(c *Client) error {
		c.tls = config
		return nil
	}
}

// NewLogger creates new iothub service client.
func New(opts ...ClientOption) (*Client, error) {
	c := &Client{
		done:   make(chan struct{}),
		logger: common.NewLoggerFromEnv("iotservice", "IOTHUB_SERVICE_LOG_LEVEL"),
	}

	var err error
	for _, opt := range opts {
		if err = opt(c); err != nil {
			return nil, err
		}
	}

	if c.creds == nil {
		cs := os.Getenv("IOTHUB_SERVICE_CONNECTION_STRING")
		if cs == "" {
			return nil, errors.New("$IOTHUB_SERVICE_CONNECTION_STRING is empty")
		}
		c.creds, err = credentials.ParseConnectionString(cs)
		if err != nil {
			return nil, err
		}
	}

	if c.tls == nil {
		c.tls = &tls.Config{RootCAs: common.RootCAs()}
	}
	if c.http == nil {
		c.http = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs: common.RootCAs(),
				},
			},
		}
	}
	return c, nil
}

type Client struct {
	mu     sync.Mutex
	tls    *tls.Config
	conn   *amqp.Client
	done   chan struct{}
	creds  *credentials.Credentials
	logger common.Logger
	http   *http.Client // REST client

	sendMu   sync.Mutex
	sendLink *amqp.Sender
}

// connectToIoTHub connects to IoT Hub's AMQP broker,
// it's needed for sending C2S events and subscribing to events feedback.
//
// It establishes connection only once, subsequent calls return immediately.
func (c *Client) connectToIoTHub(ctx context.Context) (*amqp.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn, nil // already connected
	}
	conn, err := amqp.Dial("amqps://"+c.creds.HostName,
		amqp.ConnTLSConfig(c.tls),
	)
	if err != nil {
		return nil, err
	}

	c.logger.Debugf("connected to %s", c.creds.HostName)
	if err = c.putTokenContinuously(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, err
	}

	c.conn = conn
	return conn, nil
}

// putTokenContinuously writes token first time in blocking mode and returns
// maintaining token updates in the background until the client is closed.
func (c *Client) putTokenContinuously(ctx context.Context, conn *amqp.Client) error {
	const (
		tokenUpdateInterval = time.Hour

		// we need to update tokens before they expire to prevent disconnects
		// from azure, without interrupting the message flow
		tokenUpdateSpan = 10 * time.Minute
	)

	token, err := c.creds.GenerateToken(
		c.creds.HostName, credentials.WithDuration(tokenUpdateInterval),
	)
	if err != nil {
		return err
	}

	sess, err := conn.NewSession()
	if err != nil {
		return err
	}
	defer sess.Close(context.Background())

	if err := c.putToken(ctx, sess, token); err != nil {
		return err
	}

	go func() {
		ticker := time.NewTimer(tokenUpdateInterval - tokenUpdateSpan)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				token, err := c.creds.GenerateToken(
					c.creds.HostName, credentials.WithDuration(tokenUpdateInterval),
				)
				if err != nil {
					c.logger.Errorf("generate token error: %s", err)
					return
				}
				if err := c.putToken(context.Background(), sess, token); err != nil {
					c.logger.Errorf("put token error: %s", err)
					return
				}
				ticker.Reset(tokenUpdateInterval - tokenUpdateSpan)
			case <-c.done:
				return
			}
		}
	}()
	return nil
}

func (c *Client) putToken(ctx context.Context, sess *amqp.Session, token string) error {
	send, err := sess.NewSender(
		amqp.LinkTargetAddress("$cbs"),
	)
	if err != nil {
		return err
	}
	defer send.Close(context.Background())

	recv, err := sess.NewReceiver(amqp.LinkSourceAddress("$cbs"))
	if err != nil {
		return err
	}
	defer recv.Close(context.Background())

	if err = send.Send(ctx, &amqp.Message{
		Value: token,
		Properties: &amqp.MessageProperties{
			To:      "$cbs",
			ReplyTo: "cbs",
		},
		ApplicationProperties: map[string]interface{}{
			"operation": "put-token",
			"type":      "servicebus.windows.net:sastoken",
			"name":      c.creds.HostName,
		},
	}); err != nil {
		return err
	}

	msg, err := recv.Receive(ctx)
	if err != nil {
		return err
	}
	if err = msg.Accept(); err != nil {
		return err
	}
	return eventhub.CheckMessageResponse(msg)
}

// connectToEventHub connects to IoT Hub endpoint compatible with Eventhub
// for receiving D2C events, it uses different endpoints and authentication
// mechanisms than connectToIoTHub.
func (c *Client) connectToEventHub(ctx context.Context) (*eventhub.Client, error) {
	conn, err := c.connectToIoTHub(ctx)
	if err != nil {
		return nil, err
	}

	// iothub broker should redirect us to an eventhub compatible instance
	// straight after subscribing to events stream, for that we need to connect twice
	sess, err := conn.NewSession()
	if err != nil {
		return nil, err
	}
	defer sess.Close(context.Background())

	recv, err := sess.NewReceiver(amqp.LinkSourceAddress("messages/events/"))
	if err != nil {
		return nil, err
	}
	defer recv.Close(context.Background())
	_, err = recv.Receive(ctx)
	if err == nil {
		return nil, errors.New("expected redirect error")
	}

	rerr, ok := err.(*amqp.DetachError)
	if !ok || rerr.RemoteError.Condition != amqp.ErrorLinkRedirect {
		return nil, err
	}

	// "amqps://{host}:5671/{consumerGroup}/"
	group := rerr.RemoteError.Info["address"].(string)
	group = group[strings.Index(group, ":5671/")+6 : len(group)-1]

	host := rerr.RemoteError.Info["hostname"].(string)
	c.logger.Debugf("redirected to %s eventhub", host)

	tlsCfg := c.tls.Clone()
	tlsCfg.ServerName = host

	eh, err := eventhub.Dial(host, group,
		eventhub.WithLogger(c.logger),
		eventhub.WithTLSConfig(tlsCfg),
		eventhub.WithSASLPlain(c.creds.SharedAccessKeyName, c.creds.SharedAccessKey),
	)
	if err != nil {
		return nil, err
	}
	return eh, nil
}

// EventHandler handles incoming cloud-to-device events.
type EventHandler func(e *Event) error

// Event is a device-to-cloud message.
type Event struct {
	*common.Message
}

// SubscribeEvents subscribes to D2C events.
//
// Event handler is blocking, handle asynchronous processing on your own.
func (c *Client) SubscribeEvents(ctx context.Context, fn EventHandler) error {
	// a new connection is established for every invocation,
	// this made on purpose because normally an app calls the method once
	eh, err := c.connectToEventHub(ctx)
	if err != nil {
		return err
	}
	defer eh.Close()

	return eh.Subscribe(ctx, func(msg *eventhub.Event) error {
		return fn(&Event{FromAMQPMessage(msg.Message)})
	},
		eventhub.WithSubscribeSince(time.Now()),
	)
}

// SendOption is a send option.
type SendOption func(msg *common.Message) error

// WithSendMessageID sets message id.
func WithSendMessageID(mid string) SendOption {
	return func(msg *common.Message) error {
		msg.MessageID = mid
		return nil
	}
}

// WithSendCorrelationID sets correlation id.
func WithSendCorrelationID(cid string) SendOption {
	return func(msg *common.Message) error {
		msg.CorrelationID = cid
		return nil
	}
}

// WithSendUserID sets user id.
func WithSendUserID(uid string) SendOption {
	return func(msg *common.Message) error {
		msg.UserID = uid
		return nil
	}
}

const (
	// AckNone no feedback.
	AckNone = "none"

	// AckPositive receive a feedback message if the message was completed.
	AckPositive = "positive"

	// AckNegative receive a feedback message if the message expired
	// (or maximum delivery count was reached) without being completed by the device.
	AckNegative = "negative"

	// AckFull both positive and negative.
	AckFull = "full"
)

// WithSendAck sets message confirmation type.
func WithSendAck(typ string) SendOption {
	return func(msg *common.Message) error {
		switch typ {
		case "":
			return nil // empty value breaks message sending
		case AckNone, AckPositive, AckNegative, AckFull:
		default:
			return fmt.Errorf("unknown ack type: %q", typ)
		}
		return WithSendProperty("iothub-ack", typ)(msg)
	}
}

// WithSentExpiryTime sets message expiration time.
func WithSentExpiryTime(t time.Time) SendOption {
	return func(msg *common.Message) error {
		msg.ExpiryTime = &t
		return nil
	}
}

// WithSendProperty sets a message property.
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

// SendEvent sends the given cloud-to-device message and returns its id.
// Panics when event is nil.
func (c *Client) SendEvent(
	ctx context.Context,
	deviceID string,
	payload []byte,
	opts ...SendOption,
) error {
	if deviceID == "" {
		return errors.New("device id is empty")
	}
	if payload == nil {
		return errors.New("payload is nil")
	}

	msg := &common.Message{
		Payload: payload,
		To:      "/devices/" + deviceID + "/messages/devicebound",
	}
	for _, opt := range opts {
		if err := opt(msg); err != nil {
			return err
		}
	}

	send, err := c.getSendLink(ctx)
	if err != nil {
		return err
	}
	return send.Send(ctx, toAMQPMessage(msg))
}

// getSendLink caches sender link between calls to speed up sending events.
func (c *Client) getSendLink(ctx context.Context) (*amqp.Sender, error) {
	conn, err := c.connectToIoTHub(ctx)
	if err != nil {
		return nil, err
	}

	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if c.sendLink != nil {
		return c.sendLink, nil
	}

	sess, err := conn.NewSession()
	if err != nil {
		return nil, err
	}
	c.sendLink, err = sess.NewSender(
		amqp.LinkTargetAddress("/messages/devicebound"),
	)
	if err != nil {
		_ = sess.Close(context.Background())
		return nil, err
	}
	return c.sendLink, nil
}

// FeedbackHandler handles message feedback.
type FeedbackHandler func(f *Feedback)

// SubscribeFeedback subscribes to feedback of messages that ack was requested.
func (c *Client) SubscribeFeedback(ctx context.Context, fn FeedbackHandler) error {
	conn, err := c.connectToIoTHub(ctx)
	if err != nil {
		return err
	}
	sess, err := conn.NewSession()
	if err != nil {
		return err
	}
	defer sess.Close(context.Background())

	recv, err := sess.NewReceiver(
		amqp.LinkSourceAddress("/messages/servicebound/feedback"),
	)
	if err != nil {
		return err
	}
	defer recv.Close(context.Background())

	for {
		msg, err := recv.Receive(ctx)
		if err != nil {
			return err
		}
		if err = msg.Accept(); err != nil {
			return err
		}

		var v []*Feedback
		if err = json.Unmarshal(msg.Data[0], &v); err != nil {
			return err
		}
		for _, f := range v {
			go fn(f)
		}
	}
}

// Feedback is message feedback.
type Feedback struct {
	OriginalMessageID  string    `json:"originalMessageId"`
	Description        string    `json:"description"`
	DeviceGenerationID string    `json:"deviceGenerationId"`
	DeviceID           string    `json:"deviceId"`
	EnqueuedTimeUTC    time.Time `json:"enqueuedTimeUtc"`
	StatusCode         string    `json:"statusCode"`
}

// HostName returns service's hostname.
func (c *Client) HostName() string {
	return c.creds.HostName
}

var (
	errEmptyDeviceID   = errors.New("device id is empty")
	errEmptyJobID      = errors.New("job id is empty")
	errKeyNotAvailable = errors.New("symmetric key is not available")
)

// DeviceConnectionString builds up a connection string for the given device.
func (c *Client) DeviceConnectionString(device *Device, secondary bool) (string, error) {
	if device == nil {
		panic("device is nil")
	}
	if device.DeviceID == "" {
		return "", errEmptyDeviceID
	}
	key := deviceKey(device, secondary)
	if key == "" {
		return "", errKeyNotAvailable
	}
	return fmt.Sprintf("HostName=%s;DeviceId=%s;SharedAccessKey=%s",
		c.creds.HostName, device.DeviceID, key), nil
}

// DeviceSAS generates a GenerateToken token for the named device.
func (c *Client) DeviceSAS(device *Device, duration time.Duration, secondary bool) (string, error) {
	if device == nil {
		panic("device is nil")
	}
	if device.DeviceID == "" {
		return "", errEmptyDeviceID
	}
	key := deviceKey(device, secondary)
	if key == "" {
		return "", errKeyNotAvailable
	}
	if duration == 0 {
		duration = time.Hour
	}
	creds := credentials.Credentials{
		HostName:        c.creds.HostName,
		DeviceID:        device.DeviceID,
		SharedAccessKey: key,
	}
	return creds.GenerateToken(creds.HostName, credentials.WithDuration(duration))
}

func deviceKey(device *Device, secondary bool) string {
	if device.Authentication == nil || device.Authentication.SymmetricKey == nil {
		return ""
	}
	if secondary {
		return device.Authentication.SymmetricKey.SecondaryKey
	}
	return device.Authentication.SymmetricKey.PrimaryKey
}

type call struct {
	MethodName      string                 `json:"methodName"`
	ConnectTimeout  int                    `json:"connectTimeoutInSeconds,omitempty"`
	ResponseTimeout int                    `json:"responseTimeoutInSeconds,omitempty"`
	Payload         map[string]interface{} `json:"payload"`
}

// CallOption is a direct-method invocation option.
type CallOption func(c *call) error

// ConnectTimeout is connection timeout in seconds.
func WithCallConnectTimeout(seconds int) CallOption {
	return func(c *call) error {
		c.ConnectTimeout = seconds
		return nil
	}
}

// ResponseTimeout is response timeout in seconds.
func WithCallResponseTimeout(seconds int) CallOption {
	return func(c *call) error {
		c.ResponseTimeout = seconds
		return nil
	}
}

// Call calls the named direct method on with the given parameters.
func (c *Client) Call(
	ctx context.Context,
	deviceID string,
	methodName string,
	payload map[string]interface{},
	opts ...CallOption,
) (*Result, error) {
	if deviceID == "" {
		return nil, errors.New("deviceID is empty")
	}
	if methodName == "" {
		return nil, errors.New("methodName is empty")
	}
	if len(payload) == 0 {
		return nil, errors.New("payload is empty")
	}

	v := &call{
		MethodName: methodName,
		Payload:    payload,
	}
	for _, opt := range opts {
		if err := opt(v); err != nil {
			return nil, err
		}
	}

	r := &Result{}
	if err := c.call(ctx, http.MethodPost, "twins/"+url.PathEscape(deviceID)+"/methods", nil, v, r); err != nil {
		return nil, err
	}
	return r, nil
}

// GetDevice retrieves the named device.
func (c *Client) GetDevice(ctx context.Context, deviceID string) (*Device, error) {
	if deviceID == "" {
		return nil, errors.New("deviceID is empty")
	}
	d := &Device{}
	if err := c.call(ctx, http.MethodGet, "devices/"+url.PathEscape(deviceID), nil, nil, d); err != nil {
		return nil, err
	}
	return d, nil
}

// CreateDevice creates a new device.
func (c *Client) CreateDevice(ctx context.Context, device *Device) (*Device, error) {
	if device == nil {
		panic("device is nil")
	}
	if device.DeviceID == "" {
		return nil, errors.New("deviceID is empty")
	}
	d := &Device{}
	if err := c.call(ctx, http.MethodPut, "devices/"+url.PathEscape(device.DeviceID), nil, device, d); err != nil {
		return nil, err
	}
	return d, nil
}

// UpdateDevice updates the named device.
func (c *Client) UpdateDevice(ctx context.Context, device *Device) (*Device, error) {
	if device == nil {
		panic("device is nil")
	}
	if device.DeviceID == "" {
		return nil, errEmptyDeviceID
	}
	d := &Device{}
	if err := c.call(ctx, http.MethodPut, "devices/"+url.PathEscape(device.DeviceID), http.Header{
		"If-Match": {"*"},
	}, device, d); err != nil {
		return nil, err
	}
	return d, nil
}

// DeleteDevice deletes the named device.
func (c *Client) DeleteDevice(ctx context.Context, deviceID string) error {
	if deviceID == "" {
		return errEmptyDeviceID
	}
	return c.call(ctx, http.MethodDelete, "devices/"+url.PathEscape(deviceID), http.Header{
		"If-Match": {"*"},
	}, nil, nil)
}

// ListDevices lists all registered devices.
func (c *Client) ListDevices(ctx context.Context) ([]*Device, error) {
	l := make([]*Device, 0)
	if err := c.call(ctx, http.MethodGet, "devices", nil, nil, &l); err != nil {
		return nil, err
	}
	return l, nil
}

// GetTwin retrieves the named twin device from the registry.
func (c *Client) GetTwin(ctx context.Context, deviceID string) (*Twin, error) {
	if deviceID == "" {
		return nil, errEmptyDeviceID
	}
	t := &Twin{}
	if err := c.call(ctx, http.MethodGet, "twins/"+url.PathEscape(deviceID), nil, nil, t); err != nil {
		return nil, err
	}
	return t, nil
}

// UpdateTwin updates the named twin desired properties.
func (c *Client) UpdateTwin(
	ctx context.Context,
	deviceID string,
	twin *Twin,
	etag string,
) (*Twin, error) {
	if deviceID == "" {
		return nil, errEmptyDeviceID
	}
	if twin == nil {
		panic("twin is nil")
	}
	t := &Twin{}
	if err := c.call(ctx, http.MethodPatch, "twins/"+url.PathEscape(deviceID), http.Header{
		"If-Match": []string{etag},
	}, twin, t); err != nil {
		return nil, err
	}
	return t, nil
}

// Stats retrieves the device registry statistic.
func (c *Client) Stats(ctx context.Context) (*Stats, error) {
	v := &Stats{}
	if err := c.call(ctx, http.MethodGet, "statistics/devices", nil, nil, &v); err != nil {
		return nil, err
	}
	return v, nil
}

func (c *Client) ImportDevicesFromBlob(
	ctx context.Context,
	inputBlobURL string,
	outputBlobURL string,
) (map[string]interface{}, error) {
	var v map[string]interface{}
	if err := c.call(ctx, http.MethodGet, "jobs/create", nil, map[string]interface{}{
		"type":                   "import",
		"inputBlobContainerUri":  inputBlobURL,
		"outputBlobContainerUri": outputBlobURL,
	}, &v); err != nil {
		return nil, err
	}
	return v, nil
}

func (c *Client) ExportDevicesToBlob(
	ctx context.Context,
	outputBlobURL string,
	excludeKeys bool,
) (map[string]interface{}, error) {
	var v map[string]interface{}
	if err := c.call(ctx, http.MethodGet, "jobs/create", nil, map[string]interface{}{
		"type":                   "export",
		"outputBlobContainerUri": outputBlobURL,
		"excludeKeysInExport":    excludeKeys,
	}, &v); err != nil {
		return nil, err
	}
	return v, nil
}

func (c *Client) ListJobs(ctx context.Context) ([]map[string]interface{}, error) {
	var v []map[string]interface{}
	if err := c.call(ctx, http.MethodGet, "jobs", nil, nil, &v); err != nil {
		return nil, err
	}
	return v, nil
}

func (c *Client) GetJob(ctx context.Context, jobID string) (map[string]interface{}, error) {
	if jobID == "" {
		return nil, errEmptyJobID
	}
	var v map[string]interface{}
	if err := c.call(ctx, http.MethodGet, "jobs/"+url.PathEscape(jobID), nil, nil, &v); err != nil {
		return nil, err
	}
	return v, nil
}

func (c *Client) CancelJob(ctx context.Context, jobID string) (map[string]interface{}, error) {
	if jobID == "" {
		return nil, errEmptyJobID
	}
	var v map[string]interface{}
	if err := c.call(ctx, http.MethodDelete, "jobs/"+url.PathEscape(jobID), nil, nil, &v); err != nil {
		return nil, err
	}
	return v, nil
}

// TODO: add the following registry operations:
//   add/delete/update devices (bulk)
//
// see: https://github.com/Azure/azure-iot-sdk-node/blob/master/service/src/registry.ts
func (c *Client) call(
	ctx context.Context, method, path string,
	headers http.Header,
	r, v interface{}, // request and response objects
) error {
	var b []byte
	if r != nil {
		var err error
		b, err = json.Marshal(r)
		if err != nil {
			return err
		}
	}

	uri := "https://" + c.creds.HostName + "/" + path + "?api-version=" + common.APIVersion
	req, err := http.NewRequest(method, uri, bytes.NewReader(b))
	if err != nil {
		return err
	}

	token, err := c.creds.GenerateToken(c.creds.HostName)
	if err != nil {
		return err
	}

	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Authorization", token)
	req.Header.Set("Request-Id", common.GenID())
	if headers != nil {
		for k, v := range headers {
			if len(v) != 1 {
				panic("only one value per key allowed")
			}
			req.Header.Set(k, v[0])
		}
	}

	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}
	c.logger.Debugf("%s %s %d:\n%s\n%s",
		method, uri, res.StatusCode, prefix(b, "> "), prefix(body, "< "),
	)
	if v == nil && res.StatusCode == http.StatusNoContent {
		return nil
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("code = %d, desc = %q", res.StatusCode, string(body))
	}
	return json.Unmarshal(body, v)
}

func prefix(s []byte, prefix string) string {
	if len(s) == 0 {
		return prefix + "[EMPTY]"
	}
	b := &bytes.Buffer{}
	b.WriteString(prefix)
	if err := json.Indent(b, s, prefix, "\t"); err != nil {
		return string(s)
	}
	return b.String()
}

// Close closes transport.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.done:
		return nil
	default:
		close(c.done)
	}
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// NewSymmetricKey generates a random symmetric key.
func NewSymmetricKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}
