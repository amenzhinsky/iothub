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
	"strings"
	"sync"
	"time"

	"github.com/amenzhinsky/iothub/common"
	"github.com/amenzhinsky/iothub/common/commonamqp"
	"github.com/amenzhinsky/iothub/eventhub"
	"pack.ag/amqp"
)

// ClientOption is a client connectivity option.
type ClientOption func(c *Client) error

// WithConnectionString parses the given connection string instead of using `WithCredentials`.
func WithConnectionString(cs string) ClientOption {
	return func(c *Client) error {
		creds, err := common.ParseConnectionString(cs)
		if err != nil {
			return err
		}
		c.creds = creds
		return nil
	}
}

// WithCredentials uses the given credentials to generate SAS tokens.
func WithCredentials(creds *common.Credentials) ClientOption {
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

// NewClient creates new iothub service client.
func NewClient(opts ...ClientOption) (*Client, error) {
	c := &Client{
		done:   make(chan struct{}),
		logger: common.NewLogWrapper(false),
	}
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	if c.creds == nil {
		return nil, errors.New("credentials are missing, consider using `WithCredentials` or `WithConnectionString` option")
	}

	// set the default rest client, it uses only bundled ca-certificates
	// it's useful when the ca-certificates package is not present on
	// a very slim host systems like alpine or busybox.
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
	conn   *eventhub.Client
	done   chan struct{}
	creds  *common.Credentials
	logger common.Logger
	http   *http.Client // REST client
}

// ConnectToAMQP connects to the iothub AMQP broker, it's done automatically before
// publishing events or subscribing to the feedback topic.
func (c *Client) ConnectToAMQP(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return nil // already connected
	}

	c.logger.Debugf("connecting to %s", c.creds.HostName)
	eh, err := eventhub.Dial("amqps://"+c.creds.HostName, &tls.Config{
		ServerName: c.creds.HostName,
		RootCAs:    common.RootCAs(),
	})
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			eh.Close()
		}
	}()

	if err = eh.PutTokenContinuously(ctx, c.creds.HostName, c.creds, c.done); err != nil {
		return err
	}
	c.conn = eh
	return nil
}

// Subscribing to C2D events requires connection to an eventhub instance,
// that's hostname and authentication mechanism is absolutely different
// from raw connection to an AMQP broker.
func (c *Client) connectToEventHub(ctx context.Context) (*amqp.Client, string, error) {
	user := c.creds.SharedAccessKeyName + "@sas.root." + c.creds.HostName
	user = user[:len(user)-18] // sub .azure-devices.net"
	pass, err := c.creds.SAS(c.creds.HostName, time.Hour)
	if err != nil {
		return nil, "", err
	}

	addr := "amqps://" + c.creds.HostName
	conn, err := amqp.Dial(addr, amqp.ConnSASLPlain(user, pass))
	if err != nil {
		return nil, "", err
	}
	defer conn.Close()

	sess, err := conn.NewSession()
	if err != nil {
		return nil, "", err
	}
	defer sess.Close(context.Background())

	// trigger redirect error
	recv, err := sess.NewReceiver(amqp.LinkSourceAddress("messages/events/"))
	if err != nil {
		return nil, "", err
	}
	defer recv.Close(context.Background())
	_, err = recv.Receive(ctx)
	if err == nil {
		return nil, "", errors.New("expected redirect error")
	}

	rerr, ok := err.(*amqp.DetachError)
	if !ok || rerr.RemoteError.Condition != amqp.ErrorLinkRedirect {
		return nil, "", err
	}

	// "amqps://{host}:5671/{consumerGroup}/"
	group := rerr.RemoteError.Info["address"].(string)
	group = group[strings.Index(group, ":5671/")+6 : len(group)-1]

	addr = "amqps://" + rerr.RemoteError.Info["hostname"].(string)
	conn, err = amqp.Dial(addr, amqp.ConnSASLPlain(c.creds.SharedAccessKeyName, c.creds.SharedAccessKey))
	if err != nil {
		return nil, "", err
	}
	return conn, group, nil
}

// MessageHandler handles incoming cloud-to-device events.
type MessageHandler func(e *common.Message)

// SubscribeEvents subscribes to device events.
// No need to call Connect first, because this method different connect
// method that dials an eventhub instance first opposed to SendEvent func.
func (c *Client) SubscribeEvents(ctx context.Context, fn MessageHandler) error {
	conn, group, err := c.connectToEventHub(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	sess, err := conn.NewSession()
	if err != nil {
		return err
	}
	defer sess.Close(context.Background())

	return eventhub.SubscribePartitions(ctx, sess, group, "$Default", func(msg *amqp.Message) {
		go fn(commonamqp.FromAMQPMessage(msg))
	})
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

	if err := c.ConnectToAMQP(ctx); err != nil {
		return err
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

	// opening a new link for every message is not the most efficient way
	send, err := c.conn.Sess().NewSender(
		amqp.LinkTargetAddress("/messages/devicebound"),
	)
	if err != nil {
		return err
	}
	defer send.Close(context.Background())
	return send.Send(ctx, commonamqp.ToAMQPMessage(msg))
}

// FeedbackHandler handles message feedback.
type FeedbackHandler func(f *Feedback)

// SubscribeFeedback subscribes to feedback of messages that ack was requested.
func (c *Client) SubscribeFeedback(ctx context.Context, fn FeedbackHandler) error {
	if err := c.ConnectToAMQP(ctx); err != nil {
		return err
	}
	recv, err := c.conn.Sess().NewReceiver(
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
		msg.Accept()

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

// DeviceSAS generates a SAS token for the named device.
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
	creds := common.Credentials{
		HostName:        c.creds.HostName,
		DeviceID:        device.DeviceID,
		SharedAccessKey: key,
	}
	return creds.SAS(creds.HostName, duration)
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

	sas, err := c.creds.SAS(c.creds.HostName, time.Hour)
	if err != nil {
		return err
	}
	rid, err := eventhub.RandString()
	if err != nil {
		return err
	}

	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Authorization", sas)
	req.Header.Set("Request-Id", rid)
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
