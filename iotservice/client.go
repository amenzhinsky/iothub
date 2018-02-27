package iotservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/amenzhinsky/iothub/credentials"
	"github.com/amenzhinsky/iothub/eventhub"
	"gopkg.in/satori/go.uuid.v1"
	"pack.ag/amqp"
)

// ClientOption is a client connectivity option.
type ClientOption func(*Client) error

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

// WithCredentials uses the given credentials to generate SAS tokens.
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
func WithLogger(l *log.Logger) ClientOption {
	return func(c *Client) error {
		c.logger = l
		return nil
	}
}

// New creates new iothub service client.
func New(opts ...ClientOption) (*Client, error) {
	c := &Client{
		done:   make(chan struct{}),
		logger: log.New(os.Stdout, "[iotsvc] ", 0),
	}
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	if c.creds == nil {
		return nil, errors.New("credentials are missing, consider using `WithCredentials` option")
	}

	// set the default rest client, it uses only bundled ca-certificates
	// it's useful when the ca-certificates package is not present on
	// very slim host systems like alpine and busybox.
	if c.http == nil {
		c.http = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: credentials.TLSConfig(c.creds.HostName),
			},
		}
	}
	return c, nil
}

type Client struct {
	mu     sync.Mutex
	conn   *eventhub.Client
	done   chan struct{}
	creds  *credentials.Credentials
	logger *log.Logger
	http   *http.Client // REST client
}

// Connect connects to AMQP broker, has to be done before publishing events.
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	eh, err := eventhub.Dial(c.creds.HostName)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			eh.Close()
		}
	}()

	sas, err := c.creds.SAS(c.creds.HostName, time.Hour)
	if err != nil {
		return err
	}
	if err = eh.PutTokenContinuously(ctx, c.creds.HostName, sas, c.done); err != nil {
		return err
	}
	c.conn = eh
	return nil
}

// C2D used two absolutely different ways of authentication for sending
// messages and subscribing to events stream.
//
// In this case we connect to an eventhub instance to listen to events.
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
	defer func() {
		if err != nil {
			conn.Close()
		}
	}()

	sess, err := conn.NewSession()
	if err != nil {
		return nil, "", err
	}
	defer sess.Close()

	// trigger redirect error
	recv, err := sess.NewReceiver(amqp.LinkSourceAddress("messages/events/"))
	if err != nil {
		return nil, "", err
	}
	defer recv.Close()
	_, err = recv.Receive(ctx)

	if err == nil {
		return nil, "", errors.New("expected redirect error")
	}

	rerr, ok := err.(amqp.DetachError)
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

func (c *Client) isConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn != nil
}

var errNotConnected = errors.New("not connected")

// SubscribeFunc handles incoming cloud-to-device events.
type SubscribeFunc func(e *Event)

// Event is a cloud-to-device event.
type Event struct {
	DeviceID   string
	Payload    []byte
	Properties map[string]string

	// Metadata is event annotations available only for incoming events.
	Metadata map[interface{}]interface{}

	// Ack is type of the message feedback, available only for outgoing events.
	Ack string
}

// SubscribeEvents subscribes to device events.
// No need to call Connect first, because this method different connect
// method that dials an eventhub instance first opposed to Publish func.
func (c *Client) Subscribe(ctx context.Context, f SubscribeFunc) error {
	conn, group, err := c.connectToEventHub(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	sess, err := conn.NewSession()
	if err != nil {
		return err
	}
	defer sess.Close()

	return eventhub.SubscribePartitions(ctx, sess, group, "$Default", func(msg *amqp.Message) {
		props := make(map[string]string, len(msg.ApplicationProperties))
		for k, v := range msg.ApplicationProperties {
			props[k] = fmt.Sprint(v)
		}
		devid, ok := msg.Annotations["iothub-connection-device-id"].(string)
		if !ok {
			c.logf("error: unable to typecast iothub-connection-device-id")
			return
		}

		go f(&Event{
			DeviceID:   devid,
			Payload:    msg.Data[0],
			Properties: props,
			Metadata:   msg.Annotations,
		})
	})
}

// Publish sends the given cloud-to-device message and returns its id.
// Panics when event is nil.
func (c *Client) Publish(ctx context.Context, event *Event) (string, error) {
	if event == nil {
		panic("event is nil")
	}
	if event.DeviceID == "" {
		return "", errors.New("device id is empty")
	}
	if event.Payload == nil {
		return "", errors.New("payload is nil")
	}

	if !c.isConnected() {
		return "", errNotConnected
	}
	send, err := c.conn.Sess().NewSender(
		amqp.LinkTargetAddress("/messages/devicebound"),
	)
	if err != nil {
		return "", err
	}
	defer send.Close()

	// convert Properties to ApplicationProperties
	ap := make(map[string]interface{}, len(event.Properties))
	for k, v := range event.Properties {
		ap[k] = v
	}
	if event.Ack != "" {
		ap["iothub-ack"] = event.Ack
	}

	msgID := uuid.NewV4().String()
	if err = send.Send(ctx, &amqp.Message{
		Data: [][]byte{event.Payload},
		Properties: &amqp.MessageProperties{
			MessageID: msgID,
			To:        fmt.Sprintf("/devices/%s/messages/devicebound", event.DeviceID),
		},
		ApplicationProperties: ap,
	}); err != nil {
		return "", err
	}
	return msgID, nil
}

// FeedbackFunc handles message feedback.
type FeedbackFunc func(f *Feedback)

// SubscribeFeedback subscribes to feedback of messages that ack was requested.
func (c *Client) SubscribeFeedback(ctx context.Context, fn FeedbackFunc) error {
	if !c.isConnected() {
		return errNotConnected
	}
	recv, err := c.conn.Sess().NewReceiver(
		amqp.LinkSourceAddress("/messages/servicebound/feedback"),
	)
	if err != nil {
		return err
	}
	defer recv.Close()

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

// invocation is direct method invocation object.
type invocation struct {
	MethodName      string                 `json:"methodName"`
	ResponseTimeout int                    `json:"responseTimeoutInSeconds,omitempty"`
	Payload         map[string]interface{} `json:"payload,omitempty"`
}

const restAPIVersion = "2017-06-30"

// InvokeOption direct-method invocation option.
type InvokeOption func(i *invocation) error

// WithInvokeResponseTimeout sets response timeout in seconds,
// 30 seconds is the default value.
func WithInvokeResponseTimeout(seconds int) InvokeOption {
	return func(i *invocation) error {
		i.ResponseTimeout = seconds
		return nil
	}
}

// InvokeMethod calls the given device direct method.
func (c *Client) InvokeMethod(
	ctx context.Context,
	deviceID, methodName string,
	payload map[string]interface{},
	opts ...InvokeOption) (map[string]interface{}, error) {
	if deviceID == "" {
		return nil, errors.New("deviceID cannot be empty")
	}
	if methodName == "" {
		return nil, errors.New("methodName cannot be empty")
	}

	m := &invocation{MethodName: methodName, Payload: payload, ResponseTimeout: 30}
	for _, o := range opts {
		if err := o(m); err != nil {
			return nil, err
		}
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	r, err := http.NewRequest(
		http.MethodPost,
		fmt.Sprintf("https://%s/twins/%s/methods?api-version=%s",
			c.creds.HostName, deviceID, restAPIVersion),
		bytes.NewReader(b),
	)
	if err != nil {
		return nil, err
	}

	auth, err := c.creds.SAS(c.creds.HostName, time.Hour)
	if err != nil {
		return nil, err
	}

	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", auth)
	r.Header.Set("Request-Id", uuid.NewV4().String())
	r.WithContext(ctx)

	res, err := c.http.Do(r)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	b, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("code = %d, body = %q", res.StatusCode, string(b))
	}

	var ir struct {
		Status  int
		Payload map[string]interface{}
	}
	return ir.Payload, json.Unmarshal(b, &ir)
}

func (c *Client) logf(format string, v ...interface{}) {
	if c.logger != nil {
		c.logger.Printf(format, v...)
	}
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
