package mqtt

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dangeroushobo/iothub/common"
	"github.com/dangeroushobo/iothub/iotdevice/transport"
	"github.com/dangeroushobo/iothub/iotservice"
	"github.com/dangeroushobo/iothub/logger"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var ErrNotImplemented = errors.New("not implemented")

// DefaultQoS is the default quality of service value.
const DefaultQoS = 1

// TransportOption is a transport configuration option.
type TransportOption func(tr *Transport)

// WithLogger sets logger for errors and warnings
// plus debug messages when it's enabled.
func WithLogger(l logger.Logger) TransportOption {
	return func(tr *Transport) {
		tr.logger = l
	}
}

// WithClientOptionsConfig configures the mqtt client options structure,
// use it only when you know EXACTLY what you're doing, because changing
// some of opts attributes may lead to unexpected behaviour.
//
// Typical usecase is to change adjust connect or reconnect interval.
func WithClientOptionsConfig(fn func(opts *mqtt.ClientOptions)) TransportOption {
	if fn == nil {
		panic("fn is nil")
	}
	return func(tr *Transport) {
		tr.cocfg = fn
	}
}

// WithWebSocket makes the mqtt client use MQTT over WebSockets on port 443,
// which is great if e.g. port 8883 is blocked.
func WithWebSocket(enable bool) TransportOption {
	return func(tr *Transport) {
		tr.webSocket = enable
	}
}

// WithModelId makes the mqtt client register the specified DTDL modelID when a connection
// is established, this is useful for Azure PNP integration.
func WithModelID(modelID string) TransportOption {
	return func(tr *Transport) {
		tr.mid = modelID
	}
}

// New returns new Transport transport.
// See more: https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-mqtt-support
func New(opts ...TransportOption) *Transport {
	tr := &Transport{
		//done: make(chan struct{}),
		onConn: make(chan int),
		connLost: make(chan int),
	}
	for _, opt := range opts {
		opt(tr)
	}

	return tr
}

type Transport struct {
	mu   sync.RWMutex
	conn mqtt.Client

	did string // device id
	rid uint32 // request id, incremented each request
	mid string // model id

	subm sync.RWMutex // cannot use mu for protecting subs
	subs []subFunc    // on-connect mqtt subscriptions

	done chan struct{}         // closed when the transport is closed
	resp map[uint32]chan *resp // responses from iothub

	logger logger.Logger
	cocfg  func(opts *mqtt.ClientOptions)

	webSocket bool

	onConn chan int // channel to listen on for OnConnection events
	connLost chan int // channel to listen on for LostConnection events
}

type resp struct {
	code int
	body []byte

	ver int // twin response only
}

func (tr *Transport) SetLogger(logger logger.Logger) {
	tr.logger = logger
}

// OnConnectionChan returns channel for listening for OnConnection events.
func (tr *Transport) OnConnectionChan() <-chan int {
	return tr.onConn
}

// LostConnectionChan retruns channel for listening for LostConnection events.
func (tr *Transport) LostConnectionChan() <-chan int {
	return tr.connLost
}

func (tr *Transport) Connect(ctx context.Context, creds transport.Credentials) error {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	tlsCfg := &tls.Config{
		RootCAs:       common.RootCAs(),
		Renegotiation: tls.RenegotiateOnceAsClient,
	}
	if crt := creds.GetCertificate(); crt != nil {
		tlsCfg.Certificates = append(tlsCfg.Certificates, *crt)
	}

	username := creds.GetHostName() + "/" + creds.GetDeviceID() + "/api-version=2020-09-30"
	if tr.mid != "" {
		username += "&model-id=" + url.QueryEscape(tr.mid)
	}

	tr.done = make(chan struct{})

	o := mqtt.NewClientOptions()
	o.SetTLSConfig(tlsCfg)
	if tr.webSocket {
		o.AddBroker("wss://" + creds.GetHostName() + ":443/$iothub/websocket") // https://github.com/MicrosoftDocs/azure-docs/issues/21306
	} else {
		o.AddBroker("tls://" + creds.GetHostName() + ":8883")
	}
	o.SetProtocolVersion(4) // 4 = MQTT 3.1.1
	o.SetClientID(creds.GetDeviceID())
	o.SetCredentialsProvider(func() (string, string) {
		if crt := creds.GetCertificate(); crt != nil {
			return username, ""
		}
		// TODO: renew token only when it expires in case an external token provider is used
		// TODO: this can slow down the reconnect feature, so need to figure out max token lifetime
		sas, err := creds.Token(creds.GetHostName(), time.Hour)
		if err != nil {
			tr.logger.Errorf("cannot generate token: %s", err)
			return "", ""
		}
		return username, sas.String()
	})
	o.SetWriteTimeout(30 * time.Second)
	o.SetMaxReconnectInterval(30 * time.Second) // default is 15min, way to long
	o.SetOnConnectHandler(func(c mqtt.Client) {
		tr.logger.Debugf("connection established")
		tr.subm.RLock()
		for _, sub := range tr.subs {
			if err := sub(); err != nil {
				tr.logger.Debugf("on-connect error: %s", err)
			}
		}
		tr.subm.RUnlock()
		tr.onConn <- 1
	})
	o.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		tr.logger.Debugf("connection lost: %v", err)
		tr.connLost <- 1
	})

	if tr.cocfg != nil {
		tr.cocfg(o)
	}

	c := mqtt.NewClient(o)
	if err := contextToken(ctx, c.Connect()); err != nil {
		return err
	}

	tr.did = creds.GetDeviceID()
	tr.conn = c
	return nil
}

// IsConnected returns a bool signifying whether there is a connection or not.
func (tr *Transport) IsConnected() bool {
	return tr.conn.IsConnected()
}

// IsConnectionOpen returns a bool signifying whether the client has an active
// connection to the mqtt broker, i.e. not in disconnected or reconnect mode.
func (tr *Transport) IsConnectionOpen() bool {
	return tr.conn.IsConnectionOpen()
}

type subFunc func() error

// sub invokes the given sub function and if it passes with no error,
// pushes it to the on-re-connect subscriptions list, because the client
// has to resubscribe every reconnect.
func (tr *Transport) sub(sub subFunc) error {
	if err := sub(); err != nil {
		return err
	}
	tr.subm.Lock()
	tr.subs = append(tr.subs, sub)
	tr.subm.Unlock()
	return nil
}

func (tr *Transport) SubscribeEvents(ctx context.Context, mux transport.MessageDispatcher) error {
	return tr.sub(tr.subEvents(ctx, mux))
}

func (tr *Transport) subEvents(ctx context.Context, mux transport.MessageDispatcher) subFunc {
	return func() error {
		return contextToken(ctx, tr.conn.Subscribe(
			"devices/"+tr.did+"/messages/devicebound/#", DefaultQoS, func(_ mqtt.Client, m mqtt.Message) {
				tr.logger.Debugf("%d %s", m.Qos(), m.Topic())
				msg, err := parseEventMessage(m)
				if err != nil {
					tr.logger.Errorf("message parse error: %s", err)
					return
				}
				mux.Dispatch(msg)
			},
		))
	}
}

func (tr *Transport) SubscribeTwinUpdates(ctx context.Context, mux transport.TwinStateDispatcher) error {
	return tr.sub(tr.subTwinUpdates(ctx, mux))
}

func (tr *Transport) subTwinUpdates(ctx context.Context, mux transport.TwinStateDispatcher) subFunc {
	return func() error {
		return contextToken(ctx, tr.conn.Subscribe(
			"$iothub/twin/PATCH/properties/desired/#", DefaultQoS, func(_ mqtt.Client, m mqtt.Message) {
				mux.Dispatch(m.Payload())
			},
		))
	}
}

func parseEventMessage(m mqtt.Message) (*common.Message, error) {
	p, err := parseCloudToDeviceTopic(m.Topic())
	if err != nil {
		return nil, err
	}
	e := &common.Message{
		Payload:    m.Payload(),
		Properties: make(map[string]string, len(p)),
	}
	for k, v := range p {
		switch k {
		case "$.mid":
			e.MessageID = v
		case "$.cid":
			e.CorrelationID = v
		case "$.uid":
			e.UserID = v
		case "$.to":
			e.To = v
		case "$.exp":
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return nil, err
			}
			e.ExpiryTime = &t
		default:
			e.Properties[k] = v
		}
	}
	return e, nil
}

// devices/{device}/messages/devicebound/%24.to=%2Fdevices%2F{device}%2Fmessages%2FdeviceBound&a=b&b=c
func parseCloudToDeviceTopic(s string) (map[string]string, error) {
	s, err := url.QueryUnescape(s)
	if err != nil {
		return nil, err
	}

	// attributes prefixed with $.,
	// e.g. `messageId` becomes `$.mid`, `to` becomes `$.to`, etc.
	i := strings.Index(s, "$.")
	if i == -1 {
		return nil, errors.New("malformed cloud-to-device topic name")
	}

	// any non-URL-encoded semicolon are considered invalid
	prop := strings.ReplaceAll(s[i:], ";", "%3B")

	q, err := url.ParseQuery(prop)
	if err != nil {
		return nil, err
	}

	p := make(map[string]string, len(q))
	for k, v := range q {
		if len(v) != 1 {
			return nil, fmt.Errorf("unexpected number of property values: %d", len(q))
		}
		p[k] = v[0]
	}
	return p, nil
}

func (tr *Transport) RegisterDirectMethods(ctx context.Context, mux transport.MethodDispatcher) error {
	return tr.sub(tr.subDirectMethods(ctx, mux))
}

func (tr *Transport) subDirectMethods(ctx context.Context, mux transport.MethodDispatcher) subFunc {
	return func() error {
		return contextToken(ctx, tr.conn.Subscribe(
			"$iothub/methods/POST/#", DefaultQoS, func(_ mqtt.Client, m mqtt.Message) {
				method, rid, err := parseDirectMethodTopic(m.Topic())
				if err != nil {
					tr.logger.Errorf("parse error: %s", err)
					return
				}
				rc, b, err := mux.Dispatch(method, m.Payload())
				if err != nil {
					tr.logger.Errorf("dispatch error: %s", err)
					return
				}
				dst := fmt.Sprintf("$iothub/methods/res/%d/?$rid=%s", rc, rid)
				if err = tr.send(ctx, dst, DefaultQoS, b); err != nil {
					tr.logger.Errorf("method response error: %s", err)
					return
				}
			},
		))
	}
}

// returns method name and rid
// format: $iothub/methods/POST/{method}/?$rid={rid}
func parseDirectMethodTopic(s string) (string, string, error) {
	const prefix = "$iothub/methods/POST/"

	s, err := url.QueryUnescape(s)
	if err != nil {
		return "", "", err
	}
	u, err := url.Parse(s)
	if err != nil {
		return "", "", err
	}

	p := strings.TrimRight(u.Path, "/")
	if !strings.HasPrefix(p, prefix) {
		return "", "", errors.New("malformed direct method topic")
	}

	q := u.Query()
	if len(q["$rid"]) != 1 {
		return "", "", errors.New("$rid is not available")
	}
	return p[len(prefix):], q["$rid"][0], nil
}

func (tr *Transport) RetrieveTwinProperties(ctx context.Context) ([]byte, error) {
	r, err := tr.request(ctx, "$iothub/twin/GET/?$rid=%x", nil)
	if err != nil {
		return nil, err
	}
	return r.body, nil
}

func (tr *Transport) UpdateTwinProperties(ctx context.Context, b []byte) (int, error) {
	r, err := tr.request(ctx, "$iothub/twin/PATCH/properties/reported/?$rid=%x", b)
	if err != nil {
		return 0, err
	}
	return r.ver, nil
}

func (tr *Transport) request(ctx context.Context, topic string, b []byte) (*resp, error) {
	if err := tr.enableTwinResponses(ctx); err != nil {
		return nil, err
	}
	rid := atomic.AddUint32(&tr.rid, 1) // increment rid counter
	dst := fmt.Sprintf(topic, rid)
	rch := make(chan *resp, 1)
	tr.mu.Lock()
	tr.resp[rid] = rch
	tr.mu.Unlock()
	defer func() {
		tr.mu.Lock()
		delete(tr.resp, rid)
		tr.mu.Unlock()
	}()

	if err := tr.send(ctx, dst, DefaultQoS, b); err != nil {
		return nil, err
	}

	select {
	case r := <-rch:
		if r.code < 200 || r.code > 299 {
			return nil, fmt.Errorf("request failed with %d response code", r.code)
		}
		return r, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (tr *Transport) enableTwinResponses(ctx context.Context) error {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	// already subscribed
	if tr.resp != nil {
		return nil
	}
	if err := tr.sub(tr.subTwinResponses(ctx)); err != nil {
		return err
	}
	tr.resp = make(map[uint32]chan *resp)
	return nil
}

func (tr *Transport) subTwinResponses(ctx context.Context) subFunc {
	return func() error {
		return contextToken(ctx, tr.conn.Subscribe(
			"$iothub/twin/res/#", DefaultQoS, func(_ mqtt.Client, m mqtt.Message) {
				rc, rid, ver, err := parseTwinPropsTopic(m.Topic())
				if err != nil {
					fmt.Printf("parse twin props topic error: %s", err)
					return
				}

				tr.mu.RLock()
				defer tr.mu.RUnlock()
				for r, rch := range tr.resp {
					if int(r) != rid {
						continue
					}
					res := &resp{code: rc, ver: ver, body: m.Payload()}
					select {
					case rch <- res:
						// try to push without a goroutine first
						// if the channel buffer is not busy
					default:
						go func() {
							rch <- res
						}()
					}
					return
				}
				tr.logger.Warnf("unknown rid: %q", rid)
			},
		))
	}
}

// parseTwinPropsTopic parses the given topic name into rc, rid and ver.
// $iothub/twin/res/{rc}/?$rid={rid}(&$version={ver})?
func parseTwinPropsTopic(s string) (int, int, int, error) {
	const prefix = "$iothub/twin/res/"

	u, err := url.Parse(s)
	if err != nil {
		return 0, 0, 0, err
	}

	p := strings.Trim(u.Path, "/")
	if !strings.HasPrefix(p, prefix) {
		return 0, 0, 0, errors.New("malformed twin response topic")
	}
	rc, err := strconv.Atoi(p[len(prefix):])
	if err != nil {
		return 0, 0, 0, err
	}

	q := u.Query()
	if len(q["$rid"]) != 1 {
		return 0, 0, 0, errors.New("$rid is not available")
	}
	rid, err := strconv.ParseInt(q["$rid"][0], 16, 0)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("$rid parse error: %w", err)
	}

	var ver int // version is available only for update responses
	if len(q["$version"]) == 1 {
		ver, err = strconv.Atoi(q["$version"][0])
		if err != nil {
			return 0, 0, 0, err
		}
	}
	return rc, int(rid), ver, nil
}

func encodeProperties(props url.Values) string {
	enc := props.Encode()

	return strings.ReplaceAll(enc, "+", "%20")
}

const rfc3339Milli = "2006-01-02T15:04:05.999Z07:00"

func (tr *Transport) Send(ctx context.Context, msg *common.Message) error {
	// this is just copying functionality from the nodejs sdk, but
	// seems like adding meta attributes does nothing or in some cases,
	// e.g. when $.exp is set the cloud just disconnects.
	u := make(url.Values, len(msg.Properties)+5)
	if msg.MessageID != "" {
		u.Add("$.mid", msg.MessageID)
	}
	if msg.CorrelationID != "" {
		u.Add("$.cid", msg.CorrelationID)
	}
	if msg.UserID != "" {
		u.Add("$.uid", msg.UserID)
	}
	if msg.To != "" {
		u.Add("$.to", msg.To)
	}
	if msg.ExpiryTime != nil && !msg.ExpiryTime.IsZero() {
		u.Add("$.exp", msg.ExpiryTime.UTC().Format(rfc3339Milli))
	}
	if msg.EnqueuedTime != nil && !msg.EnqueuedTime.IsZero() {
		u.Add("$.ctime", msg.EnqueuedTime.UTC().Format(rfc3339Milli))
	}
	for k, v := range msg.Properties {
		u.Add(k, v)
	}

	dst := "devices/" + tr.did + "/messages/events/" + encodeProperties(u)
	qos := DefaultQoS
	if q, ok := msg.TransportOptions["qos"]; ok {
		qos = q.(int) // panic if it's not an int
		if qos != 0 && qos != 1 {
			return fmt.Errorf("invalid QoS value: %d", qos)
		}
	}
	return tr.send(ctx, dst, qos, msg.Payload)
}

func (tr *Transport) send(ctx context.Context, topic string, qos int, b []byte) error {
	tr.mu.RLock()
	if tr.conn == nil {
		tr.mu.RUnlock()
		return errors.New("not connected")
	}
	tr.mu.RUnlock()
	return contextToken(ctx, tr.conn.Publish(topic, byte(qos), false, b))
}

// mqtt lib doesn't support contexts currently
func contextToken(ctx context.Context, t mqtt.Token) error {
	done := make(chan struct{})
	go func() {
		for !t.WaitTimeout(time.Second) {
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
		close(done)
	}()
	select {
	case <-done:
		return t.Error()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (tr *Transport) Close() error {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	select {
	case <-tr.done:
		return nil
	default:
		close(tr.done)
	}
	if tr.conn != nil && tr.conn.IsConnected() {
		tr.conn.Disconnect(250)
		tr.logger.Debugf("disconnected")
	}
	return nil
}

// GetBlobSharedAccessSignature is not available in the MQTT transport.
func (tr *Transport) GetBlobSharedAccessSignature(ctx context.Context, blobName string) (string, string, error) {
	return "", "", fmt.Errorf("unavailable in the MQTT transport")
}

// UploadToBlob is not available in the MQTT transport.
func (tr *Transport) UploadToBlob(ctx context.Context, sasURI string, file io.Reader, size int64) error {
	return fmt.Errorf("unavailable in the MQTT transport")
}

// NotifyUploadComplete is not available in the MQTT transport.
func (tr *Transport) NotifyUploadComplete(ctx context.Context, correlationID string, success bool, statusCode int, statusDescription string) error {
	return fmt.Errorf("unavailable in the MQTT transport")
}

// ListModules list all the registered modules on the device.
func (tr *Transport) ListModules(ctx context.Context) ([]*iotservice.Module, error) {
	return nil, ErrNotImplemented
}

// CreateModule Creates adds the given module to the registry.
func (tr *Transport) CreateModule(ctx context.Context, m *iotservice.Module) (*iotservice.Module, error) {
	return nil, ErrNotImplemented
}

// GetModule retrieves the named module.
func (tr *Transport) GetModule(ctx context.Context, moduleID string) (*iotservice.Module, error) {
	return nil, ErrNotImplemented
}

// UpdateModule updates the given module.
func (tr *Transport) UpdateModule(ctx context.Context, m *iotservice.Module) (*iotservice.Module, error) {
	return nil, ErrNotImplemented
}

// DeleteModule removes the named device module.
func (tr *Transport) DeleteModule(ctx context.Context, m *iotservice.Module) error {
	return ErrNotImplemented
}
