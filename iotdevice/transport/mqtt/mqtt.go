package mqtt

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amenzhinsky/golang-iothub/common"
	"github.com/amenzhinsky/golang-iothub/iotdevice/transport"
	"github.com/eclipse/paho.mqtt.golang"
)

const (
	// existing SDKs use QoS 1
	defaultQoS = 1
)

type TransportOption func(tr *Transport)

func WithLogger(l *log.Logger) TransportOption {
	return func(tr *Transport) {
		tr.logger = l
	}
}

// New returns new Transport transport.
// See more: https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-mqtt-support
func New(opts ...TransportOption) transport.Transport {
	tr := &Transport{done: make(chan struct{})}
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

	done chan struct{}         // closed when the transport is closed
	resp map[string]chan *resp // responses from iothub

	logger *log.Logger
}

func (tr *Transport) logf(format string, v ...interface{}) {
	if tr.logger != nil {
		tr.logger.Printf(format, v...)
	}
}

func (tr *Transport) Connect(
	ctx context.Context,
	tlsConfig *tls.Config,
	deviceID string,
	auth transport.AuthFunc,
) error {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.conn != nil {
		return errors.New("already connected")
	}

	host := tlsConfig.ServerName
	pass := ""
	if auth != nil {
		var err error
		host, pass, err = auth(ctx, "")
		if err != nil {
			return err
		}
	}

	o := mqtt.NewClientOptions()
	o.AddBroker("tls://" + host + ":8883")
	o.SetClientID(deviceID)
	o.SetUsername(host + "/" + deviceID + "/api-version=" + common.APIVersion)
	o.SetPassword(pass)
	o.SetTLSConfig(tlsConfig)
	o.SetAutoReconnect(true)
	o.SetOnConnectHandler(func(_ mqtt.Client) {
		tr.logf("connection established")
	})
	o.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		tr.logf("connection lost: %v", err)
	})

	c := mqtt.NewClient(o)
	if t := c.Connect(); t.Wait() && t.Error() != nil {
		return t.Error()
	}

	tr.did = deviceID
	tr.conn = c
	return nil
}

func (tr *Transport) SubscribeEvents(ctx context.Context, mux transport.MessageDispatcher) error {
	t := tr.conn.Subscribe(
		"devices/"+tr.did+"/messages/devicebound/#", defaultQoS, func(_ mqtt.Client, m mqtt.Message) {
			msg, err := parseEventMessage(m)
			if err != nil {
				tr.logf("parse error: %s", err)
				return
			}
			mux.Dispatch(msg)
		},
	)
	t.Wait()
	return t.Error()
}

func (tr *Transport) SubscribeTwinUpdates(ctx context.Context, mux transport.TwinStateDispatcher) error {
	t := tr.conn.Subscribe(
		"$iothub/twin/PATCH/properties/desired/#", defaultQoS, func(_ mqtt.Client, m mqtt.Message) {
			mux.Dispatch(m.Payload())
		},
	)
	t.Wait()
	return t.Error()
}

// mqtt library wraps errors with fmt.Errorf.
func (tr *Transport) IsNetworkError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Network Error")
}

func parseEventMessage(m mqtt.Message) (*common.Message, error) {
	p, err := parseCloudToDeviceTopic(m.Topic())
	if err != nil {
		return nil, err
	}
	e := &common.Message{
		Payload:    string(m.Payload()),
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
	q, err := url.QueryUnescape(s)
	if err != nil {
		return nil, err
	}

	// attributes prefixed with $.,
	// e.g. `messageId` becomes `$.mid`, `to` becomes `$.to`, etc.
	i := strings.Index(q, "$.")
	if i == -1 {
		return nil, errors.New("malformed cloud-to-device topic name")
	}
	v, err := url.ParseQuery(q[i:])
	if err != nil {
		return nil, err
	}

	p := make(map[string]string, len(v))
	for k, x := range v {
		if len(x) != 1 {
			return nil, fmt.Errorf("unexpected number of property values: %d", len(v))
		}
		p[k] = x[0]
	}
	return p, nil
}

func (tr *Transport) RegisterDirectMethods(ctx context.Context, mux transport.MethodDispatcher) error {
	t := tr.conn.Subscribe(
		"$iothub/methods/POST/#", defaultQoS, func(_ mqtt.Client, m mqtt.Message) {
			method, rid, err := parseDirectMethodTopic(m.Topic())
			if err != nil {
				tr.logf("parse error: %s", err)
				return
			}
			rc, b, err := mux.Dispatch(method, m.Payload())
			if err != nil {
				tr.logf("dispatch error: %s", err)
				return
			}
			dst := fmt.Sprintf("$iothub/methods/res/%d/?$rid=%s", rc, rid)
			if err = tr.send(ctx, dst, defaultQoS, b); err != nil {
				tr.logf("method response error: %s", err)
				return
			}
		},
	)
	t.Wait()
	return t.Error()
}

// returns method name and rid
// format: $iothub/methods/POST/{method}/?$rid={rid}
func parseDirectMethodTopic(s string) (string, string, error) {
	ss := strings.Split(s, "/")
	if len(ss) != 5 {
		return "", "", errors.New("malformed direct-method topic name")
	}
	if !strings.HasPrefix(ss[4], "?$rid=") {
		return "", "", errors.New("malformed direct-method topic name")
	}
	return ss[3], ss[4][6:], nil
}

type resp struct {
	code int
	ver  int // twin response only
	body []byte
}

func (tr *Transport) RetrieveTwinProperties(ctx context.Context) ([]byte, error) {
	r, err := tr.request(ctx, "$iothub/twin/GET/?$rid=%s", nil)
	if err != nil {
		return nil, err
	}
	return r.body, nil
}

func (tr *Transport) UpdateTwinProperties(ctx context.Context, b []byte) (int, error) {
	r, err := tr.request(ctx, "$iothub/twin/PATCH/properties/reported/?$rid=%s", b)
	if err != nil {
		return 0, err
	}
	return r.ver, nil
}

func (tr *Transport) request(ctx context.Context, topic string, b []byte) (*resp, error) {
	if err := tr.enableTwinResponses(); err != nil {
		return nil, err
	}
	rid := fmt.Sprintf("%d", atomic.AddUint32(&tr.rid, 1)) // increment rid counter
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

	if err := tr.send(ctx, dst, defaultQoS, b); err != nil {
		return nil, err
	}

	select {
	case r := <-rch:
		if r.code < 200 && r.code > 299 {
			return nil, fmt.Errorf("request failed with %d response code", r.code)
		}
		return r, nil
	case <-time.After(30 * time.Second):
		return nil, errors.New("request timed out")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (tr *Transport) enableTwinResponses() error {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	// already subscribed
	if tr.resp != nil {
		return nil
	}

	if t := tr.conn.Subscribe(
		"$iothub/twin/res/#", defaultQoS, func(_ mqtt.Client, m mqtt.Message) {
			rc, rid, ver, err := parseTwinPropsTopic(m.Topic())
			if err != nil {
				// TODO
				fmt.Printf("error: %s", err)
				return
			}

			tr.mu.RLock()
			defer tr.mu.RUnlock()
			for r, rch := range tr.resp {
				if r != rid {
					continue
				}
				select {
				case rch <- &resp{code: rc, ver: ver, body: m.Payload()}:
				default:
					// we cannot allow blocking here,
					// buffered channel should solve it.
					panic("response sending blocked")
				}
				return
			}
			tr.logf("unknown rid: %q", rid)
		},
	); t.Wait() && t.Error() != nil {
		return t.Error()
	}

	tr.resp = make(map[string]chan *resp)
	return nil
}

var twinResponseRegexp = regexp.MustCompile(
	`\$iothub/twin/res/(\d+)/\?\$rid=(\w+)(?:&\$version=(\d+))?`,
)

// parseTwinPropsTopic parses the given topic name into rc, rid and ver.
// $iothub/twin/res/{rc}/?$rid={rid}(&$version={ver})?
func parseTwinPropsTopic(s string) (int, string, int, error) {
	ss := twinResponseRegexp.FindStringSubmatch(s)
	if ss == nil {
		return 0, "", 0, errors.New("malformed topic name")
	}

	// regexp already returns valid strings of digits.
	rc, _ := strconv.Atoi(ss[1])
	ver, _ := strconv.Atoi(ss[3])

	return rc, ss[2], ver, nil
}

func (tr *Transport) Send(ctx context.Context, msg *common.Message) error {
	// this is just copying functionality from the nodejs sdk, but
	// seems like adding meta attributes does nothing or in some cases,
	// e.g. when $.exp is set the cloud just disconnects.
	u := make(url.Values, len(msg.Properties)+5)
	if msg.MessageID != "" {
		u["$.mid"] = []string{msg.MessageID}
	}
	if msg.CorrelationID != "" {
		u["$.cid"] = []string{msg.CorrelationID}
	}
	if msg.UserID != "" {
		u["$.uid"] = []string{msg.UserID}
	}
	if msg.To != "" {
		u["$.to"] = []string{msg.To}
	}
	if msg.ExpiryTime != nil && !msg.ExpiryTime.IsZero() {
		u["$.exp"] = []string{msg.ExpiryTime.UTC().Format(time.RFC3339)}
	}
	for k, v := range msg.Properties {
		u[k] = []string{v}
	}

	dst := "devices/" + tr.did + "/messages/events/" + u.Encode()
	qos := defaultQoS
	if q, ok := msg.TransportOptions["qos"]; ok {
		qos = q.(int)
	}
	return tr.send(ctx, dst, qos, []byte(msg.Payload))
}

func (tr *Transport) send(ctx context.Context, topic string, qos int, b []byte) error {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	if tr.conn == nil {
		return errors.New("not connected")
	}
	t := tr.conn.Publish(topic, defaultQoS, false, b)
	t.Wait()
	return t.Error()
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
		tr.logf("disconnected")
	}
	return nil
}
