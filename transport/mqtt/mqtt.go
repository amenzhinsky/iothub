package mqtt

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/amenzhinsky/iothub/common"
	"github.com/amenzhinsky/iothub/credentials"
	"github.com/amenzhinsky/iothub/iotutil"
	"github.com/amenzhinsky/iothub/transport"
	"github.com/eclipse/paho.mqtt.golang"
)

const (
	// existing SDKs use QoS 1
	defaultQoS = 1
)

type MQTTOption func(tr *MQTT) error

func WithLogger(l *log.Logger) MQTTOption {
	return func(tr *MQTT) error {
		tr.logger = l
		return nil
	}
}

// New returns new MQTT transport.
// See more: https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-mqtt-support
func New(opts ...MQTTOption) (transport.Transport, error) {
	tr := &MQTT{
		done: make(chan struct{}),
		c2ds: make(chan *transport.Event, 10),
		dmis: make(chan *transport.Call, 10),
		dscs: make(chan []byte, 10),
		resp: make(map[string]chan *resp),

		logger: log.New(os.Stdout, "[mqtt] ", 0),
	}
	for _, opt := range opts {
		if err := opt(tr); err != nil {
			return nil, err
		}
	}
	return tr, nil
}

type MQTT struct {
	mu   sync.RWMutex
	conn mqtt.Client
	ridg iotutil.RIDGenerator

	done chan struct{}         // closed when Close() invoked
	c2ds chan *transport.Event // cloud-to-device messages
	dmis chan *transport.Call  // direct method invocations
	dscs chan []byte           // desired state changes
	resp map[string]chan *resp // responses from iothub

	logger *log.Logger
}

func (tr *MQTT) logf(format string, v ...interface{}) {
	if tr.logger != nil {
		tr.logger.Printf(format, v...)
	}
}

func (tr *MQTT) Connect(ctx context.Context, deviceID string, sasFunc transport.AuthFunc) error {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.conn != nil {
		return errors.New("already connected")
	}

	host, sas, err := sasFunc(ctx, "")
	if err != nil {
		return err
	}

	o := mqtt.NewClientOptions()
	o.AddBroker("tls://" + host + ":8883")
	o.SetClientID(deviceID)
	o.SetUsername(host + "/" + deviceID + "/api-version=" + common.APIVersion)
	o.SetPassword(sas)
	o.SetTLSConfig(credentials.TLSConfig(host))
	o.SetAutoReconnect(true)
	o.SetCleanSession(false) // TODO: investigate ability to enable it
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

	// TODO: on-demand subscriptions
	for topic, handler := range map[string]mqtt.MessageHandler{
		"devices/" + deviceID + "/messages/devicebound/#": tr.cloudToDeviceHandler,
		"$iothub/methods/POST/#":                          tr.directMethodHandler,
		"$iothub/twin/res/#":                              tr.twinResponseHandler,
		"$iothub/twin/PATCH/properties/desired/#":         tr.desiredStateChangesHandler,
	} {
		if t := c.Subscribe(topic, defaultQoS, handler); t.Wait() && t.Error() != nil {
			c.Disconnect(250)
			return t.Error()
		}
	}

	tr.conn = c
	return nil
}

// mqtt library wraps errors with fmt.Errorf.
func (tr *MQTT) IsNetworkError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Network Error")
}

func (tr *MQTT) cloudToDeviceHandler(_ mqtt.Client, m mqtt.Message) {
	p, err := parseCloudToDeviceTopic(m.Topic())
	if err != nil {
		tr.logf("cloud-to-device error: %s", err)
		return
	}
	select {
	case tr.c2ds <- &transport.Event{
		Payload:    m.Payload(),
		Properties: p,
	}:
	case <-tr.done:
	}
}

func (tr *MQTT) C2D() chan *transport.Event {
	return tr.c2ds
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

func (tr *MQTT) directMethodHandler(_ mqtt.Client, m mqtt.Message) {
	method, rid, err := parseDirectMethodTopic(m.Topic())
	if err != nil {
		tr.logf("direct-method error: %s", err)
		return
	}
	select {
	case tr.dmis <- &transport.Call{
		RID:     rid,
		Method:  method,
		Payload: m.Payload(),
	}:
	case <-tr.done:
	}
}

func (tr *MQTT) DMI() chan *transport.Call {
	return tr.dmis
}

func (tr *MQTT) RespondDirectMethod(ctx context.Context, rid string, code int, b []byte) error {
	return tr.send(ctx, fmt.Sprintf("$iothub/methods/res/%d/?$rid=%s", code, rid), b)
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

func (tr *MQTT) twinResponseHandler(_ mqtt.Client, m mqtt.Message) {
	rc, rid, ver, err := parseTwinPropsTopic(m.Topic())
	if err != nil {
		tr.logf("error parsing twin response topic: %s", err)
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
}

type resp struct {
	code int
	ver  int // twin response only
	body []byte
}

func (tr *MQTT) RetrieveTwinProperties(ctx context.Context) ([]byte, error) {
	r, err := tr.request(ctx, "$iothub/twin/GET/?$rid=%s", nil)
	if err != nil {
		return nil, err
	}
	return r.body, nil
}

func (tr *MQTT) UpdateTwinProperties(ctx context.Context, b []byte) (int, error) {
	r, err := tr.request(ctx, "$iothub/twin/PATCH/properties/reported/?$rid=%s", b)
	if err != nil {
		return 0, err
	}
	return r.ver, nil
}

func (tr *MQTT) request(ctx context.Context, topic string, b []byte) (*resp, error) {
	rid := tr.ridg.Next()
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

	if err := tr.send(ctx, dst, b); err != nil {
		return nil, err
	}

	select {
	case r := <-rch:
		if r.code < 200 && r.code > 299 {
			return nil, fmt.Errorf("request failed with %d response code", r.code)
		}
		return r, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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

func (tr *MQTT) desiredStateChangesHandler(_ mqtt.Client, m mqtt.Message) {
	select {
	case tr.dscs <- m.Payload():
	case <-tr.done:
	}
}

func (tr *MQTT) DSC() chan []byte {
	return tr.dscs
}

func (tr *MQTT) PublishEvent(ctx context.Context, ev *transport.Event) error {
	u := make(url.Values, len(ev.Properties))
	for k, v := range ev.Properties {
		u[k] = []string{v}
	}
	return tr.send(ctx, "devices/"+ev.DeviceID+"/messages/events/"+u.Encode(), ev.Payload)
}

func (tr *MQTT) send(ctx context.Context, topic string, b []byte) error {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	if tr.conn == nil {
		return errors.New("not connected")
	}

	t := tr.conn.Publish(topic, defaultQoS, false, b)
	t.Wait()
	return t.Error()
}

func (tr *MQTT) Close() error {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	select {
	case <-tr.done:
		return nil
	default:
		close(tr.done)
	}
	if tr.conn.IsConnected() {
		tr.conn.Disconnect(250)
		tr.logf("disconnected")
	}
	tr.conn = nil
	return nil
}
