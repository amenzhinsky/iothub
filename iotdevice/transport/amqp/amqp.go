package amqp

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/amenzhinsky/golang-iothub/common"
	"github.com/amenzhinsky/golang-iothub/common/commonamqp"
	"github.com/amenzhinsky/golang-iothub/eventhub"
	"github.com/amenzhinsky/golang-iothub/iotdevice/transport"
	"pack.ag/amqp"
)

// TransportOption is transport configuration option.
type TransportOption func(tr *Transport)

// WithLogger overrides transport logger.
func WithLogger(l *log.Logger) TransportOption {
	return func(c *Transport) {
		c.logger = l
	}
}

// New creates new amqp iothub transport.
func New(opts ...TransportOption) transport.Transport {
	tr := &Transport{
		c2ds: make(chan *transport.Message, 10),
		dmis: make(chan *transport.Invocation, 10),
		tscs: make(chan *transport.TwinState, 10),
		done: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(tr)
	}
	return tr
}

type Transport struct {
	mu     sync.RWMutex
	conn   *eventhub.Client
	logger *log.Logger

	did string // device id
	cid uint64 // correlation id counter

	c2ds chan *transport.Message
	dmis chan *transport.Invocation
	tscs chan *transport.TwinState
	done chan struct{}

	d2cSend *amqp.Sender
	dmiSend *amqp.Sender
}

const (
	propAPIVersion    = "com.microsoft:api-version"
	propClientVersion = "com.microsoft:client-version"
	propCorrelationID = "com.microsoft:channel-correlation-id"

	clientVersion = "azure-iot-device/1.3.2"
)

func (tr *Transport) Connect(
	ctx context.Context,
	tlsConfig *tls.Config,
	deviceID string,
	auth transport.AuthFunc,
) (chan *transport.Message, chan *transport.Invocation, chan *transport.TwinState, error) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.conn != nil {
		return nil, nil, nil, errors.New("already connected")
	}
	tr.did = deviceID

	host := tlsConfig.ServerName
	token := ""

	var err error
	if auth != nil {
		// SAS uri for amqp has to be: hostname + "/devices/" + deviceID
		host, token, err = auth(ctx, "/devices/"+deviceID)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	tr.conn, err = eventhub.Dial(host, tlsConfig)
	if err != nil {
		return nil, nil, nil, err
	}
	defer func() {
		if err != nil {
			tr.conn.Close()
			tr.conn = nil
		}
	}()

	// put token in the background when sas authentication is on
	if token != "" {
		if err := tr.conn.PutTokenContinuously(ctx, host+"/devices/"+deviceID, token, tr.done); err != nil {
			return nil, nil, nil, err
		}
	}

	// interrupt all receivers when transport is closed
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-tr.done
		cancel()
	}()

	addr := "/devices/" + deviceID + "/methods/devicebound"
	tr.dmiSend, err = tr.conn.Sess().NewSender(
		amqp.LinkTargetAddress(addr),
		amqp.LinkProperty(propAPIVersion, common.APIVersion),
		amqp.LinkProperty(propCorrelationID, deviceID),
		amqp.LinkProperty(propClientVersion, clientVersion),
	)
	if err != nil {
		return nil, nil, nil, err
	}

	dmiRecv, err := tr.conn.Sess().NewReceiver(
		amqp.LinkSourceAddress(addr),
		amqp.LinkProperty(propAPIVersion, common.APIVersion),
		amqp.LinkProperty(propCorrelationID, deviceID),
		amqp.LinkProperty(propClientVersion, clientVersion),
		amqp.LinkCredit(100),
	)
	if err != nil {
		return nil, nil, nil, err
	}

	go func() {
		defer close(tr.dmis)

		for {
			msg, err := dmiRecv.Receive(ctx)
			if err != nil {
				tr.dmis <- &transport.Invocation{Err: err}
				return
			}
			tr.dmis <- &transport.Invocation{
				RID:     msg.Properties.CorrelationID.(amqp.UUID).String(),
				Method:  msg.ApplicationProperties["IoThub-methodname"].(string),
				Payload: msg.Data[0],
			}
		}
	}()

	c2d, err := tr.conn.Sess().NewReceiver(
		amqp.LinkSourceAddress("/devices/" + deviceID + "/messages/devicebound"),
	)
	if err != nil {
		return nil, nil, nil, err
	}

	go func() {
		defer close(tr.c2ds)

		for {
			msg, err := c2d.Receive(ctx)
			if err != nil {
				select {
				case tr.c2ds <- &transport.Message{Err: err}:
					return
				case <-tr.done:
					return
				}
			}

			select {
			case tr.c2ds <- &transport.Message{Msg: commonamqp.FromAMQPMessage(msg)}:
				msg.Accept()
			case <-tr.done:
				return
			}
		}
	}()

	twinSend, twinRecv, err := tr.twinSendRecv()
	if err != nil {
		return nil, nil, nil, err
	}

	go func() {
		defer close(tr.tscs)

		if err = twinSend.Send(ctx, tr.twinRequest(
			"PUT",
			"/notifications/twin/properties/desired",
			nil,
		)); err != nil {
			tr.tscs <- &transport.TwinState{Err: err}
			return
		}

		msg, err := twinRecv.Receive(ctx)
		if err != nil {
			tr.tscs <- &transport.TwinState{Err: err}
			return
		}

		if err = checkTwinResponse(msg); err != nil {
			tr.tscs <- &transport.TwinState{Err: err}
			return
		}

		for {
			msg, err := twinRecv.Receive(ctx)
			if err != nil {
				tr.tscs <- &transport.TwinState{Err: err}
				return
			}
			tr.tscs <- &transport.TwinState{
				Payload: msg.Data[0],
			}
		}
	}()

	return tr.c2ds, tr.dmis, tr.tscs, nil
}

func (tr *Transport) IsNetworkError(err error) bool {
	return false
}

func (tr *Transport) Send(ctx context.Context, deviceID string, msg *common.Message) error {
	var err error
	if err = tr.checkConnection(); err != nil {
		return err
	}

	if msg.To == "" {
		msg.To = "/devices/" + tr.did + "/messages/events" // required
	}
	props := make(map[string]interface{}, len(msg.Properties))
	for k, v := range msg.Properties {
		props[k] = v
	}

	// lock mu here to open the sending linkSend just once,
	// plus amqp.Send might not be thread-safe.
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.d2cSend == nil {
		tr.d2cSend, err = tr.conn.Sess().NewSender(
			// TODO: msg.To can be different from the default value
			amqp.LinkTargetAddress(msg.To),
		)
	}
	return tr.d2cSend.Send(ctx, commonamqp.ToAMQPMessage(msg))
}

func (tr *Transport) RespondDirectMethod(ctx context.Context, rid string, rc int, data []byte) error {
	// convert rid back into amqp.UUID
	cid := amqp.UUID{}
	if _, err := hex.Decode(cid[:], []byte(strings.Replace(rid, "-", "", 4))); err != nil {
		return err
	}
	return tr.dmiSend.Send(ctx, &amqp.Message{
		Data: [][]byte{data},
		Properties: &amqp.MessageProperties{
			CorrelationID: cid,
		},
		ApplicationProperties: map[string]interface{}{
			"IoThub-status": int32(rc),
		},
	})
}

func (tr *Transport) RetrieveTwinProperties(ctx context.Context) ([]byte, error) {
	send, recv, err := tr.twinSendRecv()
	if err != nil {
		return nil, err
	}
	defer func() {
		send.Close()
		recv.Close()
	}()

	if err = send.Send(ctx, tr.twinRequest("GET", "", nil)); err != nil {
		return nil, err
	}

	msg, err := recv.Receive(ctx)
	if err != nil {
		return nil, err
	}
	if err = checkTwinResponse(msg); err != nil {
		return nil, err
	}
	return msg.Data[0], nil
}

func (tr *Transport) twinRequest(action, resource string, body []byte) *amqp.Message {
	return &amqp.Message{
		Data: [][]byte{body},
		Annotations: amqp.Annotations{
			"operation": action,
			"resource":  resource,
		},
		Properties: &amqp.MessageProperties{
			CorrelationID: atomic.AddUint64(&tr.cid, 1),
		},
	}
}

// TODO: open this links once
func (tr *Transport) twinSendRecv() (*amqp.Sender, *amqp.Receiver, error) {
	cid, err := eventhub.RandString()
	if err != nil {
		return nil, nil, err
	}

	send, err := tr.conn.Sess().NewSender(
		amqp.LinkTargetAddress("/devices/"+tr.did+"/twin"),
		amqp.LinkProperty(propAPIVersion, common.APIVersion),
		amqp.LinkProperty(propCorrelationID, "twin:"+cid),
		amqp.LinkProperty(propClientVersion, clientVersion),
	)
	if err != nil {
		return nil, nil, err
	}

	recv, err := tr.conn.Sess().NewReceiver(
		amqp.LinkSourceAddress("/devices/"+tr.did+"/twin"),
		amqp.LinkProperty(propAPIVersion, common.APIVersion),
		amqp.LinkProperty(propCorrelationID, "twin:"+cid),
		amqp.LinkProperty(propClientVersion, clientVersion),
	)
	if err != nil {
		send.Close()
		return nil, nil, err
	}
	return send, recv, nil
}

func checkTwinResponse(msg *amqp.Message) error {
	if rc, ok := msg.Annotations["status"].(int32); !ok || rc != 200 {
		return fmt.Errorf("unexpected response status = %v", msg.Annotations["status"])
	}
	return nil
}

func (tr *Transport) UpdateTwinProperties(ctx context.Context, data []byte) (int, error) {
	send, recv, err := tr.twinSendRecv()
	if err != nil {
		return 0, err
	}
	defer func() {
		send.Close()
		recv.Close()
	}()

	if err = send.Send(ctx, tr.twinRequest("PATCH", "/properties/reported", data)); err != nil {
		return 0, err
	}

	msg, err := recv.Receive(ctx)
	if err != nil {
		return 0, err
	}
	if err = checkTwinResponse(msg); err != nil {
		return 0, err
	}

	ver := msg.Annotations["version"].(int64)
	return int(ver), nil
}

func (tr *Transport) checkConnection() error {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	select {
	case <-tr.done:
		return errors.New("closed")
	default:
	}
	if tr.conn == nil {
		return errors.New("not connected")
	}
	return nil
}

func (tr *Transport) Close() error {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	select {
	case <-tr.done:
		return nil
	default:
		close(tr.done)
	}
	if tr.d2cSend != nil {
		tr.d2cSend.Close()
	}
	return tr.conn.Close()
}
