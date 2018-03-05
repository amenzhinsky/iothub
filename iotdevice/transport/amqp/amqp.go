package amqp

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"log"
	"strings"
	"sync"

	"github.com/amenzhinsky/golang-iothub/common"
	"github.com/amenzhinsky/golang-iothub/common/commonamqp"
	"github.com/amenzhinsky/golang-iothub/eventhub"
	"github.com/amenzhinsky/golang-iothub/iotdevice/transport"
	"pack.ag/amqp"
)

// TransportOption is transport configuration option.
type TransportOption func(tr *Transport) error

// WithLogger overrides transport logger.
func WithLogger(l *log.Logger) TransportOption {
	return func(c *Transport) error {
		c.logger = l
		return nil
	}
}

// New creates new amqp iothub transport.
func New(opts ...TransportOption) (transport.Transport, error) {
	tr := &Transport{
		c2ds: make(chan *transport.Message, 10),
		dmis: make(chan *transport.Invocation, 10),
		done: make(chan struct{}),
	}
	for _, opt := range opts {
		if err := opt(tr); err != nil {
			return nil, err
		}
	}
	return tr, nil
}

type Transport struct {
	mu     sync.RWMutex
	conn   *eventhub.Client
	logger *log.Logger

	c2ds chan *transport.Message
	dmis chan *transport.Invocation
	done chan struct{}

	d2cSend *amqp.Sender
	dmiSend *amqp.Sender
}

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

	host := tlsConfig.ServerName
	token := ""

	if auth != nil {
		// SAS uri for amqp has to be: hostname + "/devices/" + deviceID
		var err error
		host, token, err = auth(ctx, "/devices/"+deviceID)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	c, err := eventhub.Dial(host, tlsConfig)
	if err != nil {
		return nil, nil, nil, err
	}
	defer func() {
		if err != nil {
			c.Close()
		}
	}()

	// put token in the background when sas authentication is on
	if token != "" {
		if err := c.PutTokenContinuously(ctx, host+"/devices/"+deviceID, token, tr.done); err != nil {
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
	tr.dmiSend, err = c.Sess().NewSender(
		amqp.LinkTargetAddress(addr),
		amqp.LinkProperty("com.microsoft:api-version", common.APIVersion),
		amqp.LinkProperty("com.microsoft:channel-correlation-id", deviceID),
		amqp.LinkProperty("com.microsoft:client-version", "azure-iot-device/1.3.2"),
	)
	if err != nil {
		return nil, nil, nil, err
	}

	dmiRecv, err := c.Sess().NewReceiver(
		amqp.LinkSourceAddress(addr),
		amqp.LinkProperty("com.microsoft:api-version", common.APIVersion),
		amqp.LinkProperty("com.microsoft:channel-correlation-id", deviceID),
		amqp.LinkProperty("com.microsoft:client-version", "azure-iot-device/1.3.2"),
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

	c2d, err := c.Sess().NewReceiver(
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
				// TODO: find a way to handle disconnect errors
				//if _, ok := err.(amqp.DetachError); ok {
				//	c.Close()
				//}

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

	tr.conn = c
	return tr.c2ds, tr.dmis, nil, nil
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
		msg.To = "/devices/" + deviceID + "/messages/events" // required
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

func (tr *Transport) RetrieveTwinProperties(ctx context.Context) (payload []byte, err error) {
	return nil, nil
}

func (tr *Transport) UpdateTwinProperties(ctx context.Context, payload []byte) (version int, err error) {
	return 0, nil
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
