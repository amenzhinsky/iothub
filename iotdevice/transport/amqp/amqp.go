package amqp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/amenzhinsky/golang-iothub/common"
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
	done chan struct{}

	d2cSender *amqp.Sender
}

func (tr *Transport) Connect(
	ctx context.Context,
	tlsConfig *tls.Config,
	deviceID string,
	authFunc transport.AuthFunc,
) (chan *transport.Message, chan *transport.Invocation, chan *transport.TwinState, error) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.conn != nil {
		return nil, nil, nil, errors.New("already connected")
	}

	host := tlsConfig.ServerName
	token := ""

	if authFunc != nil {
		// SAS uri for amqp has to be: hostname + "/devices/" + deviceID
		var err error
		host, token, err = authFunc(ctx, "/devices/"+deviceID)
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

	//dmiOpts := []amqp.LinkOption{
	//	amqp.LinkSourceAddress("/devices/" + deviceID + "/methods/devicebound"),
	//	amqp.LinkProperty("com.microsoft:client-version", "azure-iot-device/1.3.2"),
	//	amqp.LinkProperty("com.microsoft:api-version", common.APIVersion),
	//	amqp.LinkProperty("com.microsoft:channel-correlation-id", deviceID),
	//}
	//
	//dmiSend, err := c.Sess().NewSender(dmiOpts...)
	//if err != nil {
	//	return nil, nil, nil, err
	//}
	//_ = dmiSend
	//
	//dmiRecv, err := c.Sess().NewReceiver(dmiOpts...)
	//if err != nil {
	//	return nil, nil, nil, err
	//}
	//
	//for {
	//	msg, err := dmiRecv.Receive(ctx)
	//	if err != nil {
	//
	//	}
	//	fmt.Printf("--> %#v\n", msg)
	//	fmt.Printf("--> %#v\n", err.Error())
	//}
	c2d, err := c.Sess().NewReceiver(
		amqp.LinkSourceAddress("/devices/" + deviceID + "/messages/devicebound"),
	)
	if err != nil {
		return nil, nil, nil, err
	}

	go func() {
		defer close(tr.c2ds)

		// TODO: copy from iotservice
		for {
			msg, err := c2d.Receive(ctx)
			if err != nil {
				if _, ok := err.(amqp.DetachError); ok {
					c.Close()
				}

				select {
				case tr.c2ds <- &transport.Message{Err: err}:
					return
				case <-tr.done:
					return
				}
			}

			props := make(map[string]string, len(msg.ApplicationProperties))
			for k, v := range msg.ApplicationProperties {
				props[k] = fmt.Sprint(v)
			}

			select {
			case tr.c2ds <- &transport.Message{
				//DeviceID:   deviceID,
				//Payload:    msg.Data[0],
				//Properties: props,
			}:
				msg.Accept()
			case <-tr.done:
				return
			}
		}
	}()

	tr.conn = c
	return tr.c2ds, nil, nil, nil
}

func (tr *Transport) IsNetworkError(err error) bool {
	return false
}

func (tr *Transport) Send(ctx context.Context, deviceID string, msg *common.Message) error {
	if err := tr.checkConnection(); err != nil {
		return err
	}

	if msg.To == "" {
		msg.To = "/devices/" + deviceID + "/messages/events" // required
	}
	if err := tr.enablePublishing(msg.To); err != nil {
		return err
	}

	// TODO: see iotservice.SendEvent
	props := &amqp.MessageProperties{
		To: msg.To,
	}
	if msg.UserID != "" {
		props.UserID = []byte(msg.UserID)
	}
	if msg.MessageID != "" {
		props.MessageID = msg.MessageID
	}
	if msg.CorrelationID != "" {
		props.CorrelationID = msg.CorrelationID
	}
	if !msg.ExpiryTime.IsZero() {
		props.AbsoluteExpiryTime = msg.ExpiryTime
	}
	appProps := make(map[string]interface{}, len(msg.Properties))
	for k, v := range msg.Properties {
		appProps[k] = v
	}
	return tr.d2cSender.Send(ctx, &amqp.Message{
		Data:                  [][]byte{msg.Payload},
		Properties:            props,
		ApplicationProperties: appProps,
	})
}

// enablePublishing initializes the sender link just once,
// because we don't want to do this every `SendEvent` call.
func (tr *Transport) enablePublishing(to string) error {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.d2cSender != nil {
		return nil
	}
	var err error
	tr.d2cSender, err = tr.conn.Sess().NewSender(
		amqp.LinkTargetAddress(to),
	)
	return err
}

func (tr *Transport) RespondDirectMethod(ctx context.Context, rid string, code int, payload []byte) error {
	return nil
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
	if tr.d2cSender != nil {
		tr.d2cSender.Close()
	}
	return tr.conn.Close()
}
