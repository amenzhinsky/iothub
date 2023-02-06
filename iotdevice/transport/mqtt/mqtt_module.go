package mqtt

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/dangeroushobo/iothub/common"
	"github.com/dangeroushobo/iothub/iotdevice/transport"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// New returns new Transport transport.
// See more: https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-mqtt-support
func NewModuleTransport(opts ...TransportOption) *ModuleTransport {
	tr := &ModuleTransport{
		Transport: Transport{
			done: make(chan struct{}),
		},
	}
	for _, opt := range opts {
		opt(&tr.Transport)
	}
	return tr
}

type ModuleTransport struct {
	Transport
	mid         string // module id
	gid         string // generation id
	edgeGateway bool   // connect via edge gateway
}

func (tr *ModuleTransport) Connect(ctx context.Context, creds transport.Credentials) error {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.conn != nil {
		return errors.New("already connected")
	}

	tlsCfg := &tls.Config{}

	if creds.UseEdgeGateway() {
		if tb, err := common.TrustBundle(creds.GetWorkloadURI()); err != nil {
			tlsCfg.InsecureSkipVerify = true // x509: certificate signed by unknown authority if missing
			tr.logger.Warnf("error getting trust bundle: %s", err)
		} else {
			tlsCfg.RootCAs = tb
		}
	} else {
		tlsCfg.RootCAs = common.RootCAs()
	}

	if crt := creds.GetCertificate(); crt != nil {
		tlsCfg.Certificates = append(tlsCfg.Certificates, *crt)
	}

	username := creds.GetHostName() + "/" + creds.GetDeviceID() + "/" + creds.GetModuleID() + "/?api-version=2018-06-30"
	o := mqtt.NewClientOptions()
	o.SetTLSConfig(tlsCfg)
	if tr.webSocket {
		o.AddBroker("wss://" + creds.GetBroker() + ":443/$iothub/websocket") // https://github.com/MicrosoftDocs/azure-docs/issues/21306
	} else {
		o.AddBroker("tls://" + creds.GetBroker() + ":8883")
	}
	o.SetProtocolVersion(4) // 4 = MQTT 3.1.1
	o.SetClientID(creds.GetDeviceID() + "/" + creds.GetModuleID())
	o.SetCredentialsProvider(func() (string, string) {
		if crt := creds.GetCertificate(); crt != nil {
			return username, ""
		}
		audience := creds.GetHostName() + "/devices/" + url.QueryEscape(creds.GetDeviceID()) + "/modules/" + url.QueryEscape(creds.GetModuleID())
		if creds.UseEdgeGateway() {
			sas, err := creds.TokenFromEdge(creds.GetWorkloadURI(), creds.GetModuleID(), creds.GetGenerationID(), audience, time.Hour)
			if err != nil {
				tr.logger.Errorf("cannot generate token: %s", err)
				return "", ""
			}
			return username, sas.String()
		}

		sas, err := creds.Token(url.QueryEscape(audience), time.Hour)
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
	})
	o.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		tr.logger.Debugf("connection lost: %v", err)
	})

	if tr.cocfg != nil {
		tr.cocfg(o)
	}

	c := mqtt.NewClient(o)
	if err := contextToken(ctx, c.Connect()); err != nil {
		return err
	}

	tr.did = creds.GetDeviceID()
	tr.mid = creds.GetModuleID()
	tr.gid = creds.GetGenerationID()
	tr.edgeGateway = creds.UseEdgeGateway()
	tr.conn = c
	return nil
}

func (tr *ModuleTransport) SubscribeEvents(ctx context.Context, mux transport.MessageDispatcher) error {
	return tr.sub(tr.subEvents(ctx, mux))
}

func (tr *ModuleTransport) subEvents(ctx context.Context, mux transport.MessageDispatcher) subFunc {
	return func() error {
		return contextToken(ctx, tr.conn.Subscribe(
			"devices/"+tr.did+"/modules/"+tr.mid+"/messages/devicebound/#", DefaultQoS, func(_ mqtt.Client, m mqtt.Message) {
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

// SubscribeTwinUpdates subscribes to module desired state changes.
func (tr *ModuleTransport) SubscribeTwinUpdates(ctx context.Context, mux transport.TwinStateDispatcher) error {
	return tr.sub(tr.subTwinUpdates(ctx, mux))
}

func (tr *ModuleTransport) subTwinUpdates(ctx context.Context, mux transport.TwinStateDispatcher) subFunc {
	return func() error {
		return contextToken(ctx, tr.conn.Subscribe(
			"$iothub/twin/PATCH/properties/desired/#", DefaultQoS, func(_ mqtt.Client, m mqtt.Message) {
				mux.Dispatch(m.Payload())
			},
		))
	}
}

func (tr *ModuleTransport) Send(ctx context.Context, msg *common.Message) error {
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

	dst := "devices/" + tr.did + "/modules/" + tr.mid + "/messages/events/" + u.Encode()

	qos := DefaultQoS
	if q, ok := msg.TransportOptions["qos"]; ok {
		qos = q.(int) // panic if it's not an int
		if qos != 0 && qos != 1 {
			return fmt.Errorf("invalid QoS value: %d", qos)
		}
	}
	return tr.send(ctx, dst, qos, msg.Payload)
}
