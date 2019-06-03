package tests

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/amenzhinsky/iothub/common"
	"github.com/amenzhinsky/iothub/credentials"
	"github.com/amenzhinsky/iothub/iotdevice"
	"github.com/amenzhinsky/iothub/iotdevice/transport"
	"github.com/amenzhinsky/iothub/iotdevice/transport/mqtt"
	"github.com/amenzhinsky/iothub/iotservice"
)

func TestEnd2End(t *testing.T) {
	sc := newServiceClient(t)

	// delete previously created devices that weren't cleaned up
	for _, did := range []string{"golang-iothub-sas", "golang-iothub-x509", "golang-iothub-ca"} {
		_ = sc.DeleteDevice(context.Background(), &iotservice.Device{
			DeviceID: did,
		})
	}

	// create a device with sas authentication
	sasDevice, err := sc.CreateDevice(context.Background(), &iotservice.Device{
		DeviceID: "golang-iothub-sas",
		Authentication: &iotservice.Authentication{
			Type: iotservice.AuthSAS,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// create a x509 self signed device
	x509Device, err := sc.CreateDevice(context.Background(), &iotservice.Device{
		DeviceID: "golang-iothub-x509",
		Authentication: &iotservice.Authentication{
			X509Thumbprint: &iotservice.X509Thumbprint{
				PrimaryThumbprint:   "443ABB6DEA8F93D5987D31D2607BE2931217752C",
				SecondaryThumbprint: "443ABB6DEA8F93D5987D31D2607BE2931217752C",
			},
			Type: iotservice.AuthSelfSigned,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	caDevice, err := sc.CreateDevice(context.Background(), &iotservice.Device{
		DeviceID: "golang-iothub-ca",
		Authentication: &iotservice.Authentication{
			Type: iotservice.AuthCA,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = caDevice // test that a CA-authenticated device can be created

	dcs, err := sc.DeviceConnectionString(sasDevice, false)
	if err != nil {
		t.Fatal(err)
	}

	creds, err := credentials.ParseConnectionString(dcs)
	if err != nil {
		t.Fatal(err)
	}

	for name, mktransport := range map[string]func() transport.Transport{
		"mqtt": func() transport.Transport { return mqtt.New() },
		// TODO: "amqp": func() transport.Transport { return amqp.New() },
		// TODO: "http": func() transport.Transport { return http.New() },
	} {
		mktransport := mktransport
		t.Run(name, func(t *testing.T) {
			for auth, suite := range map[string]struct {
				opts []iotdevice.ClientOption
				test string
			}{
				"custom": {
					[]iotdevice.ClientOption{
						iotdevice.WithCredentials(&thirdPartyCreds{creds}),
					},
					"TwinDevice", // just need to check access
				},
				"x509": {
					[]iotdevice.ClientOption{
						iotdevice.WithX509FromFile(
							x509Device.DeviceID,
							sc.HostName(),
							"testdata/device.crt",
							"testdata/device.key",
						),
					},
					"TwinDevice", // just need to check access
				},
				"sas": {
					[]iotdevice.ClientOption{
						iotdevice.WithConnectionString(dcs),
					},
					"*",
				},
			} {
				for name, test := range map[string]func(*testing.T, ...iotdevice.ClientOption){
					"DeviceToCloud": testDeviceToCloud,
					"CloudToDevice": testCloudToDevice,
					"DirectMethod":  testDirectMethod,
					"UpdateTwin":    testUpdateTwin,
					"SubscribeTwin": testSubscribeTwin,
					"Modules":       testModules,
				} {
					if suite.test != "*" && suite.test != name {
						continue
					}

					test := test
					mktransport := mktransport
					t.Run(auth+"/"+name, func(t *testing.T) {
						test(t, append(suite.opts, iotdevice.WithTransport(mktransport()))...)
					})
				}
			}
		})
	}
	for _, did := range []string{sasDevice.DeviceID, x509Device.DeviceID} {
		if err = sc.DeleteDevice(context.Background(), &iotservice.Device{
			DeviceID: did,
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// we're just simulating that sas token is requested some other way, like an external API.
type thirdPartyCreds struct {
	creds *credentials.Credentials
}

func (c *thirdPartyCreds) DeviceID() string {
	return c.creds.DeviceID
}

func (c *thirdPartyCreds) Hostname() string {
	return c.creds.HostName
}

func (c *thirdPartyCreds) IsSAS() bool {
	return true
}

func (c *thirdPartyCreds) TLSConfig() *tls.Config {
	return &tls.Config{
		ServerName: c.creds.HostName,
		RootCAs:    common.RootCAs(),
	}
}

func (c *thirdPartyCreds) Token(ctx context.Context, uri string, d time.Duration) (string, error) {
	return c.creds.GenerateToken(uri, credentials.WithDuration(d))
}

func testDeviceToCloud(t *testing.T, opts ...iotdevice.ClientOption) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dc, sc := newDeviceAndServiceClient(t, ctx, opts...)
	defer closeDeviceService(t, dc, sc)

	evsc := make(chan *iotservice.Event, 1)
	errc := make(chan error, 2)
	go func() {
		errc <- sc.SubscribeEvents(ctx, func(ev *iotservice.Event) error {
			if ev.ConnectionDeviceID == dc.DeviceID() {
				evsc <- ev
			}
			return nil
		})
	}()

	payload := []byte(`hello`)
	props := map[string]string{"a": "a", "b": "b"}

	// send events until one of them is received
	go func() {
		for {
			if err := dc.SendEvent(ctx, payload,
				iotdevice.WithSendMessageID(common.GenID()),
				iotdevice.WithSendCorrelationID(common.GenID()),
				iotdevice.WithSendProperties(props),
			); err != nil {
				errc <- err
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
		}
	}()

	select {
	case msg := <-evsc:
		if msg.MessageID == "" {
			t.Error("MessageID is empty")
		}
		if msg.CorrelationID == "" {
			t.Error("CorrelationID is empty")
		}
		if msg.ConnectionDeviceID != dc.DeviceID() {
			t.Errorf("ConnectionDeviceID = %q, want %q", msg.ConnectionDeviceID, dc.DeviceID())
		}
		if msg.ConnectionDeviceGenerationID == "" {
			t.Error("ConnectionDeviceGenerationID is empty")
		}
		if msg.ConnectionAuthMethod == "" {
			t.Error("ConnectionAuthMethod is empty")
		}
		if msg.MessageSource == "" {
			t.Error("MessageSource is empty")
		}
		if msg.EnqueuedTime.IsZero() {
			t.Error("EnqueuedTime is zero")
		}
		if !bytes.Equal(msg.Payload, payload) {
			t.Errorf("Payload = %v, want %v", msg.Payload, payload)
		}
		testProperties(t, msg.Properties, props)
	case err := <-errc:
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		t.Fatal("d2c timed out")
	}
}

func testProperties(t *testing.T, got, want map[string]string) {
	t.Helper()
	for k, v := range want {
		x, ok := got[k]
		if !ok || x != v {
			t.Errorf("Properties = %v, want %v", got, want)
			return
		}
	}
}

func testCloudToDevice(t *testing.T, opts ...iotdevice.ClientOption) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dc, sc := newDeviceAndServiceClient(t, ctx, opts...)
	defer closeDeviceService(t, dc, sc)

	fbsc := make(chan *iotservice.Feedback, 1)
	errc := make(chan error, 3)

	sub, err := dc.SubscribeEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// subscribe to feedback and report first registered message id
	go func() {
		if err := sc.SubscribeFeedback(ctx, func(fb *iotservice.Feedback) {
			fbsc <- fb
		}); err != nil {
			errc <- err
		}
	}()

	payload := []byte("hello")
	props := map[string]string{"a": "a", "b": "b"}
	uid := "golang-iothub"

	mid := common.GenID()
	if err := sc.SendEvent(ctx, dc.DeviceID(), payload,
		iotservice.WithSendAck("full"),
		iotservice.WithSendProperties(props),
		iotservice.WithSendUserID(uid),
		iotservice.WithSendMessageID(mid),
		iotservice.WithSendCorrelationID(common.GenID()),
		iotservice.WithSentExpiryTime(time.Now().Add(5*time.Second)),
	); err != nil {
		errc <- err
		return
	}

	for {
		select {
		case msg := <-sub.C():
			if msg.MessageID != mid {
				continue
			}

			// validate event feedback
		Outer:
			for {
				select {
				case fb := <-fbsc:
					if fb.OriginalMessageID != mid {
						continue
					}
					if fb.StatusCode != "Success" {
						t.Errorf("feedback status = %q, want %q", fb.StatusCode, "Success")
					}
					break Outer
				case <-time.After(30 * time.Second):
					t.Fatal("feedback timed out")
				}
			}

			// validate message content
			if msg.To == "" {
				t.Error("To is empty")
			}
			if msg.UserID != uid {
				t.Errorf("UserID = %q, want %q", msg.UserID, uid)
			}
			if !bytes.Equal(msg.Payload, payload) {
				t.Errorf("Payload = %v, want %v", msg.Payload, payload)
			}
			if msg.MessageID == "" {
				t.Error("MessageID is empty")
			}
			if msg.CorrelationID == "" {
				t.Error("CorrelationID is empty")
			}
			if msg.ExpiryTime.IsZero() {
				t.Error("ExpiryTime is zero")
			}
			testProperties(t, msg.Properties, props)
			return
		case err := <-errc:
			t.Fatal(err)
		case <-time.After(10 * time.Second):
			t.Fatal("c2d timed out")
		}
	}
}

func testUpdateTwin(t *testing.T, opts ...iotdevice.ClientOption) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dc, sc := newDeviceAndServiceClient(t, ctx, opts...)
	defer closeDeviceService(t, dc, sc)

	// update state and keep track of version
	s := fmt.Sprintf("%d", time.Now().UnixNano())
	v, err := dc.UpdateTwinState(ctx, map[string]interface{}{
		"ts": s,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, r, err := dc.RetrieveTwinState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if v != r.Version() {
		t.Errorf("update-twin version = %d, want %d", r.Version(), v)
	}
	if r["ts"] != s {
		t.Errorf("update-twin parameter = %q, want %q", r["ts"], s)
	}
}

func testSubscribeTwin(t *testing.T, opts ...iotdevice.ClientOption) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dc, sc := newDeviceAndServiceClient(t, ctx, opts...)
	defer closeDeviceService(t, dc, sc)

	sub, err := dc.SubscribeTwinUpdates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer dc.UnsubscribeTwinUpdates(sub)

	// TODO: hacky, but reduces flakiness
	time.Sleep(time.Second)

	twin, err := sc.UpdateTwin(ctx, &iotservice.Twin{
		DeviceID: dc.DeviceID(),
		Tags: map[string]interface{}{
			"test-device": true,
		},
		Properties: &iotservice.Properties{
			Desired: map[string]interface{}{
				"test-prop": time.Now().UnixNano() / 1000,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case state := <-sub.C():
		if state["$version"] != twin.Properties.Desired["$version"] {
			t.Errorf("version = %d, want %d", state["$version"], twin.Properties.Desired["$version"])
		}
		if state["test-prop"] != twin.Properties.Desired["test-prop"] {
			t.Errorf("test-prop = %q, want %q", state["test-prop"], twin.Properties.Desired["test-prop"])
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SubscribeTwinUpdates timed out")
	}
}

func testDirectMethod(t *testing.T, opts ...iotdevice.ClientOption) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dc, sc := newDeviceAndServiceClient(t, ctx, opts...)
	defer closeDeviceService(t, dc, sc)

	if err := dc.RegisterMethod(ctx, "sum", func(v map[string]interface{}) (map[string]interface{}, error) {
		return map[string]interface{}{
			"result": v["a"].(float64) + v["b"].(float64),
		}, nil
	}); err != nil {
		t.Fatal(err)
	}

	resc := make(chan *iotservice.Result, 1)
	errc := make(chan error, 2)
	go func() {
		v, err := sc.Call(ctx, dc.DeviceID(), "sum", map[string]interface{}{
			"a": 1.5,
			"b": 3,
		},
			iotservice.WithCallConnectTimeout(0),
			iotservice.WithCallResponseTimeout(5),
		)
		if err != nil {
			errc <- err
		}
		resc <- v
	}()

	select {
	case v := <-resc:
		w := &iotservice.Result{
			Status: 200,
			Payload: map[string]interface{}{
				"result": 4.5,
			},
		}
		if !reflect.DeepEqual(v, w) {
			t.Errorf("direct-method result = %v, want %v", v, w)
		}
	case err := <-errc:
		t.Fatal(err)
	}
}

func testModules(t *testing.T, opts ...iotdevice.ClientOption) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dc, sc := newDeviceAndServiceClient(t, ctx, opts...)
	defer closeDeviceService(t, dc, sc)

	module, err := sc.CreateModule(ctx, &iotservice.Module{
		DeviceID: dc.DeviceID(),
		ModuleID: "testmodule",
	})
	if err != nil {
		t.Fatal(err)
	}

	// there should be only the previously created module
	modules, err := sc.ListModules(ctx, dc.DeviceID())
	if err != nil {
		t.Fatal(err)
	}
	if len(modules) != 1 {
		t.Fatalf("modules number = %d, want %d", len(modules), 1)
	}

	// check that module exists
	_, err = sc.GetModule(ctx, dc.DeviceID(), module.ModuleID)
	if err != nil {
		t.Fatal(err)
	}
	if err = sc.DeleteModule(ctx, module); err != nil {
		t.Fatal(err)
	}
}

func newDeviceAndServiceClient(
	t *testing.T,
	ctx context.Context,
	opts ...iotdevice.ClientOption,
) (*iotdevice.Client, *iotservice.Client) {
	sc := newServiceClient(t)

	dc, err := iotdevice.New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	if err = dc.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	return dc, sc
}

func newServiceClient(t *testing.T) *iotservice.Client {
	cs := os.Getenv("TEST_IOTHUB_SERVICE_CONNECTION_STRING")
	if cs == "" {
		t.Fatal("$TEST_IOTHUB_SERVICE_CONNECTION_STRING is empty")
	}
	c, err := iotservice.New(
		iotservice.WithConnectionString(cs),
	)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func closeDeviceService(t *testing.T, dc *iotdevice.Client, sc *iotservice.Client) {
	if err := dc.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sc.Close(); err != nil {
		t.Fatal(err)
	}
}
