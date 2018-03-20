package tests

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/amenzhinsky/golang-iothub/common"
	"github.com/amenzhinsky/golang-iothub/iotdevice"
	"github.com/amenzhinsky/golang-iothub/iotdevice/transport"
	"github.com/amenzhinsky/golang-iothub/iotdevice/transport/mqtt"
	"github.com/amenzhinsky/golang-iothub/iotservice"
)

func TestEnd2End(t *testing.T) {
	sc := newServiceClient(t)
	pk, err := iotservice.NewSymmetricKey()
	if err != nil {
		t.Fatal(err)
	}

	// delete previously created devices that weren't cleaned up
	for _, did := range []string{"golang-iothub-sas", "golang-iothub-x509"} {
		sc.DeleteDevice(context.Background(), did)
	}

	// create a device with sas authentication
	sasDevice, err := sc.CreateDevice(context.Background(), &iotservice.Device{
		DeviceID: "golang-iothub-sas",
		Authentication: &iotservice.Authentication{
			SymmetricKey: &iotservice.SymmetricKey{
				PrimaryKey:   pk,
				SecondaryKey: pk,
			},
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

	dcs, err := sc.DeviceConnectionString(sasDevice, false)
	if err != nil {
		t.Fatal(err)
	}

	for name, tr := range map[string]func() transport.Transport{
		"mqtt": func() transport.Transport { return mqtt.New() },
		//"amqp": func() transport.Transport { return amqp.New() },
	} {
		t.Run(name, func(t *testing.T) {
			for auth, suite := range map[string]struct {
				opts []iotdevice.ClientOption
				test string
			}{
				"x509": {
					[]iotdevice.ClientOption{
						iotdevice.WithDeviceID(x509Device.DeviceID),
						iotdevice.WithHostname(sc.HostName()),
						iotdevice.WithX509FromFile("testdata/device.crt", "testdata/device.key"),
					},

					// we test only access here so we don't want to run all the tests
					"TwinDevice",
				},
				"sas": {
					[]iotdevice.ClientOption{
						iotdevice.WithConnectionString(dcs),
					},
					"*",
				},
			} {
				t.Run(auth, func(t *testing.T) {
					for name, test := range map[string]func(*testing.T, ...iotdevice.ClientOption){
						"DeviceToCloud": testDeviceToCloud,
						"CloudToDevice": testCloudToDevice,
						"DirectMethod":  testDirectMethod,
						"UpdateTwin":    testUpdateTwin,
						"SubscribeTwin": testSubscribeTwin,
					} {
						if suite.test != "*" && suite.test != name {
							continue
						}
						t.Run(name, func(t *testing.T) {
							test(t, append(suite.opts, iotdevice.WithTransport(tr()))...)
						})
					}
				})
			}
		})
	}

	for _, did := range []string{sasDevice.DeviceID, x509Device.DeviceID} {
		if err = sc.DeleteDevice(context.Background(), did); err != nil {
			t.Fatal(err)
		}
	}
}

func randString() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", b)
}

func testDeviceToCloud(t *testing.T, opts ...iotdevice.ClientOption) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dc, sc := newDeviceAndServiceClient(t, ctx, opts...)
	defer closeDeviceService(t, dc, sc)

	msgc := make(chan *common.Message, 1)
	errc := make(chan error, 2)
	go func() {
		errc <- sc.SubscribeEvents(ctx, func(msg *common.Message) {
			if msg.ConnectionDeviceID == dc.DeviceID() {
				msgc <- msg
			}
		})
	}()

	payload := []byte(`hello`)
	props := map[string]string{"a": "a", "b": "b"}

	// send events until one of them is received
	go func() {
		for {
			if err := dc.SendEvent(ctx, payload,
				//iotdevice.WithSendTo(to), // TODO: breaks amqp
				iotdevice.WithSendMessageID(randString()),
				iotdevice.WithSendCorrelationID(randString()),
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
	case msg := <-msgc:
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

	msgc := make(chan *common.Message, 1)
	fbsc := make(chan *iotservice.Feedback, 1)
	errc := make(chan error, 3)

	if err := dc.SubscribeEvents(ctx, func(msg *common.Message) {
		msgc <- msg
	}); err != nil {
		t.Fatal(err)
	}

	// track send message ids
	mu := sync.Mutex{}
	msgIDs := make([]string, 10)

	// subscribe to feedback and report first registered message id
	go func() {
		if err := sc.SubscribeFeedback(ctx, func(fb *iotservice.Feedback) {
			mu.Lock()
			for _, id := range msgIDs {
				if fb.OriginalMessageID == id {
					fbsc <- fb
				}
			}
			mu.Unlock()
		}); err != nil {
			errc <- err
		}
	}()

	payload := []byte("hello")
	props := map[string]string{"a": "a", "b": "b"}
	uid := "golang-iothub"

	// send events until one of them received.
	go func() {
		for {
			msgID := randString()
			if err := sc.SendEvent(ctx, dc.DeviceID(), payload,
				iotservice.WithSendAck("full"),
				iotservice.WithSendProperties(props),
				iotservice.WithSendUserID(uid),
				iotservice.WithSendMessageID(msgID),
				iotservice.WithSendCorrelationID(randString()),
				iotservice.WithSentExpiryTime(time.Now().Add(5*time.Second)),
			); err != nil {
				errc <- err
				return
			}

			mu.Lock()
			msgIDs = append(msgIDs, msgID)
			mu.Unlock()

			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
		}
	}()

	select {
	case msg := <-msgc:
		// test feedback is received
		select {
		case fb := <-fbsc:
			if fb.StatusCode != "Success" {
				t.Errorf("feedback status = %q, want %q", fb.StatusCode, "Success")
			}
		case <-time.After(30 * time.Second):
			t.Fatal("feedback timed out")
		}

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
	case err := <-errc:
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		t.Fatal("c2d timed out")
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

	sch := make(chan iotdevice.TwinState)
	if err := dc.SubscribeTwinUpdates(ctx, func(s iotdevice.TwinState) {
		sch <- s
	}); err != nil {
		t.Fatal(err)
	}

	twin, err := sc.UpdateTwin(ctx, dc.DeviceID(), &iotservice.Twin{
		Tags: map[string]interface{}{
			"test-device": true,
		},
		Properties: &iotservice.Properties{
			Desired: map[string]interface{}{
				"test-prop": time.Now().UnixNano() / 1000,
			},
		},
	}, "*")
	if err != nil {
		t.Fatal(err)
	}

	select {
	case state := <-sch:
		if state["$version"] != twin.Properties.Desired["$version"] {
			t.Errorf("version = %d, want %d", state["$version"], twin.Properties.Desired["$version"])
		}
		if state["test-prop"] != twin.Properties.Desired["test-prop"] {
			t.Errorf("test-prop = %q, want %q", state["test-prop"], twin.Properties.Desired["test-prop"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SubscribeTwinUpdates twin timed out")
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

func newDeviceAndServiceClient(
	t *testing.T,
	ctx context.Context,
	opts ...iotdevice.ClientOption,
) (*iotdevice.Client, *iotservice.Client) {
	sc := newServiceClient(t)

	dc, err := iotdevice.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	if err = dc.ConnectInBackground(ctx, iotdevice.WithConnIgnoreNetErrors(true)); err != nil {
		t.Fatal(err)
	}
	return dc, sc
}

func newServiceClient(t *testing.T) *iotservice.Client {
	cs := os.Getenv("TEST_SERVICE_CONNECTION_STRING")
	if cs == "" {
		t.Fatal("TEST_SERVICE_CONNECTION_STRING is empty")
	}
	c, err := iotservice.NewClient(
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
