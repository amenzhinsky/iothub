package tests

import (
	"bytes"
	"context"
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
	"github.com/amenzhinsky/golang-iothub/iotutil"
)

func TestEnd2End(t *testing.T) {
	dcs := os.Getenv("TEST_DEVICE_CONNECTION_STRING")
	if dcs == "" {
		t.Fatal("TEST_DEVICE_CONNECTION_STRING is empty")
	}
	ddcs := os.Getenv("TEST_DISABLED_DEVICE_CONNECTION_STRING")
	if ddcs == "" {
		t.Fatal("TEST_DISABLED_DEVICE_CONNECTION_STRING is empty")
	}
	x509DeviceID := os.Getenv("TEST_X509_DEVICE")
	if x509DeviceID == "" {
		t.Fatal("TEST_X509_DEVICE is empty")
	}
	hostname := os.Getenv("TEST_HOSTNAME")
	if hostname == "" {
		t.Fatal("TEST_HOSTNAME is empty")
	}

	for name, mk := range map[string]func() (transport.Transport, error){
		"mqtt": func() (transport.Transport, error) { return mqtt.New() },
		//"amqp": func() (transport.Transport, error) { return amqp.New() },
	} {
		t.Run(name, func(t *testing.T) {
			for auth, suite := range map[string]struct {
				opts []iotdevice.ClientOption
				test string
			}{
				"x509": {
					[]iotdevice.ClientOption{
						iotdevice.WithDeviceID(x509DeviceID),
						iotdevice.WithHostname(hostname),
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
						"TwinDevice":    testTwinDevice,
					} {
						if suite.test != "*" && suite.test != name {
							continue
						}
						t.Run(name, func(t *testing.T) {
							tr, err := mk()
							if err != nil {
								t.Fatal(err)
							}
							test(t, append(suite.opts, iotdevice.WithTransport(tr))...)
						})
					}
				})
			}

			// TODO: add test
			t.Run("DisabledDevice", func(t *testing.T) {
			})
		})
	}
}

func testDeviceToCloud(t *testing.T, opts ...iotdevice.ClientOption) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dc, sc := mkDeviceAndService(t, ctx, opts...)
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
				iotdevice.WithSendProperties(props),
			); err != nil {
				errc <- err
				break
			}
			select {
			case <-ctx.Done():
				break
			case <-time.After(250 * time.Millisecond):
			}
		}
	}()

	select {
	case msg := <-msgc:
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

	dc, sc := mkDeviceAndService(t, ctx, opts...)
	defer closeDeviceService(t, dc, sc)

	msgc := make(chan *common.Message, 1)
	fbsc := make(chan *iotservice.Feedback, 1)
	errc := make(chan error, 3)
	go func() {
		if err := dc.SubscribeEvents(ctx, func(msg *common.Message) {
			msgc <- msg
		}); err != nil {
			errc <- err
		}
	}()

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
	userID := "golang-iothub"

	// send events until one of them received.
	go func() {
		for {
			msgID := iotutil.UUID()
			if err := sc.SendEvent(ctx, dc.DeviceID(), payload,
				iotservice.WithSendAck("full"),
				iotservice.WithSendProperties(props),
				iotservice.WithSendUserID(userID),
				iotservice.WithSendMessageID(msgID),
				iotservice.WithSendCorrelationID(iotutil.UUID()),
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
		if msg.UserID != userID {
			t.Errorf("UserID = %q, want %q", msg.UserID, userID)
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

func testTwinDevice(t *testing.T, opts ...iotdevice.ClientOption) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dc, sc := mkDeviceAndService(t, ctx, opts...)
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

func testDirectMethod(t *testing.T, opts ...iotdevice.ClientOption) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dc, sc := mkDeviceAndService(t, ctx, opts...)
	defer closeDeviceService(t, dc, sc)

	errc := make(chan error, 2)
	go func() {
		if err := dc.HandleMethod(ctx, "sum", func(v map[string]interface{}) (map[string]interface{}, error) {
			return map[string]interface{}{
				"result": v["a"].(float64) + v["b"].(float64),
			}, nil
		}); err != nil {
			errc <- err
		}
	}()

	resc := make(chan map[string]interface{}, 1)
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
		w := map[string]interface{}{
			"result": 4.5,
		}
		if !reflect.DeepEqual(v, w) {
			t.Errorf("direct-method result = %v, want %v", v, w)
		}
	case err := <-errc:
		t.Fatal(err)
	}
}

func mkDeviceAndService(
	t *testing.T,
	ctx context.Context,
	opts ...iotdevice.ClientOption,
) (*iotdevice.Client, *iotservice.Client) {
	ccs := os.Getenv("TEST_SERVICE_CONNECTION_STRING")
	if ccs == "" {
		t.Fatal("TEST_SERVICE_CONNECTION_STRING is empty")
	}

	dc, err := iotdevice.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	if err = dc.ConnectInBackground(ctx, false); err != nil {
		t.Fatal(err)
	}

	sc, err := iotservice.NewClient(
		iotservice.WithConnectionString(ccs),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = sc.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	return dc, sc
}

func closeDeviceService(t *testing.T, dc *iotdevice.Client, sc *iotservice.Client) {
	if err := dc.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sc.Close(); err != nil {
		t.Fatal(err)
	}
}
