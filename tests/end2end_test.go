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

	"github.com/amenzhinsky/iothub/iotdevice"
	"github.com/amenzhinsky/iothub/iotservice"
	"github.com/amenzhinsky/iothub/transport"
	"github.com/amenzhinsky/iothub/transport/mqtt"
)

func TestEnd2End(t *testing.T) {
	dcs := os.Getenv("TEST_DEVICE_CONNECTION_STRING")
	if dcs == "" {
		t.Fatal("TEST_DEVICE_CONNECTION_STRING is empty")
	}
	device := os.Getenv("TEST_X509_DEVICE")
	if device == "" {
		t.Fatal("TEST_X509_DEVICE is empty")
	}
	hostname := os.Getenv("TEST_X509_HOSTNAME")
	if hostname == "" {
		t.Fatal("TEST_X509_HOSTNAME is empty")
	}

	for name, opts := range map[string][]iotdevice.ClientOption{
		"x509": {
			iotdevice.WithDeviceID(device),
			iotdevice.WithHostname(hostname),
			iotdevice.WithX509FromFile("./testdata/dev.crt", "./testdata/dev.key"),
		},
		"symmetric-key": {
			iotdevice.WithConnectionString(dcs),
		},
	} {
		t.Run(name, func(t *testing.T) {
			for name, mk := range map[string]func() (transport.Transport, error){
				"mqtt": func() (transport.Transport, error) { return mqtt.New(mqtt.WithLogger(nil)) },
				//"amqp": func() (transport.Transport, error) { return amqp.New() },
			} {
				t.Run(name, func(t *testing.T) {
					for name, test := range map[string]func(*testing.T, ...iotdevice.ClientOption){
						"DeviceToCloud": testDeviceToCloud,
						"CloudToDevice": testCloudToDevice,
						"DirectMethod":  testDirectMethod,
						"TwinDevice":    testTwinDevice,
					} {
						t.Run(name, func(t *testing.T) {
							tr, err := mk()
							if err != nil {
								t.Fatal(err)
							}
							test(t, append(opts, iotdevice.WithLogger(nil), iotdevice.WithTransport(tr))...)
						})
					}
				})
			}
		})
	}
}

func testDeviceToCloud(t *testing.T, opts ...iotdevice.ClientOption) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dc, sc := mkDeviceAndService(t, ctx, opts...)
	defer closeDeviceService(t, dc, sc)

	evch := make(chan *iotservice.Event, 1)
	errc := make(chan error, 2)
	go func() {
		errc <- sc.Subscribe(ctx, func(ev *iotservice.Event) {
			if ev.DeviceID == dc.DeviceID() {
				evch <- ev
			}
		})
	}()

	w := &iotdevice.Event{
		Payload:    []byte(`hello`),
		Properties: map[string]string{"foo": "bar"},
	}

	// send events until one of them is received
	go func() {
		for {
			if err := dc.Publish(ctx, w); err != nil {
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
	case g := <-evch:
		testEventsAreEqual(t, w, g)
	case err := <-errc:
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		t.Fatal("d2c timed out")
	}
}

func testCloudToDevice(t *testing.T, opts ...iotdevice.ClientOption) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dc, sc := mkDeviceAndService(t, ctx, opts...)
	defer closeDeviceService(t, dc, sc)

	evsc := make(chan *iotdevice.Event, 1)
	fbsc := make(chan *iotservice.Feedback, 1)
	errc := make(chan error, 3)
	go func() {
		if err := dc.SubscribeEvents(ctx, func(ev *iotdevice.Event) {
			evsc <- ev
		}); err != nil {
			errc <- err
		}
	}()

	// list of sent event ids
	mu := sync.Mutex{}
	ids := make([]string, 10)

	// subscribe to feedback and report first registered message id
	go func() {
		if err := sc.SubscribeFeedback(ctx, func(fb *iotservice.Feedback) {
			mu.Lock()
			for _, id := range ids {
				if fb.OriginalMessageID == id {
					fbsc <- fb
				}
			}
			mu.Unlock()
		}); err != nil {
			errc <- err
		}
	}()

	w := &iotservice.Event{
		DeviceID: dc.DeviceID(),
		Payload:  []byte("hello"),
		Properties: map[string]string{
			"foo": "bar",
		},
		Ack: "full",
	}

	// send events until one of them received.
	go func() {
		for {
			id, err := sc.Publish(ctx, w)
			if err != nil {
				errc <- err
				break
			}

			mu.Lock()
			ids = append(ids, id)
			mu.Unlock()

			select {
			case <-ctx.Done():
				break
			case <-time.After(500 * time.Millisecond):
			}
		}
	}()

	select {
	case g := <-evsc:
		select {
		case <-fbsc:
			// feedback has successfully arrived
		case <-time.After(15 * time.Second):
			t.Fatal("feedback timed out")
		}
		testEventsAreEqual(t, g, w)
	case err := <-errc:
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		t.Fatal("c2d timed out")
	}
}

func testEventsAreEqual(t *testing.T, d *iotdevice.Event, c *iotservice.Event) {
	t.Helper()
	if !bytes.Equal(d.Payload, c.Payload) {
		goto Error
	}
	for k, v := range c.Properties {
		if d.Properties[k] != v {
			goto Error
		}
	}
	return
Error:
	t.Errorf("events are not equal, got %v, want %v", d, c)
}

func testTwinDevice(t *testing.T, opts ...iotdevice.ClientOption) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dc, sc := mkDeviceAndService(t, ctx, opts...)
	defer closeDeviceService(t, dc, sc)

	// update state and keep track of version
	s := fmt.Sprintf("%d", time.Now().UnixNano())
	v, err := dc.UpdateTwin(ctx, map[string]interface{}{
		"ts": s,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, r, err := dc.RetrieveState(ctx)
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
		v, err := sc.InvokeMethod(ctx, &iotservice.Invocation{
			DeviceID:        dc.DeviceID(),
			MethodName:      "sum",
			ConnectTimeout:  0,
			ResponseTimeout: 5,
			Payload: map[string]interface{}{
				"a": 1.5,
				"b": 3,
			},
		})
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

	dc, err := iotdevice.New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	if err = dc.ConnectInBackground(ctx, false); err != nil {
		t.Fatal(err)
	}

	sc, err := iotservice.New(
		iotservice.WithLogger(nil),
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
