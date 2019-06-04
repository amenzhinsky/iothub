package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"

	"github.com/amenzhinsky/iothub/cmd/internal"
	"github.com/amenzhinsky/iothub/iotdevice"
	"github.com/amenzhinsky/iothub/iotdevice/transport"
	"github.com/amenzhinsky/iothub/iotdevice/transport/mqtt"
)

var transports = map[string]func() (transport.Transport, error){
	"mqtt": func() (transport.Transport, error) {
		return mqtt.New(), nil
	},
	"amqp": func() (transport.Transport, error) {
		return nil, errors.New("not implemented")
	},
	"http": func() (transport.Transport, error) {
		return nil, errors.New("not implemented")
	},
}

var (
	debugFlag     bool
	formatFlag    string
	quiteFlag     bool
	transportFlag string
	midFlag       string
	cidFlag       string
	qosFlag       int

	// x509 flags
	tlsCertFlag  string
	tlsKeyFlag   string
	deviceIDFlag string
	hostnameFlag string
)

func main() {
	if err := run(); err != nil {
		if err != internal.ErrInvalidUsage {
			fmt.Fprintf(os.Stderr, "error: %s\n", err)
		}
		os.Exit(1)
	}
}

const help = `iothub-device helps iothub devices to communicate with the cloud.
$IOTHUB_DEVICE_CONNECTION_STRING environment variable is required unless you use x509 authentication.`

func run() error {
	cli, err := internal.New(help, func(f *flag.FlagSet) {
		f.BoolVar(&debugFlag, "debug", false, "enable debug mode")
		f.StringVar(&formatFlag, "format", "json-pretty", "data output format <json|json-pretty>")
		f.StringVar(&transportFlag, "transport", "mqtt", "transport to use <mqtt|amqp|http>")
		f.StringVar(&tlsCertFlag, "tls-cert", "", "path to x509 cert file")
		f.StringVar(&tlsKeyFlag, "tls-key", "", "path to x509 key file")
		f.StringVar(&deviceIDFlag, "device-id", "", "device id, required for x509")
		f.StringVar(&hostnameFlag, "hostname", "", "hostname to connect to, required for x509")
	}, []*internal.Command{
		{
			Name:    "send",
			Alias:   "s",
			Help:    "PAYLOAD [KEY VALUE]...",
			Desc:    "send a message to the cloud (D2C)",
			Handler: wrap(send),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&midFlag, "mid", "", "identifier for the message")
				f.StringVar(&cidFlag, "cid", "", "message identifier in a request-reply")
				f.IntVar(&qosFlag, "qos", mqtt.DefaultQoS, "QoS value, 0 or 1 (mqtt only)")
			},
		},
		{
			Name:    "watch-events",
			Alias:   "we",
			Desc:    "subscribe to messages sent from the cloud (C2D)",
			Handler: wrap(watchEvents),
		},
		{
			Name:    "watch-twin",
			Alias:   "wt",
			Desc:    "subscribe to desired twin state updates",
			Handler: wrap(watchTwin),
		},
		{
			Name:    "direct-method",
			Alias:   "dm",
			Help:    "NAME",
			Desc:    "handle the named direct method, reads responses from STDIN",
			Handler: wrap(directMethod),
			ParseFunc: func(f *flag.FlagSet) {
				f.BoolVar(&quiteFlag, "quite", false, "disable additional hints")
			},
		},
		{
			Name:    "twin-state",
			Alias:   "ts",
			Desc:    "retrieve desired and reported states",
			Handler: wrap(twin),
		},
		{
			Name:    "update-twin",
			Alias:   "ut",
			Help:    "[KEY VALUE]...",
			Desc:    "updates the twin device deported state, null means delete the key",
			Handler: wrap(updateTwin),
		},
	})
	if err != nil {
		return err
	}
	return cli.Run(context.Background(), os.Args...)
}

func wrap(fn func(context.Context, *flag.FlagSet, *iotdevice.Client) error) internal.HandlerFunc {
	return func(ctx context.Context, f *flag.FlagSet) error {
		mk, ok := transports[transportFlag]
		if !ok {
			return fmt.Errorf("unknown transport %q", transportFlag)
		}
		t, err := mk()
		if err != nil {
			return err
		}

		opts := []iotdevice.ClientOption{iotdevice.WithTransport(t)}
		if tlsCertFlag != "" && tlsKeyFlag != "" {
			if hostnameFlag == "" {
				return errors.New("hostname is required for x509 authentication")
			}
			if deviceIDFlag == "" {
				return errors.New("device-id is required for x509 authentication")
			}
			opts = append(opts,
				iotdevice.WithX509FromFile(deviceIDFlag, hostnameFlag, tlsCertFlag, tlsKeyFlag),
			)
		}

		c, err := iotdevice.New(opts...)
		if err != nil {
			return err
		}
		if err := c.Connect(ctx); err != nil {
			return err
		}
		return fn(ctx, f, c)
	}
}

func send(ctx context.Context, f *flag.FlagSet, c *iotdevice.Client) error {
	if f.NArg() < 1 {
		return internal.ErrInvalidUsage
	}
	var props map[string]string
	if f.NArg() > 1 {
		var err error
		props, err = internal.ArgsToMap(f.Args()[1:])
		if err != nil {
			return err
		}
	}
	return c.SendEvent(ctx, []byte(f.Arg(0)),
		iotdevice.WithSendProperties(props),
		iotdevice.WithSendMessageID(midFlag),
		iotdevice.WithSendCorrelationID(cidFlag),
		iotdevice.WithSendQoS(qosFlag),
	)
}

func watchEvents(ctx context.Context, f *flag.FlagSet, c *iotdevice.Client) error {
	if f.NArg() != 0 {
		return internal.ErrInvalidUsage
	}
	sub, err := c.SubscribeEvents(ctx)
	if err != nil {
		return err
	}
	for msg := range sub.C() {
		if err = internal.Output(msg, formatFlag); err != nil {
			return err
		}
	}
	return sub.Err()
}

func watchTwin(ctx context.Context, f *flag.FlagSet, c *iotdevice.Client) error {
	if f.NArg() != 0 {
		return internal.ErrInvalidUsage
	}
	sub, err := c.SubscribeTwinUpdates(ctx)
	if err != nil {
		return err
	}
	for twin := range sub.C() {
		if err = internal.Output(twin, formatFlag); err != nil {
			return err
		}
	}
	return sub.Err()
}

func directMethod(ctx context.Context, f *flag.FlagSet, c *iotdevice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}

	// if an error occurs during the method invocation,
	// immediately return and display the error.
	errc := make(chan error, 1)

	in := bufio.NewReader(os.Stdin)
	mu := &sync.Mutex{}

	if err := c.RegisterMethod(ctx, f.Arg(0),
		func(p map[string]interface{}) (map[string]interface{}, error) {
			mu.Lock()
			defer mu.Unlock()

			b, err := json.Marshal(p)
			if err != nil {
				errc <- err
				return nil, err
			}
			if quiteFlag {
				fmt.Println(string(b))
			} else {
				fmt.Printf("Payload: %s\n", string(b))
				fmt.Printf("Enter json response: ")
			}
			b, _, err = in.ReadLine()
			if err != nil {
				errc <- err
				return nil, err
			}
			var v map[string]interface{}
			if err = json.Unmarshal(b, &v); err != nil {
				errc <- errors.New("unable to parse json input")
				return nil, err
			}
			return v, nil
		}); err != nil {
		return err
	}

	return <-errc
}

func twin(ctx context.Context, _ *flag.FlagSet, c *iotdevice.Client) error {
	desired, reported, err := c.RetrieveTwinState(ctx)
	if err != nil {
		return err
	}

	b, err := json.Marshal(desired)
	if err != nil {
		return err
	}
	fmt.Println("desired:  " + string(b))

	b, err = json.Marshal(reported)
	if err != nil {
		return err
	}
	fmt.Println("reported: " + string(b))

	return nil
}

func updateTwin(ctx context.Context, f *flag.FlagSet, c *iotdevice.Client) error {
	if f.NArg() == 0 {
		return internal.ErrInvalidUsage
	}

	s, err := internal.ArgsToMap(f.Args())
	if err != nil {
		return err
	}
	m := make(iotdevice.TwinState, len(s))
	for k, v := range s {
		if v == "null" {
			m[k] = nil
		} else {
			m[k] = v
		}
	}
	ver, err := c.UpdateTwinState(ctx, m)
	if err != nil {
		return err
	}
	fmt.Printf("version: %d\n", ver)
	return nil
}
