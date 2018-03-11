package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/amenzhinsky/golang-iothub/cmd/internal"
	"github.com/amenzhinsky/golang-iothub/common"
	"github.com/amenzhinsky/golang-iothub/iotdevice"
	"github.com/amenzhinsky/golang-iothub/iotdevice/transport"
	"github.com/amenzhinsky/golang-iothub/iotdevice/transport/mqtt"
)

var transports = map[string]func() (transport.Transport, error){
	"mqtt": func() (transport.Transport, error) {
		return mqtt.New(mqtt.WithLogger(mklog("[mqtt]   "))), nil
	},
	"amqp": func() (transport.Transport, error) {
		//return amqp.New(amqp.WithLogger(mklog("[amqp]   "))), nil
		return nil, errors.New("not implemented")
	},
	"http": func() (transport.Transport, error) {
		return nil, errors.New("not implemented")
	},
}

var (
	debugFlag     = false
	quiteFlag     = false
	transportFlag = "mqtt"
	midFlag       = ""
	cidFlag       = ""

	// x509 flags
	tlsCertFlag  = ""
	tlsKeyFlag   = ""
	deviceIDFlag = ""
	hostnameFlag = ""
)

func main() {
	if err := run(); err != nil {
		if err != internal.ErrInvalidUsage {
			fmt.Fprintf(os.Stderr, "error: %s\n", err)
		}
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	return internal.Run(ctx, map[string]*internal.Command{
		"send": {
			"PAYLOAD [KEY VALUE]...",
			"send a message to the cloud (D2C)",
			wrap(send),
			func(f *flag.FlagSet) {
				f.StringVar(&midFlag, "mid", midFlag, "identifier for the message")
				f.StringVar(&cidFlag, "cid", cidFlag, "message identifier in a request-reply")
			},
		},
		"watch-events": {
			"",
			"subscribe to messages sent from the cloud (C2D)",
			wrap(watchEvents),
			nil,
		},
		"watch-twin": {
			"",
			"subscribe to desired twin state updates",
			wrap(watchTwin),
			nil,
		},
		"direct-method": {
			"NAME",
			"handle the named direct method, reads responses from STDIN",
			wrap(directMethod),
			func(f *flag.FlagSet) {
				f.BoolVar(&quiteFlag, "quite", quiteFlag, "disable additional hints")
			},
		},
		"twin-state": {
			"",
			"retrieve desired and reported states",
			wrap(twin),
			nil,
		},
		"update-twin": {
			"[KEY VALUE]...",
			"updates the twin device deported state, null means delete the key",
			wrap(updateTwin),
			nil,
		},
	}, os.Args, func(f *flag.FlagSet) {
		f.BoolVar(&debugFlag, "debug", debugFlag, "enable debug mode")
		f.StringVar(&transportFlag, "transport", transportFlag, "transport to use <mqtt|amqp|http>")
		f.StringVar(&tlsCertFlag, "tls-cert", tlsCertFlag, "path to x509 cert file")
		f.StringVar(&tlsKeyFlag, "tls-key", tlsKeyFlag, "path to x509 key file")
		f.StringVar(&deviceIDFlag, "device-id", deviceIDFlag, "device id, required for x509")
		f.StringVar(&hostnameFlag, "hostname", hostnameFlag, "hostname to connect to, required for x509")
	})
}

func wrap(fn func(context.Context, *flag.FlagSet, *iotdevice.Client) error) internal.HandlerFunc {
	return func(ctx context.Context, f *flag.FlagSet) error {
		var opts []iotdevice.ClientOption
		if tlsCertFlag != "" {
			if tlsKeyFlag == "" {
				return errors.New("tlsKeyFlag is empty")
			}
			if hostnameFlag == "" {
				return errors.New("hostname must be set when using x509 authentication")
			}
			if deviceIDFlag == "" {
				return errors.New("device id must be set when using 509 authentication")
			}

			opts = append(opts,
				iotdevice.WithX509FromFile(tlsCertFlag, tlsKeyFlag),
				iotdevice.WithHostname(hostnameFlag),
				iotdevice.WithDeviceID(deviceIDFlag),
			)
		} else {
			cs := os.Getenv("DEVICE_CONNECTION_STRING")
			if cs == "" {
				return errors.New("DEVICE_CONNECTION_STRING is blank")
			}
			opts = append(opts, iotdevice.WithConnectionString(cs))
		}

		mk, ok := transports[transportFlag]
		if !ok {
			return fmt.Errorf("unknown transport %q", transportFlag)
		}
		t, err := mk()
		if err != nil {
			return err
		}
		c, err := iotdevice.NewClient(append(opts,
			iotdevice.WithLogger(mklog("[iothub] ")),
			iotdevice.WithTransport(t),
			iotdevice.WithDebug(debugFlag),
		)...)
		if err != nil {
			return err
		}
		if err := c.ConnectInBackground(ctx); err != nil {
			return err
		}
		return fn(ctx, f, c)
	}
}

// mklog enables logging only when debug mode is on
func mklog(prefix string) *log.Logger {
	if !debugFlag {
		return nil
	}
	return log.New(os.Stderr, prefix, 0)
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
	)
}

func watchEvents(ctx context.Context, f *flag.FlagSet, c *iotdevice.Client) error {
	if f.NArg() != 0 {
		return internal.ErrInvalidUsage
	}
	errc := make(chan error, 1)
	if err := c.SubscribeEvents(ctx, func(msg *common.Message) {
		if err := internal.OutputJSON(msg); err != nil {
			errc <- err
		}
	}); err != nil {
		return err
	}
	return <-errc
}

func watchTwin(ctx context.Context, f *flag.FlagSet, c *iotdevice.Client) error {
	if f.NArg() != 0 {
		return internal.ErrInvalidUsage
	}

	errc := make(chan error, 1)
	if err := c.SubscribeTwinUpdates(ctx, func(s iotdevice.TwinState) {
		if err := internal.OutputJSON(s); err != nil {
			errc <- err
		}
	}); err != nil {
		return err
	}
	return <-errc
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
