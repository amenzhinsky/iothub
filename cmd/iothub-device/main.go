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

	"github.com/amenzhinsky/iothub/cmd/internal"
	"github.com/amenzhinsky/iothub/iotdevice"
	"github.com/amenzhinsky/iothub/iotutil"
	"github.com/amenzhinsky/iothub/transport"
	"github.com/amenzhinsky/iothub/transport/amqp"
	"github.com/amenzhinsky/iothub/transport/mqtt"
)

var transports = map[string]func() (transport.Transport, error){
	"mqtt": func() (transport.Transport, error) {
		return mqtt.New(mqtt.WithLogger(mklog("[mqtt]   ")))
	},
	"amqp": func() (transport.Transport, error) {
		return amqp.New(amqp.WithLogger(mklog("[amqp]   ")))
	},
	"http": func() (transport.Transport, error) {
		return nil, errors.New("not implemented")
	},
}

var (
	debugFlag     = false
	quiteFlag     = false
	transportFlag = "mqtt"
	formatFlag    = internal.NewChoiceFlag("simple", "json")
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
			conn(send),
			nil,
		},
		"watch-events": {
			"",
			"subscribe to events sent from the cloud (C2D)",
			conn(watchEvents),
			func(fs *flag.FlagSet) {
				fs.Var(formatFlag, "format", "output format <simple|json>")
			},
		},
		"watch-twin": {
			"",
			"subscribe to twin device updates",
			conn(watchTwin),
			nil,
		},
		"direct-method": {
			"NAME",
			"handle the named direct method, reads responses from STDIN",
			conn(directMethod),
			func(fs *flag.FlagSet) {
				fs.BoolVar(&quiteFlag, "quite", quiteFlag, "disable additional hints")
			},
		},

		// TODO: other methods
	}, os.Args, func(fs *flag.FlagSet) {
		fs.BoolVar(&debugFlag, "debug", debugFlag, "enable debug mode")
		fs.StringVar(&transportFlag, "transport", transportFlag, "transport to use <mqtt|amqp|http>")
	})
}

func conn(fn func(context.Context, *flag.FlagSet, *iotdevice.Client) error) internal.HandlerFunc {
	return func(ctx context.Context, fs *flag.FlagSet) error {
		s := os.Getenv("DEVICE_CONNECTION_STRING")
		if s == "" {
			return errors.New("DEVICE_CONNECTION_STRING is blank")
		}
		f, ok := transports[transportFlag]
		if !ok {
			return fmt.Errorf("unknown transport %q", transportFlag)
		}
		t, err := f()
		if err != nil {
			return err
		}
		c, err := iotdevice.New(
			iotdevice.WithLogger(mklog("[iothub] ")),
			iotdevice.WithConnectionString(s),
			iotdevice.WithTransport(t),
		)
		if err != nil {
			return err
		}
		if err := c.ConnectInBackground(ctx, false); err != nil {
			return err
		}
		return fn(ctx, fs, c)
	}
}

// mklog enables logging only when debug mode is on
func mklog(prefix string) *log.Logger {
	if !debugFlag {
		return nil
	}
	return log.New(os.Stderr, prefix, 0)
}

func send(ctx context.Context, fs *flag.FlagSet, c *iotdevice.Client) error {
	if fs.NArg() < 1 {
		return internal.ErrInvalidUsage
	}
	p := map[string]string{}
	if fs.NArg() > 1 {
		if fs.NArg()%2 != 1 {
			return errors.New("number of key-value arguments must be even")
		}
		for i := 1; i < fs.NArg(); i += 2 {
			p[fs.Arg(i)] = fs.Arg(i + 1)
		}
	}
	return c.Publish(ctx, &iotdevice.Event{
		Payload:    []byte(fs.Arg(0)),
		Properties: p,
	})
}

const eventFormat = `---- PROPERTIES -----------
%s
---- PAYLOAD --------------
%v
===========================
`

func watchEvents(ctx context.Context, fs *flag.FlagSet, c *iotdevice.Client) error {
	if fs.NArg() != 0 {
		return internal.ErrInvalidUsage
	}
	return c.SubscribeEvents(ctx, func(ev *iotdevice.Event) {
		switch formatFlag.String() {
		case "json":
			b, err := json.Marshal(ev)
			if err != nil {
				panic(err)
			}
			fmt.Println(string(b))
		case "simple":
			fmt.Printf(eventFormat,
				iotutil.FormatProperties(ev.Properties),
				iotutil.FormatPayload(ev.Payload),
			)
		default:
			panic("unknown output format")
		}
	})
}

func watchTwin(ctx context.Context, fs *flag.FlagSet, c *iotdevice.Client) error {
	if fs.NArg() != 0 {
		return internal.ErrInvalidUsage
	}
	return c.SubscribeTwinChanges(ctx, func(s iotdevice.State) {
		b, err := json.MarshalIndent(s, "", "  ")
		if err != nil {
			panic(err)
		}
		fmt.Println(string(b))
	})
}

func directMethod(ctx context.Context, fs *flag.FlagSet, c *iotdevice.Client) error {
	if fs.NArg() != 1 {
		return internal.ErrInvalidUsage
	}

	// if an error occurs during a method invocation,
	// immediately return and display the error
	errc := make(chan error, 1)

	go func() {
		read := bufio.NewReader(os.Stdin)
		errc <- c.HandleMethod(ctx, fs.Arg(0),
			func(p map[string]interface{}) (map[string]interface{}, error) {
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
				b, _, err = read.ReadLine()
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
			})
	}()

	return <-errc
}
