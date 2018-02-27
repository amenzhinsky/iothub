package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/amenzhinsky/iothub/cmd/internal"
	"github.com/amenzhinsky/iothub/iotdevice"
	"github.com/amenzhinsky/iothub/transport"
	"github.com/amenzhinsky/iothub/transport/amqp"
	"github.com/amenzhinsky/iothub/transport/mqtt"
)

var transports = map[string]func() (transport.Transport, error){
	"mqtt": func() (transport.Transport, error) { return mqtt.New() },
	"amqp": func() (transport.Transport, error) { return amqp.New() },
	"http": func() (transport.Transport, error) { return nil, errors.New("not implemented") },
}

var (
	debugFlag     = false
	transportFlag = "mqtt"
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
		"send":         {"PAYLOAD [KEY VALUE]...", "send a message to the cloud (D2C)", conn(send), nil},
		"watch-events": {"", "subscribe to events sent from the cloud (C2D)", conn(watchEvents), nil},
		"watch-twin":   {"", "subscribe to twin device updates", conn(watchTwin), nil},

		// TODO: other methods
	}, os.Args, func(fs *flag.FlagSet) {
		fs.BoolVar(&debugFlag, "d", debugFlag, "enable debug mode")
		fs.StringVar(&transportFlag, "t", transportFlag, "transport to use (mqtt, amqp, http)")
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
			iotdevice.WithLogger(nil), // disable logging
			iotdevice.WithDebug(debugFlag),
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

const eventFormat = `---- PAYLOAD --------------
%s
---------------------------
%v
===========================
`

func watchEvents(ctx context.Context, fs *flag.FlagSet, c *iotdevice.Client) error {
	if fs.NArg() != 0 {
		return internal.ErrInvalidUsage
	}
	return c.SubscribeEvents(ctx, func(ev *iotdevice.Event) {
		fmt.Printf(eventFormat, ev.Payload, ev.Properties)
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
