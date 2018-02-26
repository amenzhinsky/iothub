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
	cs := os.Getenv("DEVICE_CONNECTION_STRING")
	if cs == "" {
		return errors.New("DEVICE_CONNECTION_STRING is blank")
	}

	c, err := iotdevice.New(
		iotdevice.WithLogger(nil), // disable logging
		iotdevice.WithConnectionString(cs),
	)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	return internal.Run(ctx, map[string]*internal.Command{
		"send":         {"PAYLOAD [KEY VALUE]...", "send a message to the cloud (D2C)", send(c), nil},
		"watch-events": {"", "subscribe to events sent from the cloud (C2D)", watchEvents(c), nil},
		"watch-twin":   {"", "subscribe to twin device updates", watchTwin(c), nil},

		// TODO: other methods
	}, os.Args)
}

func send(c *iotdevice.Client) internal.HandlerFunc {
	return func(ctx context.Context, fs *flag.FlagSet) error {
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

		if err := c.Connect(ctx, false); err != nil {
			return err
		}
		defer c.Close()

		return c.Publish(ctx, &iotdevice.Event{
			Payload:    []byte(fs.Arg(0)),
			Properties: p,
		})
	}
}

const eventFormat = `---- PAYLOAD --------------
%s
---------------------------
%v
===========================
`

func watchEvents(c *iotdevice.Client) internal.HandlerFunc {
	return func(ctx context.Context, fs *flag.FlagSet) error {
		if fs.NArg() != 0 {
			return internal.ErrInvalidUsage
		}

		if err := c.Connect(ctx, false); err != nil {
			return err
		}
		defer c.Close()

		return c.SubscribeEvents(ctx, func(ev *iotdevice.Event) {
			fmt.Printf(eventFormat, ev.Payload, ev.Properties)
		})
	}
}

func watchTwin(c *iotdevice.Client) internal.HandlerFunc {
	return func(ctx context.Context, fs *flag.FlagSet) error {
		if fs.NArg() != 0 {
			return internal.ErrInvalidUsage
		}

		if err := c.Connect(ctx, false); err != nil {
			return err
		}
		defer c.Close()

		return c.SubscribeTwinChanges(ctx, func(s iotdevice.State) {
			b, err := json.MarshalIndent(s, "", "  ")
			if err != nil {
				panic(err)
			}
			fmt.Println(string(b))
		})
	}
}
