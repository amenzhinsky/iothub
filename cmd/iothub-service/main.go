package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/amenzhinsky/iothub/cmd/internal"
	"github.com/amenzhinsky/iothub/iotservice"
	"github.com/amenzhinsky/iothub/iotutil"
)

// globally accessible by command handlers, is it a good idea?
var (
	ackFlag    = internal.NewChoiceFlag("none", "positive", "negative", "full")
	formatFlag = internal.NewChoiceFlag("simple", "json")
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
	// accept only from environment
	cs := os.Getenv("SERVICE_CONNECTION_STRING")
	if cs == "" {
		return errors.New("SERVICE_CONNECTION_STRING is blank")
	}

	c, err := iotservice.New(
		iotservice.WithLogger(nil), // disable logging
		iotservice.WithConnectionString(cs),
	)
	if err != nil {
		return err
	}
	defer c.Close()

	return internal.Run(context.Background(), map[string]*internal.Command{
		"send": {
			"DEVICE PAYLOAD [KEY VALUE]...",
			"send a message to the named device (C2D)",
			send(c),
			func(fs *flag.FlagSet) {
				fs.Var(ackFlag, "ack", "type of ack feedback")
			},
		},
		"watch-events": {
			"",
			"subscribe to device messages (D2C)",
			watchEvents(c),
			func(fs *flag.FlagSet) {
				fs.Var(formatFlag, "format", "output format <simple|json>")
			},
		},
		"watch-feedback": {
			"",
			"monitor message feedback send by devices",
			watchFeedback(c),
			nil,
		},
		"call": {
			"DEVICE METHOD PAYLOAD",
			"call a direct method on the named device (DM)",
			directMethod(c),
			nil,
		},
	}, os.Args, nil)
}

func directMethod(c *iotservice.Client) internal.HandlerFunc {
	return func(ctx context.Context, fs *flag.FlagSet) error {
		if fs.NArg() != 3 {
			return internal.ErrInvalidUsage
		}
		var v map[string]interface{}
		if err := json.Unmarshal([]byte(fs.Arg(2)), &v); err != nil {
			return err
		}
		v, err := c.InvokeMethod(ctx, fs.Arg(0), fs.Arg(1), v)
		if err != nil {
			return err
		}
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		fmt.Println(string(b))
		return nil
	}
}

func send(c *iotservice.Client) internal.HandlerFunc {
	return func(ctx context.Context, fs *flag.FlagSet) error {
		if fs.NArg() < 2 {
			return internal.ErrInvalidUsage
		}

		// number of props arguments has to be even
		// they are used as keys and values of props map.
		p := map[string]string{}
		if fs.NArg() > 2 {
			if fs.NArg()%2 != 0 {
				return errors.New("number of key-value arguments must be even")
			}
			for i := 2; i < fs.NArg(); i += 2 {
				p[fs.Arg(i)] = fs.Arg(i + 1)
			}
		}

		if err := c.Connect(ctx); err != nil {
			return err
		}
		msgID, err := c.Publish(ctx, &iotservice.Event{
			DeviceID:   fs.Arg(0),
			Payload:    []byte(fs.Arg(1)),
			Properties: p,
			Ack:        ackFlag.String(),
		})
		if err != nil {
			return err
		}
		fmt.Println(msgID)
		return nil
	}
}

const eventFormat = `----- DEVICE -----------------
%s
----- PROPERTIES -------------
%s
----- PAYLOAD ----------------
%s
----- METADATA----------------
%s
==============================
`

func watchEvents(c *iotservice.Client) internal.HandlerFunc {
	return func(ctx context.Context, fs *flag.FlagSet) error {
		return c.Subscribe(ctx, func(ev *iotservice.Event) {
			switch formatFlag.String() {
			case "json":
				b, err := json.Marshal(ev)
				if err != nil {
					panic(err)
				}
				fmt.Println(string(b))
			case "simple":
				fmt.Printf(eventFormat,
					ev.DeviceID,
					iotutil.FormatProperties(ev.Properties),
					iotutil.FormatPayload(ev.Payload),
					iotutil.FormatProperties(ev.Metadata),
				)
			default:
				panic("unknown output format")
			}
		})
	}
}

// TODO: format
func watchFeedback(c *iotservice.Client) internal.HandlerFunc {
	return func(ctx context.Context, fs *flag.FlagSet) error {
		if err := c.Connect(context.Background()); err != nil {
			return err
		}
		return c.SubscribeFeedback(ctx, func(f *iotservice.Feedback) {
			b, err := json.MarshalIndent(f, "", "  ")
			if err != nil {
				panic(err)
			}
			fmt.Println(string(b))
		})
	}
}
