package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/amenzhinsky/golang-iothub/cmd/internal"
	"github.com/amenzhinsky/golang-iothub/common"
	"github.com/amenzhinsky/golang-iothub/iotservice"
)

// globally accessible by command handlers, is it a good idea?
var (
	uidFlag             = "golang-iothub"
	midFlag             = ""
	cidFlag             = ""
	expFlag             = time.Duration(0)
	ackFlag             = internal.NewChoiceFlag("none", "positive", "negative", "full")
	formatFlag          = internal.NewChoiceFlag("simple", "json")
	connectTimeoutFlag  = 0
	responseTimeoutFlag = 30
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
	return internal.Run(context.Background(), map[string]*internal.Command{
		"send": {
			"DEVICE PAYLOAD [KEY VALUE]...",
			"send a message to the named device (C2D)",
			wrap(send),
			func(fs *flag.FlagSet) {
				fs.Var(ackFlag, "ack", "type of ack feedback")
				fs.StringVar(&uidFlag, "uid", uidFlag, "origin of the message")
				fs.StringVar(&midFlag, "mid", midFlag, "identifier for the message")
				fs.StringVar(&cidFlag, "cid", cidFlag, "message identifier in a request-reply")
				fs.DurationVar(&expFlag, "exp", expFlag, "message lifetime")
			},
		},
		"watch-events": {
			"",
			"subscribe to device messages (D2C)",
			wrap(watchEvents),
			func(fs *flag.FlagSet) {
				fs.Var(formatFlag, "format", "output format <simple|json>")
			},
		},
		"watch-feedback": {
			"",
			"monitor message feedback send by devices",
			wrap(watchFeedback),
			nil,
		},
		"call": {
			"DEVICE METHOD PAYLOAD",
			"call a direct method on the named device (DM)",
			wrap(call),
			func(fs *flag.FlagSet) {
				fs.IntVar(&connectTimeoutFlag, "c", connectTimeoutFlag, "connect timeout in seconds")
				fs.IntVar(&responseTimeoutFlag, "r", responseTimeoutFlag, "response timeout in seconds")
			},
		},
		"update-twin": {
			"DEVICE [KEY VALUE]...",
			"update twin device",
			wrap(updateTwin),
			nil,
		},
		"device": {
			"DEVICE",
			"get device information",
			wrap(device),
			nil,
		},
	}, os.Args, nil)
}

func wrap(fn func(context.Context, *flag.FlagSet, *iotservice.Client) error) internal.HandlerFunc {
	return func(ctx context.Context, fs *flag.FlagSet) error {
		// accept only from environment
		cs := os.Getenv("SERVICE_CONNECTION_STRING")
		if cs == "" {
			return errors.New("SERVICE_CONNECTION_STRING is blank")
		}

		c, err := iotservice.NewClient(
			iotservice.WithLogger(nil), // disable logging
			iotservice.WithConnectionString(cs),
		)
		if err != nil {
			return err
		}
		defer c.Close()
		return fn(ctx, fs, c)
	}
}

func device(ctx context.Context, fs *flag.FlagSet, c *iotservice.Client) error {
	if fs.NArg() != 1 {
		return internal.ErrInvalidUsage
	}

	d, err := c.GetDevice(ctx, fs.Arg(0))
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(d, "", "\t")
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}

func updateTwin(ctx context.Context, fs *flag.FlagSet, c *iotservice.Client) error {
	if fs.NArg() < 3 {
		return internal.ErrInvalidUsage
	}

	m, err := internal.ArgsToMap(fs.Args()[1:])
	if err != nil {
		return err
	}

	props := make(map[string]interface{}, len(m))
	for k, v := range m {
		if v == "null" {
			props[k] = nil
		} else {
			props[k] = v
		}
	}

	twin, err := c.UpdateTwin(ctx, fs.Arg(0), props)
	if err != nil {
		return err
	}
	fmt.Printf("version: %v\n", twin.Properties.Desired["$version"])
	return nil
}

func call(ctx context.Context, fs *flag.FlagSet, c *iotservice.Client) error {
	if fs.NArg() != 3 {
		return internal.ErrInvalidUsage
	}
	var v map[string]interface{}
	if err := json.Unmarshal([]byte(fs.Arg(2)), &v); err != nil {
		return err
	}
	v, err := c.Call(ctx, fs.Arg(0), fs.Arg(1), v,
		iotservice.WithCallConnectTimeout(connectTimeoutFlag),
		iotservice.WithCallResponseTimeout(responseTimeoutFlag),
	)
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}

func send(ctx context.Context, fs *flag.FlagSet, c *iotservice.Client) error {
	if fs.NArg() < 2 {
		return internal.ErrInvalidUsage
	}

	var err error
	var props map[string]string
	if fs.NArg() > 2 {
		props, err = internal.ArgsToMap(fs.Args()[2:])
		if err != nil {
			return err
		}
	}
	if err = c.Connect(ctx); err != nil {
		return err
	}
	expiryTime := time.Time{}
	if expFlag != 0 {
		expiryTime = time.Now().Add(expFlag)
	}
	if err := c.SendEvent(ctx, fs.Arg(0), []byte(fs.Arg(1)),
		iotservice.WithSendMessageID(midFlag),
		iotservice.WithSendAck(ackFlag.String()),
		iotservice.WithSendProperties(props),
		iotservice.WithSendUserID(uidFlag),
		iotservice.WithSendCorrelationID(cidFlag),
		iotservice.WithSentExpiryTime(expiryTime),
	); err != nil {
		return err
	}
	return nil
}

func watchEvents(ctx context.Context, _ *flag.FlagSet, c *iotservice.Client) error {
	return c.SubscribeEvents(ctx, func(msg *common.Message) {
		switch formatFlag.String() {
		case "json":
			b, err := json.MarshalIndent(msg, "", "\t")
			if err != nil {
				panic(err)
			}
			fmt.Println(string(b))
		case "simple":
			fmt.Println(msg.Inspect())
		default:
			panic("unknown output format")
		}
	})
}

// TODO: different formats
func watchFeedback(ctx context.Context, fs *flag.FlagSet, c *iotservice.Client) error {
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
