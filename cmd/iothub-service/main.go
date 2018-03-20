package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
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
	ackFlag             = ""
	connectTimeoutFlag  = 0
	responseTimeoutFlag = 30

	// create device
	autoGenerateFlag = false

	// create/update device
	primaryKeyFlag          = ""
	secondaryKeyFlag        = ""
	primaryThumbprintFlag   = ""
	secondaryThumbprintFlag = ""

	// common flags
	debugFlag = false

	// sas and connection string
	secondaryFlag = false

	// sas
	uriFlag      = ""
	durationFlag = time.Hour
)

func main() {
	if err := run(); err != nil {
		if err != internal.ErrInvalidUsage {
			fmt.Fprintf(os.Stderr, "error: %s\n", err)
		}
		os.Exit(1)
	}
}

const help = `Helps with interacting and managing your iothub devices. 
The $SERVICE_CONNECTION_STRING environment variable is required for authentication.`

func run() error {
	cli, err := internal.New(help, func(f *flag.FlagSet) {
		f.BoolVar(&debugFlag, "debug", debugFlag, "enable debug mode")
	}, []*internal.Command{
		{
			"send", "s",
			"DEVICE PAYLOAD [KEY VALUE]...",
			"send a message to the named device (C2D)",
			wrap(send),
			func(f *flag.FlagSet) {
				f.StringVar(&ackFlag, "ack", ackFlag, "type of ack feedback")
				f.StringVar(&uidFlag, "uid", uidFlag, "origin of the message")
				f.StringVar(&midFlag, "mid", midFlag, "identifier for the message")
				f.StringVar(&cidFlag, "cid", cidFlag, "message identifier in a request-reply")
				f.DurationVar(&expFlag, "exp", expFlag, "message lifetime")
			},
		},
		{
			"watch-events", "we",
			"", "subscribe to device messages (D2C)",
			wrap(watchEvents),
			nil,
		},
		{
			"watch-feedback", "wf",
			"", "monitor message feedback send by devices",
			wrap(watchFeedback),
			nil,
		},
		{
			"call", "c",
			"DEVICE METHOD PAYLOAD", "call a direct method on a device",
			wrap(call),
			func(f *flag.FlagSet) {
				f.IntVar(&connectTimeoutFlag, "c", connectTimeoutFlag, "connect timeout in seconds")
				f.IntVar(&responseTimeoutFlag, "r", responseTimeoutFlag, "response timeout in seconds")
			},
		},
		{
			"device", "d",
			"DEVICE", "get device information",
			wrap(device),
			nil,
		},
		{
			"devices", "ds",
			"", "list all available devices",
			wrap(devices),
			nil,
		},
		{
			"create-device", "cd",
			"DEVICE", "creates a new device",
			wrap(createDevice),
			func(f *flag.FlagSet) {
				f.BoolVar(&autoGenerateFlag, "auto", false, "auto generate keys")
				f.StringVar(&primaryKeyFlag, "primary-key", "", "primary key (base64)")
				f.StringVar(&secondaryKeyFlag, "secondary-key", "", "secondary key (base64)")
				f.StringVar(&primaryThumbprintFlag, "primary-thumbprint", "", "x509 primary thumbprint")
				f.StringVar(&secondaryThumbprintFlag, "secondary-thumbprint", "", "x509 secondary thumbprint")
			},
		},
		{
			"update-device", "ud",
			"DEVICE", "updates the named device",
			wrap(updateDevice),
			func(f *flag.FlagSet) {
				f.StringVar(&primaryKeyFlag, "primary-key", "", "primary key (base64)")
				f.StringVar(&secondaryKeyFlag, "secondary-key", "", "secondary key (base64)")
				f.StringVar(&primaryThumbprintFlag, "primary-thumbprint", "", "x509 primary thumbprint")
				f.StringVar(&secondaryThumbprintFlag, "secondary-thumbprint", "", "x509 secondary thumbprint")
			},
		},
		{
			"delete-device", "dd",
			"DEVICE", "delete the named device",
			wrap(deleteDevice),
			nil,
		},
		{
			"twin", "t",
			"", "inspect the named twin device",
			wrap(twin),
			nil,
		},
		{
			"update-twin", "ut",
			"DEVICE [KEY VALUE]...", "update the named twin device",
			wrap(updateTwin),
			nil,
		},
		{
			"stats", "st",
			"", "get statistics about the devices",
			wrap(stats),
			nil,
		},
		{
			"jobs", "js",
			"", "list the last import/export jobs",
			wrap(jobs),
			nil,
		},
		{
			"job", "j",
			"ID", "get the status of a import/export job",
			wrap(job),
			nil,
		},
		{
			"cancel-job", "cj",
			"", "cancel a import/export job",
			wrap(cancelJob),
			nil,
		},
		{
			"connection-string", "cs",
			"DEVICE", "get a device's connection string",
			wrap(connectionString),
			func(f *flag.FlagSet) {
				f.BoolVar(&secondaryFlag, "secondary", secondaryFlag, "use the secondary key instead")
			},
		},
		{
			"access-signature", "as",
			"DEVICE", "generate a SAS token",
			wrap(sas),
			func(f *flag.FlagSet) {
				f.StringVar(&uriFlag, "uri", uriFlag, "storage resource uri")
				f.DurationVar(&durationFlag, "duration", durationFlag, "token validity time")
				f.BoolVar(&secondaryFlag, "secondary", secondaryFlag, "use the secondary key instead")
			},
		},
	})
	if err != nil {
		return err
	}
	return cli.Run(context.Background(), os.Args...)
}

func wrap(fn func(context.Context, *flag.FlagSet, *iotservice.Client) error) internal.HandlerFunc {
	return func(ctx context.Context, f *flag.FlagSet) error {
		// accept only from environment
		cs := os.Getenv("SERVICE_CONNECTION_STRING")
		if cs == "" {
			return errors.New("SERVICE_CONNECTION_STRING is blank")
		}

		var logger *log.Logger
		if debugFlag {
			logger = log.New(os.Stderr, "[iotservice] ", 0)
		}

		c, err := iotservice.NewClient(
			iotservice.WithLogger(nil), // disable logging
			iotservice.WithConnectionString(cs),
			iotservice.WithLogger(logger),
			iotservice.WithDebug(debugFlag),
		)
		if err != nil {
			return err
		}
		defer c.Close()
		return fn(ctx, f, c)
	}
}

func device(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	d, err := c.GetDevice(ctx, f.Arg(0))
	if err != nil {
		return err
	}
	return internal.OutputJSON(d)
}

func devices(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 0 {
		return internal.ErrInvalidUsage
	}
	d, err := c.ListDevices(ctx)
	if err != nil {
		return err
	}
	return internal.OutputJSON(d)
}

func createDevice(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}

	device := &iotservice.Device{DeviceID: f.Arg(0)}
	if autoGenerateFlag {
		var err error
		primaryKeyFlag, err = iotservice.NewSymmetricKey()
		if err != nil {
			return err
		}
		secondaryKeyFlag, err = iotservice.NewSymmetricKey()
		if err != nil {
			return err
		}
	}
	if primaryKeyFlag != "" || secondaryKeyFlag != "" {
		device.Authentication = &iotservice.Authentication{
			Type: iotservice.AuthSAS,
			SymmetricKey: &iotservice.SymmetricKey{
				PrimaryKey:   primaryKeyFlag,
				SecondaryKey: secondaryKeyFlag,
			},
		}
	}
	if primaryThumbprintFlag != "" || secondaryThumbprintFlag != "" {
		device.Authentication = &iotservice.Authentication{
			Type: iotservice.AuthSelfSigned,
			X509Thumbprint: &iotservice.X509Thumbprint{
				PrimaryThumbprint:   primaryThumbprintFlag,
				SecondaryThumbprint: secondaryThumbprintFlag,
			},
		}
	}

	d, err := c.CreateDevice(ctx, device)
	if err != nil {
		return err
	}
	return internal.OutputJSON(d)
}

func updateDevice(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	device := &iotservice.Device{DeviceID: f.Arg(0)}
	if primaryKeyFlag != "" || secondaryKeyFlag != "" {
		device.Authentication = &iotservice.Authentication{
			Type: "sas",
			SymmetricKey: &iotservice.SymmetricKey{
				PrimaryKey:   primaryKeyFlag,
				SecondaryKey: secondaryKeyFlag,
			},
		}
	}
	if primaryThumbprintFlag != "" || secondaryThumbprintFlag != "" {
		device.Authentication = &iotservice.Authentication{
			Type: "selfSigned",
			X509Thumbprint: &iotservice.X509Thumbprint{
				PrimaryThumbprint:   primaryThumbprintFlag,
				SecondaryThumbprint: secondaryThumbprintFlag,
			},
		}
	}
	d, err := c.UpdateDevice(ctx, device)
	if err != nil {
		return err
	}
	return internal.OutputJSON(d)
}

func deleteDevice(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	return c.DeleteDevice(ctx, f.Arg(0))
}

func stats(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 0 {
		return internal.ErrInvalidUsage
	}
	s, err := c.Stats(ctx)
	if err != nil {
		return err
	}
	return internal.OutputJSON(s)
}

func twin(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	t, err := c.GetTwin(ctx, f.Arg(0))
	if err != nil {
		return err
	}
	return internal.OutputJSON(t)
}

func updateTwin(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() < 3 {
		return internal.ErrInvalidUsage
	}

	m, err := internal.ArgsToMap(f.Args()[1:])
	if err != nil {
		return err
	}

	twin := &iotservice.Twin{
		Properties: &iotservice.Properties{
			Desired: make(map[string]interface{}, len(m)),
		},
	}
	for k, v := range m {
		if v == "null" {
			twin.Properties.Desired[k] = nil
		} else {
			twin.Properties.Desired[k] = v
		}
	}

	twin, err = c.UpdateTwin(ctx, f.Arg(0), twin, "*")
	if err != nil {
		return err
	}
	return internal.OutputJSON(twin)
}

func call(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 3 {
		return internal.ErrInvalidUsage
	}
	var v map[string]interface{}
	if err := json.Unmarshal([]byte(f.Arg(2)), &v); err != nil {
		return err
	}
	r, err := c.Call(ctx, f.Arg(0), f.Arg(1), v,
		iotservice.WithCallConnectTimeout(connectTimeoutFlag),
		iotservice.WithCallResponseTimeout(responseTimeoutFlag),
	)
	if err != nil {
		return err
	}
	return internal.OutputJSON(r)
}

func send(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() < 2 {
		return internal.ErrInvalidUsage
	}

	var err error
	var props map[string]string
	if f.NArg() > 2 {
		props, err = internal.ArgsToMap(f.Args()[2:])
		if err != nil {
			return err
		}
	}
	expiryTime := time.Time{}
	if expFlag != 0 {
		expiryTime = time.Now().Add(expFlag)
	}
	if err := c.SendEvent(ctx, f.Arg(0), []byte(f.Arg(1)),
		iotservice.WithSendMessageID(midFlag),
		iotservice.WithSendAck(ackFlag),
		iotservice.WithSendProperties(props),
		iotservice.WithSendUserID(uidFlag),
		iotservice.WithSendCorrelationID(cidFlag),
		iotservice.WithSentExpiryTime(expiryTime),
	); err != nil {
		return err
	}
	return nil
}

func watchEvents(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
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

func watchFeedback(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 0 {
		return internal.ErrInvalidUsage
	}
	errc := make(chan error, 1)
	if err := c.SubscribeFeedback(ctx, func(f *iotservice.Feedback) {
		if err := internal.OutputJSON(f); err != nil {
			errc <- err
		}
	}); err != nil {
		return err
	}
	return <-errc
}

func jobs(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 0 {
		return internal.ErrInvalidUsage
	}
	v, err := c.ListJobs(ctx)
	if err != nil {
		return err
	}
	return internal.OutputJSON(v)
}

func job(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	v, err := c.GetJob(ctx, f.Arg(0))
	if err != nil {
		return err
	}
	return internal.OutputJSON(v)
}

func cancelJob(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	v, err := c.CancelJob(ctx, f.Arg(0))
	if err != nil {
		return err
	}
	return internal.OutputJSON(v)
}

func connectionString(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}

	d, err := c.GetDevice(ctx, f.Arg(0))
	if err != nil {
		return err
	}
	cs, err := c.DeviceConnectionString(d, secondaryFlag)
	if err != nil {
		return err
	}
	return internal.OutputLine(cs)
}

func sas(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	d, err := c.GetDevice(ctx, f.Arg(0))
	if err != nil {
		return err
	}
	sas, err := c.DeviceSAS(d, durationFlag, secondaryFlag)
	if err != nil {
		return err
	}
	return internal.OutputLine(sas)
}
