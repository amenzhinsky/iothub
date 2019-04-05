package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/amenzhinsky/iothub/cmd/internal"
	"github.com/amenzhinsky/iothub/common"
	"github.com/amenzhinsky/iothub/common/commonamqp"
	"github.com/amenzhinsky/iothub/eventhub"
	"github.com/amenzhinsky/iothub/iotservice"
	"pack.ag/amqp"
)

// globally accessible by command handlers, is it a good idea?
var (
	// common
	debugFlag    bool
	compressFlag bool

	// send
	uidFlag             string
	midFlag             string
	cidFlag             string
	expFlag             time.Duration
	ackFlag             string
	connectTimeoutFlag  int
	responseTimeoutFlag int

	// create/update device
	primaryKeyFlag          string
	secondaryKeyFlag        string
	primaryThumbprintFlag   string
	secondaryThumbprintFlag string
	caFlag                  bool

	// sas and connection string
	secondaryFlag bool

	// sas
	uriFlag      string
	durationFlag time.Duration

	// watch events
	ehcsFlag string
	ehcgFlag string
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
		f.BoolVar(&compressFlag, "compress", false, "compress data (remove JSON indentations)")
	}, []*internal.Command{
		{
			"send", "s",
			"DEVICE PAYLOAD [KEY VALUE]...",
			"send a message to the named device (C2D)",
			wrap(send),
			func(f *flag.FlagSet) {
				f.StringVar(&ackFlag, "ack", "", "type of ack feedback")
				f.StringVar(&uidFlag, "uid", "golang-iothub", "origin of the message")
				f.StringVar(&midFlag, "mid", "", "identifier for the message")
				f.StringVar(&cidFlag, "cid", "", "message identifier in a request-reply")
				f.DurationVar(&expFlag, "exp", 0, "message lifetime")
			},
		},
		{
			"watch-events", "we",
			"", "subscribe to device messages (D2C)",
			wrap(watchEvents),
			func(f *flag.FlagSet) {
				f.StringVar(&ehcsFlag, "ehcs", "", "custom eventhub connection string")
				f.StringVar(&ehcgFlag, "ehcg", "$Default", "eventhub consumer group")
			},
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
				f.IntVar(&connectTimeoutFlag, "c", 0, "connect timeout in seconds")
				f.IntVar(&responseTimeoutFlag, "r", 30, "response timeout in seconds")
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
				f.StringVar(&primaryKeyFlag, "primary-key", "", "primary key (base64)")
				f.StringVar(&secondaryKeyFlag, "secondary-key", "", "secondary key (base64)")
				f.StringVar(&primaryThumbprintFlag, "primary-thumbprint", "", "x509 primary thumbprint")
				f.StringVar(&secondaryThumbprintFlag, "secondary-thumbprint", "", "x509 secondary thumbprint")
				f.BoolVar(&caFlag, "ca", false, "use certificate authority authentication")
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
				f.BoolVar(&caFlag, "ca", false, "use certificate authority authentication")
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
				f.BoolVar(&secondaryFlag, "secondary", false, "use the secondary key instead")
			},
		},
		{
			"access-signature", "sas",
			"DEVICE", "generate a SAS token",
			wrap(sas),
			func(f *flag.FlagSet) {
				f.StringVar(&uriFlag, "uri", "", "storage resource uri")
				f.DurationVar(&durationFlag, "duration", time.Hour, "token validity time")
				f.BoolVar(&secondaryFlag, "secondary", false, "use the secondary key instead")
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
		c, err := iotservice.NewClient(
			iotservice.WithConnectionString(cs),
			iotservice.WithLogger(common.NewLogWrapper(debugFlag)),
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
	return internal.OutputJSON(d, compressFlag)
}

func devices(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 0 {
		return internal.ErrInvalidUsage
	}
	d, err := c.ListDevices(ctx)
	if err != nil {
		return err
	}
	return internal.OutputJSON(d, compressFlag)
}

func createDevice(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	a, err := mkAuthentication()
	if err != nil {
		return err
	}
	d, err := c.CreateDevice(ctx, &iotservice.Device{
		DeviceID:       f.Arg(0),
		Authentication: a,
	})
	if err != nil {
		return err
	}
	return internal.OutputJSON(d, compressFlag)
}

func updateDevice(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	a, err := mkAuthentication()
	if err != nil {
		return err
	}
	d, err := c.UpdateDevice(ctx, &iotservice.Device{
		DeviceID:       f.Arg(0),
		Authentication: a,
	})
	if err != nil {
		return err
	}
	return internal.OutputJSON(d, compressFlag)
}

func mkAuthentication() (*iotservice.Authentication, error) {
	// TODO: validate that flags only of one type of auth can be passed
	if primaryThumbprintFlag != "" || secondaryThumbprintFlag != "" {
		return &iotservice.Authentication{
			Type: iotservice.AuthSelfSigned,
			X509Thumbprint: &iotservice.X509Thumbprint{
				PrimaryThumbprint:   primaryThumbprintFlag,
				SecondaryThumbprint: secondaryThumbprintFlag,
			},
		}, nil
	}
	if caFlag {
		return &iotservice.Authentication{
			Type: iotservice.AuthCA,
		}, nil
	}

	// auto-generate keys when no auth type is given
	var err error
	if primaryKeyFlag == "" {
		primaryKeyFlag, err = iotservice.NewSymmetricKey()
		if err != nil {
			return nil, err
		}
	}
	if secondaryKeyFlag == "" {
		secondaryKeyFlag, err = iotservice.NewSymmetricKey()
		if err != nil {
			return nil, err
		}
	}
	return &iotservice.Authentication{
		Type: iotservice.AuthSAS,
		SymmetricKey: &iotservice.SymmetricKey{
			PrimaryKey:   primaryKeyFlag,
			SecondaryKey: secondaryKeyFlag,
		},
	}, nil
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
	return internal.OutputJSON(s, compressFlag)
}

func twin(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	t, err := c.GetTwin(ctx, f.Arg(0))
	if err != nil {
		return err
	}
	return internal.OutputJSON(t, compressFlag)
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
	return internal.OutputJSON(twin, compressFlag)
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
	return internal.OutputJSON(r, compressFlag)
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

	if ehcsFlag != "" {
		return watchEventHubEvents(ctx, ehcsFlag, ehcgFlag)
	}

	errc := make(chan error, 1)
	if err := c.SubscribeEvents(ctx, func(msg *common.Message) {
		if err := internal.OutputJSON(msg, compressFlag); err != nil {
			errc <- err
		}
	}); err != nil {
		return err
	}
	return <-errc
}

func watchEventHubEvents(ctx context.Context, cs, group string) error {
	creds, err := eventhub.ParseConnectionString(cs)
	if err != nil {
		return err
	}

	addr := fmt.Sprintf("amqps://%s/%s", creds.Endpoint, creds.EntityPath)
	eh, err := eventhub.Dial(addr,
		eventhub.WithLogger(common.NewLogWrapper(debugFlag)),
		eventhub.WithTLSConfig(common.TLSConfig(creds.Endpoint)),

		// we cannot use username and password as a part of Dial connection string
		// because access key is base64 (not base64url) encoded and may contain
		// '=' or '+' chars that break URL parsing, simply replacing them with
		// '_' and '+' won't do the job because amqp.Dial uses them as is and
		// Azure will reject them afterwards
		eventhub.WithSASLPlain(creds.SharedAccessKeyName, creds.SharedAccessKey),
	)
	if err != nil {
		return err
	}
	return eh.SubscribePartitions(ctx, creds.EntityPath, group, func(m *amqp.Message) {
		msg := commonamqp.FromAMQPMessage(m)
		if err := internal.OutputJSON(msg, compressFlag); err != nil {
			panic(err)
		}
	})
}

func watchFeedback(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 0 {
		return internal.ErrInvalidUsage
	}
	errc := make(chan error, 1)
	if err := c.SubscribeFeedback(ctx, func(f *iotservice.Feedback) {
		if err := internal.OutputJSON(f, compressFlag); err != nil {
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
	return internal.OutputJSON(v, compressFlag)
}

func job(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	v, err := c.GetJob(ctx, f.Arg(0))
	if err != nil {
		return err
	}
	return internal.OutputJSON(v, compressFlag)
}

func cancelJob(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	v, err := c.CancelJob(ctx, f.Arg(0))
	if err != nil {
		return err
	}
	return internal.OutputJSON(v, compressFlag)
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
