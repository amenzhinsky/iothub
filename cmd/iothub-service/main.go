package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/amenzhinsky/iothub/cmd/internal"
	"github.com/amenzhinsky/iothub/eventhub"
	"github.com/amenzhinsky/iothub/iotservice"
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
The $IOTHUB_SERVICE_CONNECTION_STRING environment variable is required for authentication.`

func run() error {
	cli, err := internal.New(help, func(f *flag.FlagSet) {
		f.BoolVar(&debugFlag, "debug", debugFlag, "enable debug mode")
		f.BoolVar(&compressFlag, "compress", false, "compress data (remove JSON indentations)")
	}, []*internal.Command{
		{
			Name:    "send",
			Alias:   "s",
			Help:    "DEVICE PAYLOAD [[key value]...]",
			Desc:    "send a message to the named device (C2D)",
			Handler: wrap(send),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&ackFlag, "ack", "", "type of ack feedback")
				f.StringVar(&uidFlag, "uid", "golang-iothub", "origin of the message")
				f.StringVar(&midFlag, "mid", "", "identifier for the message")
				f.StringVar(&cidFlag, "cid", "", "message identifier in a request-reply")
				f.DurationVar(&expFlag, "exp", 0, "message lifetime")
			},
		},
		{
			Name:    "watch-events",
			Alias:   "we",
			Desc:    "subscribe to device messages (D2C)",
			Handler: wrap(watchEvents),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&ehcsFlag, "ehcs", "", "custom eventhub connection string")
				f.StringVar(&ehcgFlag, "ehcg", "$Default", "eventhub consumer group")
			},
		},
		{
			Name:    "watch-feedback",
			Alias:   "wf",
			Desc:    "monitor message feedback send by devices",
			Handler: wrap(watchFeedback),
		},
		{
			Name:    "call",
			Alias:   "c",
			Help:    "DEVICE METHOD PAYLOAD",
			Desc:    "call a direct method on a device",
			Handler: wrap(call),
			ParseFunc: func(f *flag.FlagSet) {
				f.IntVar(&connectTimeoutFlag, "c", 0, "connect timeout in seconds")
				f.IntVar(&responseTimeoutFlag, "r", 30, "response timeout in seconds")
			},
		},
		{
			Name:    "device",
			Alias:   "d",
			Help:    "DEVICE",
			Desc:    "get device information",
			Handler: wrap(device),
		},
		{
			Name:    "devices",
			Alias:   "ds",
			Desc:    "list all available devices",
			Handler: wrap(devices),
		},
		{
			Name:    "create-device",
			Alias:   "cd",
			Help:    "DEVICE",
			Desc:    "creates a new device",
			Handler: wrap(createDevice),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&primaryKeyFlag, "primary-key", "", "primary key (base64)")
				f.StringVar(&secondaryKeyFlag, "secondary-key", "", "secondary key (base64)")
				f.StringVar(&primaryThumbprintFlag, "primary-thumbprint", "", "x509 primary thumbprint")
				f.StringVar(&secondaryThumbprintFlag, "secondary-thumbprint", "", "x509 secondary thumbprint")
				f.BoolVar(&caFlag, "ca", false, "use certificate authority authentication")
			},
		},
		{
			Name:    "update-device",
			Alias:   "ud",
			Help:    "DEVICE",
			Desc:    "updates the named device",
			Handler: wrap(updateDevice),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&primaryKeyFlag, "primary-key", "", "primary key (base64)")
				f.StringVar(&secondaryKeyFlag, "secondary-key", "", "secondary key (base64)")
				f.StringVar(&primaryThumbprintFlag, "primary-thumbprint", "", "x509 primary thumbprint")
				f.StringVar(&secondaryThumbprintFlag, "secondary-thumbprint", "", "x509 secondary thumbprint")
				f.BoolVar(&caFlag, "ca", false, "use certificate authority authentication")
			},
		},
		{
			Name:    "delete-device",
			Alias:   "dd",
			Help:    "DEVICE",
			Desc:    "delete the named device",
			Handler: wrap(deleteDevice),
		},
		{
			Name:    "twin",
			Alias:   "t",
			Desc:    "inspect the named twin device",
			Handler: wrap(twin),
		},
		{
			Name:    "update-twin",
			Alias:   "ut",
			Help:    "DEVICE [[key value]...]",
			Desc:    "update the named twin device",
			Handler: wrap(updateTwin),
		},
		{
			Name:    "stats",
			Alias:   "st",
			Desc:    "get statistics about the devices",
			Handler: wrap(stats),
		},
		{
			Name:    "jobs",
			Alias:   "js",
			Desc:    "list the last import/export jobs",
			Handler: wrap(jobs),
		},
		{
			Name:    "job",
			Alias:   "j",
			Help:    "ID",
			Desc:    "get the status of a import/export job",
			Handler: wrap(job),
		},
		{
			Name:    "cancel-job",
			Alias:   "cj",
			Desc:    "cancel a import/export job",
			Handler: wrap(cancelJob),
		},
		{
			Name:    "connection-string",
			Alias:   "cs",
			Help:    "DEVICE",
			Desc:    "get a device's connection string",
			Handler: wrap(connectionString),
			ParseFunc: func(f *flag.FlagSet) {
				f.BoolVar(&secondaryFlag, "secondary", false, "use the secondary key instead")
			},
		},
		{
			Name:    "access-signature",
			Alias:   "sas",
			Help:    "DEVICE",
			Desc:    "generate a GenerateToken token",
			Handler: wrap(sas),
			ParseFunc: func(f *flag.FlagSet) {
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
		c, err := iotservice.New()
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
	return c.SubscribeEvents(ctx, func(msg *iotservice.Event) error {
		return internal.OutputJSON(msg, compressFlag)
	})
}

func watchEventHubEvents(ctx context.Context, cs, group string) error {
	c, err := eventhub.DialConnectionString(cs)
	if err != nil {
		return err
	}
	return c.Subscribe(ctx, func(m *eventhub.Event) error {
		return internal.OutputJSON(iotservice.FromAMQPMessage(m.Message), compressFlag)
	},
		eventhub.WithSubscribeConsumerGroup(group),
		eventhub.WithSubscribeSince(time.Now()),
	)
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
