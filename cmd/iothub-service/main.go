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
	"github.com/amenzhinsky/iothub/eventhub"
	"github.com/amenzhinsky/iothub/iotservice"
)

// globally accessible by command handlers, is it a good idea?
var (
	// common
	debugFlag  bool
	formatFlag string

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
	etagFlag                string

	// sas and connection string
	secondaryFlag bool

	// sas
	uriFlag      string
	durationFlag time.Duration

	// watch events
	ehcsFlag string
	ehcgFlag string

	// query
	pageSizeFlag uint
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
		f.StringVar(&formatFlag, "format", "json-pretty", "data output format <json|json-pretty>")
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
			Handler: wrap(getDevice),
		},
		{
			Name:    "devices",
			Alias:   "ds",
			Desc:    "list all available devices",
			Handler: wrap(listDevices),
		},
		{
			Name:    "create-device",
			Alias:   "cd",
			Help:    "DEVICE",
			Desc:    "create a new device",
			Handler: wrap(createDevice),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&primaryKeyFlag, "primary-key", "", "primary key (base64)")
				f.StringVar(&secondaryKeyFlag, "secondary-key", "", "secondary key (base64)")
				f.StringVar(&primaryThumbprintFlag, "primary-thumbprint", "", "x509 primary thumbprint")
				f.StringVar(&secondaryThumbprintFlag, "secondary-thumbprint", "", "x509 secondary thumbprint")
				f.BoolVar(&caFlag, "ca", false, "use certificate authority authentication")
				f.StringVar(&etagFlag, "etag", "", "specify etag to ensure consistency")
			},
		},
		{
			Name:    "update-device",
			Alias:   "ud",
			Help:    "DEVICE",
			Desc:    "update the named device",
			Handler: wrap(updateDevice),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&primaryKeyFlag, "primary-key", "", "primary key (base64)")
				f.StringVar(&secondaryKeyFlag, "secondary-key", "", "secondary key (base64)")
				f.StringVar(&primaryThumbprintFlag, "primary-thumbprint", "", "x509 primary thumbprint")
				f.StringVar(&secondaryThumbprintFlag, "secondary-thumbprint", "", "x509 secondary thumbprint")
				f.BoolVar(&caFlag, "ca", false, "use certificate authority authentication")
				f.StringVar(&etagFlag, "etag", "", "specify etag to ensure consistency")
			},
		},
		{
			Name:    "delete-device",
			Alias:   "dd",
			Help:    "DEVICE",
			Desc:    "delete the named device",
			Handler: wrap(deleteDevice),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&etagFlag, "etag", "", "specify etag to ensure consistency")
			},
		},
		{
			Name:    "modules",
			Alias:   "lm",
			Help:    "DEVICE",
			Desc:    "list the named device's modules",
			Handler: wrap(listModules),
		},
		{
			Name:    "create-module",
			Alias:   "cm",
			Help:    "DEVICE MODULE",
			Desc:    "add the given module to the registry",
			Handler: wrap(createModule),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&primaryKeyFlag, "primary-key", "", "primary key (base64)")
				f.StringVar(&secondaryKeyFlag, "secondary-key", "", "secondary key (base64)")
				f.StringVar(&primaryThumbprintFlag, "primary-thumbprint", "", "x509 primary thumbprint")
				f.StringVar(&secondaryThumbprintFlag, "secondary-thumbprint", "", "x509 secondary thumbprint")
				f.BoolVar(&caFlag, "ca", false, "use certificate authority authentication")
			},
		},
		{
			Name:    "module",
			Alias:   "gm",
			Help:    "DEVICE MODULE",
			Desc:    "get info on the named device",
			Handler: wrap(getModule),
		},
		{
			Name:    "update-module",
			Alias:   "um",
			Help:    "DEVICE MODULE",
			Desc:    "update the named module",
			Handler: wrap(updateModule),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&primaryKeyFlag, "primary-key", "", "primary key (base64)")
				f.StringVar(&secondaryKeyFlag, "secondary-key", "", "secondary key (base64)")
				f.StringVar(&primaryThumbprintFlag, "primary-thumbprint", "", "x509 primary thumbprint")
				f.StringVar(&secondaryThumbprintFlag, "secondary-thumbprint", "", "x509 secondary thumbprint")
				f.BoolVar(&caFlag, "ca", false, "use certificate authority authentication")
				f.StringVar(&etagFlag, "etag", "", "specify etag to ensure consistency")
			},
		},
		{
			Name:    "delete-module",
			Alias:   "dm",
			Help:    "DEVICE MODULE",
			Desc:    "remove the named device from the registry",
			Handler: wrap(deleteModule),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&etagFlag, "etag", "", "specify etag to ensure consistency")
			},
		},
		{
			Name:    "twin",
			Alias:   "t",
			Desc:    "inspect the named twin device",
			Handler: wrap(getTwin),
		},
		{
			Name:    "module-twin",
			Alias:   "mt",
			Help:    "DEVICE MODULE",
			Desc:    "gets the named module twin",
			Handler: wrap(getModuleTwin),
		},
		{
			Name:    "update-twin",
			Alias:   "ut",
			Help:    "DEVICE [[key value]...]",
			Desc:    "update the named twin device",
			Handler: wrap(updateTwin),
		},
		{
			Name:    "update-module-twin",
			Alias:   "",
			Help:    "DEVICE MODULE [[key value]...]",
			Desc:    "update the named module twin",
			Handler: wrap(updateModuleTwin),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&etagFlag, "etag", "", "specify etag to ensure consistency")
			},
		},
		{
			Name:    "configurations",
			Alias:   "lc",
			Desc:    "list all configurations",
			Handler: wrap(listConfigurations),
		},
		{
			Name:    "create-configuration",
			Alias:   "",
			Help:    "", // TODO
			Desc:    "add a configuration to the registry",
			Handler: wrap(createConfiguration),
		},
		{
			Name:    "configuration",
			Alias:   "",
			Help:    "CONFIGURATION_ID",
			Desc:    "retrieve the named configuration",
			Handler: wrap(getConfiguration),
		},
		{
			Name:    "update-configuration",
			Alias:   "",
			Help:    "",
			Desc:    "update the named configuration",
			Handler: wrap(updateConfiguration),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&etagFlag, "etag", "", "specify etag to ensure consistency")
			},
		},
		{
			Name:    "delete-configuration",
			Alias:   "",
			Help:    "CONFIGURATION_ID",
			Desc:    "delete the named configuration by id",
			Handler: wrap(deleteConfiguration),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&etagFlag, "etag", "", "specify etag to ensure consistency")
			},
		},
		{
			Name:    "apply-configuration",
			Alias:   "",
			Help:    "DEVICE_ID",
			Desc:    "",
			Handler: wrap(applyConfiguration),
		},
		{
			Name:    "query",
			Alias:   "q",
			Help:    "SQL",
			Desc:    "execute sql query on devices",
			Handler: wrap(query),
			ParseFunc: func(f *flag.FlagSet) {
				f.UintVar(&pageSizeFlag, "page-size", 0, "number of records per request")
			},
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

func getDevice(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	return output(c.GetDevice(ctx, f.Arg(0)))
}

func listDevices(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 0 {
		return internal.ErrInvalidUsage
	}
	return output(c.ListDevices(ctx))
}

func createDevice(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	a, err := mkAuthentication()
	if err != nil {
		return err
	}
	return output(c.CreateDevice(ctx, &iotservice.Device{
		DeviceID:       f.Arg(0),
		Authentication: a,
	}))
}

func updateDevice(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	a, err := mkAuthentication()
	if err != nil {
		return err
	}
	return output(c.UpdateDevice(ctx, &iotservice.Device{
		DeviceID:       f.Arg(0),
		Authentication: a,
		ETag:           etagFlag,

		// TODO: other fields
	}))
}

func mkAuthentication() (*iotservice.Authentication, error) {
	if primaryThumbprintFlag != "" || secondaryThumbprintFlag != "" {
		if caFlag {
			return nil, errors.New("-ca flag cannot be used with x509 flags")
		}
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
	return c.DeleteDevice(ctx, &iotservice.Device{
		DeviceID: f.Arg(0),
		ETag:     etagFlag,
	})
}

func listModules(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	return output(c.ListModules(ctx, f.Arg(0)))
}

func createModule(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 2 {
		return internal.ErrInvalidUsage
	}
	a, err := mkAuthentication()
	if err != nil {
		return err
	}
	return output(c.CreateModule(ctx, &iotservice.Module{
		DeviceID:       f.Arg(0),
		ModuleID:       f.Arg(1),
		Authentication: a,
	}))
}

func getModule(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 2 {
		return internal.ErrInvalidUsage
	}
	return output(c.GetModule(ctx, f.Arg(0), f.Arg(1)))
}

func deleteModule(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 2 {
		return internal.ErrInvalidUsage
	}
	return c.DeleteModule(ctx, &iotservice.Module{
		DeviceID: f.Arg(0),
		ModuleID: f.Arg(1),
		ETag:     etagFlag,
	})
}

func updateModule(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 2 {
		return internal.ErrInvalidUsage
	}
	a, err := mkAuthentication()
	if err != nil {
		return err
	}
	return output(c.UpdateModule(ctx, &iotservice.Module{
		DeviceID:       f.Arg(0),
		ModuleID:       f.Arg(1),
		ETag:           etagFlag,
		Authentication: a,

		// TODO: other fields
	}))
}

func listConfigurations(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 0 {
		return internal.ErrInvalidUsage
	}
	return output(c.ListConfigurations(ctx))
}

func createConfiguration(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	return output(c.CreateConfiguration(ctx, &iotservice.Configuration{
		ID:            f.Arg(0),
		SchemaVersion: "1.0", // TODO
		Priority:      10,    // TODO
	}))
}

func getConfiguration(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	return output(c.GetConfiguration(ctx, f.Arg(0)))
}

func updateConfiguration(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	return output(c.UpdateConfiguration(ctx, &iotservice.Configuration{
		ETag: etagFlag,
		// TODO
	}))
}

func deleteConfiguration(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	return c.DeleteConfiguration(ctx, &iotservice.Configuration{
		ID:   f.Arg(0),
		ETag: etagFlag,
	})
}

func applyConfiguration(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	return c.ApplyConfiguration(ctx, &iotservice.Configuration{
		// TODO
	}, f.Arg(0))
}

func query(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	return c.Query(ctx, &iotservice.Query{
		Query:    f.Arg(0),
		PageSize: pageSizeFlag,
	}, func(v map[string]interface{}) error {
		return output(v, nil)
	})
}

func stats(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 0 {
		return internal.ErrInvalidUsage
	}
	return output(c.Stats(ctx))
}

func getTwin(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	return output(c.GetTwin(ctx, f.Arg(0)))
}

func getModuleTwin(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 2 {
		return internal.ErrInvalidUsage
	}
	return output(c.GetModuleTwin(ctx, &iotservice.Module{
		DeviceID: f.Arg(0),
		ModuleID: f.Arg(1),
	}))
}

func updateTwin(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() < 3 {
		return internal.ErrInvalidUsage
	}
	p, err := mkProperties(f.Args()[1:])
	if err != nil {
		return err
	}
	return output(c.UpdateTwin(ctx, &iotservice.Twin{
		DeviceID:   f.Arg(0),
		ETag:       etagFlag,
		Properties: p,
	}))
}

func updateModuleTwin(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() < 4 {
		return internal.ErrInvalidUsage
	}
	p, err := mkProperties(f.Args()[2:])
	if err != nil {
		return err
	}
	return output(c.UpdateModuleTwin(ctx, &iotservice.ModuleTwin{
		DeviceID:   f.Arg(0),
		ETag:       etagFlag,
		Properties: p,
	}))
}

func mkProperties(argv []string) (*iotservice.Properties, error) {
	m, err := internal.ArgsToMap(argv)
	if err != nil {
		return nil, err
	}
	p := &iotservice.Properties{
		Desired: make(map[string]interface{}, len(m)),
	}
	for k, v := range m {
		if v == "null" {
			p.Desired[k] = nil
		} else {
			p.Desired[k] = v
		}
	}
	return p, nil
}

func call(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 3 {
		return internal.ErrInvalidUsage
	}
	var v map[string]interface{}
	if err := json.Unmarshal([]byte(f.Arg(2)), &v); err != nil {
		return err
	}
	return output(c.Call(ctx, f.Arg(0), f.Arg(1), v,
		iotservice.WithCallConnectTimeout(connectTimeoutFlag),
		iotservice.WithCallResponseTimeout(responseTimeoutFlag),
	))
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
		return output(msg, nil)
	})
}

func watchEventHubEvents(ctx context.Context, cs, group string) error {
	c, err := eventhub.DialConnectionString(cs)
	if err != nil {
		return err
	}
	return c.Subscribe(ctx, func(m *eventhub.Event) error {
		return output(iotservice.FromAMQPMessage(m.Message), nil)
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
		if err := output(f, nil); err != nil {
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
	return output(c.ListJobs(ctx))
}

func job(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	return output(c.GetJob(ctx, f.Arg(0)))
}

func cancelJob(ctx context.Context, f *flag.FlagSet, c *iotservice.Client) error {
	if f.NArg() != 1 {
		return internal.ErrInvalidUsage
	}
	return output(c.CancelJob(ctx, f.Arg(0)))
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

func output(v interface{}, err error) error {
	if err != nil {
		return err
	}
	return internal.Output(v, formatFlag)
}
