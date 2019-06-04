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
	sasPrimaryFlag    string
	sasSecondaryFlag  string
	x509PrimaryFlag   string
	x509SecondaryFlag string
	caFlag            bool
	etagFlag          string
	statusFlag        string
	statusReasonFlag  string

	// send
	propsFlag map[string]string

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

	// twins
	tagsFlag      map[string]interface{}
	twinPropsFlag map[string]interface{}

	// configuration
	schemaVersionFlag  string
	priorityFlag       uint
	labelsFlag         map[string]string
	modulesContentFlag map[string]interface{}
	devicesContentFlag map[string]interface{}
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
	ctx := context.Background()
	return internal.New(help, func(f *flag.FlagSet) {
		f.BoolVar(&debugFlag, "debug", debugFlag, "enable debug mode")
		f.StringVar(&formatFlag, "format", "json-pretty", "data output format <json|json-pretty>")
	}, []*internal.Command{
		{
			Name:    "send",
			Args:    []string{"DEVICE", "PAYLOAD"},
			Desc:    "send a message to the named device (C2D)",
			Handler: wrap(ctx, send),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&ackFlag, "ack", "", "type of ack feedback")
				f.StringVar(&uidFlag, "uid", "golang-iothub", "origin of the message")
				f.StringVar(&midFlag, "mid", "", "identifier for the message")
				f.StringVar(&cidFlag, "cid", "", "message identifier in a request-reply")
				f.DurationVar(&expFlag, "exp", 0, "message lifetime")
				f.Var((*internal.StringsMapFlag)(&propsFlag), "prop", "custom property (key=value)")
			},
		},
		{
			Name:    "watch-events",
			Desc:    "subscribe to device messages (D2C)",
			Handler: wrap(ctx, watchEvents),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&ehcsFlag, "ehcs", "", "custom eventhub connection string")
				f.StringVar(&ehcgFlag, "ehcg", "$Default", "eventhub consumer group")
			},
		},
		{
			Name:    "watch-feedback",
			Desc:    "monitor message feedback send by devices",
			Handler: wrap(ctx, watchFeedback),
		},
		{
			Name:    "call",
			Args:    []string{"DEVICE", "METHOD", "PAYLOAD"},
			Desc:    "call a direct method on a device",
			Handler: wrap(ctx, call),
			ParseFunc: func(f *flag.FlagSet) {
				f.IntVar(&connectTimeoutFlag, "c", 0, "connect timeout in seconds")
				f.IntVar(&responseTimeoutFlag, "r", 30, "response timeout in seconds")
			},
		},
		{
			Name:    "device",
			Args:    []string{"DEVICE"},
			Desc:    "get device information",
			Handler: wrap(ctx, getDevice),
		},
		{
			Name:    "devices",
			Desc:    "list all available devices",
			Handler: wrap(ctx, listDevices),
		},
		{
			Name:    "create-device",
			Args:    []string{"DEVICE"},
			Desc:    "create a new device",
			Handler: wrap(ctx, createDevice),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&sasPrimaryFlag, "primary-key", "", "primary key (base64)")
				f.StringVar(&sasSecondaryFlag, "secondary-key", "", "secondary key (base64)")
				f.StringVar(&x509PrimaryFlag, "primary-thumbprint", "", "x509 primary thumbprint")
				f.StringVar(&x509SecondaryFlag, "secondary-thumbprint", "", "x509 secondary thumbprint")
				f.BoolVar(&caFlag, "ca", false, "use certificate authority authentication")
				f.StringVar(&etagFlag, "etag", "", "specify etag to ensure consistency")
				f.StringVar(&statusFlag, "status", "", "device status")
				f.StringVar(&statusReasonFlag, "status-reason", "", "disabled device status reason")
			},
		},
		{
			Name:    "update-device",
			Args:    []string{"DEVICE"},
			Desc:    "update the named device",
			Handler: wrap(ctx, updateDevice),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&sasPrimaryFlag, "sas-primary", "", "SAS primary key (base64)")
				f.StringVar(&sasSecondaryFlag, "sas-secondary-key", "", "SAS secondary key (base64)")
				f.StringVar(&x509PrimaryFlag, "x509-primary", "", "x509 primary thumbprint")
				f.StringVar(&x509SecondaryFlag, "x509-secondary", "", "x509 secondary thumbprint")
				f.BoolVar(&caFlag, "ca", false, "use certificate authority authentication")
				f.StringVar(&etagFlag, "etag", "", "specify etag to ensure consistency")
				f.StringVar(&statusFlag, "status", "", "device status")
				f.StringVar(&statusReasonFlag, "status-reason", "", "disabled device status reason")
			},
		},
		{
			Name:    "delete-device",
			Args:    []string{"DEVICE"},
			Desc:    "delete the named device",
			Handler: wrap(ctx, deleteDevice),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&etagFlag, "etag", "", "specify etag to ensure consistency")
			},
		},
		{
			Name:    "modules",
			Args:    []string{"DEVICE"},
			Desc:    "list the named device's modules",
			Handler: wrap(ctx, listModules),
		},
		{
			Name:    "create-module",
			Args:    []string{"DEVICE", "MODULE"},
			Desc:    "add the given module to the registry",
			Handler: wrap(ctx, createModule),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&sasPrimaryFlag, "sas-primary", "", "SAS primary key (base64)")
				f.StringVar(&sasSecondaryFlag, "sas-secondary-key", "", "SAS secondary key (base64)")
				f.StringVar(&x509PrimaryFlag, "x509-primary", "", "x509 primary thumbprint")
				f.StringVar(&x509SecondaryFlag, "x509-secondary", "", "x509 secondary thumbprint")
				f.BoolVar(&caFlag, "ca", false, "use certificate authority authentication")
			},
		},
		{
			Name:    "module",
			Args:    []string{"DEVICE", "MODULE"},
			Desc:    "get info on the named device",
			Handler: wrap(ctx, getModule),
		},
		{
			Name:    "update-module",
			Args:    []string{"DEVICE", "MODULE"},
			Desc:    "update the named module",
			Handler: wrap(ctx, updateModule),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&sasPrimaryFlag, "sas-primary", "", "SAS primary key (base64)")
				f.StringVar(&sasSecondaryFlag, "sas-secondary-key", "", "SAS secondary key (base64)")
				f.StringVar(&x509PrimaryFlag, "x509-primary", "", "x509 primary thumbprint")
				f.StringVar(&x509SecondaryFlag, "x509-secondary", "", "x509 secondary thumbprint")
				f.BoolVar(&caFlag, "ca", false, "use certificate authority authentication")
				f.StringVar(&etagFlag, "etag", "", "specify etag to ensure consistency")
			},
		},
		{
			Name:    "delete-module",
			Args:    []string{"DEVICE", "MODULE"},
			Desc:    "remove the named device from the registry",
			Handler: wrap(ctx, deleteModule),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&etagFlag, "etag", "", "specify etag to ensure consistency")
			},
		},
		{
			Name:    "twin",
			Args:    []string{"DEVICE"},
			Desc:    "inspect the named twin device",
			Handler: wrap(ctx, getTwin),
		},
		{
			Name:    "module-twin",
			Args:    []string{"DEVICE", "MODULE"},
			Desc:    "gets the named module twin",
			Handler: wrap(ctx, getModuleTwin),
		},
		{
			Name:    "update-twin",
			Args:    []string{"DEVICE"},
			Desc:    "update the named twin device",
			Handler: wrap(ctx, updateTwin),
			ParseFunc: func(f *flag.FlagSet) {
				f.Var((*internal.JSONMapFlag)(&twinPropsFlag), "prop", "property to update (key=value)")
				f.Var((*internal.JSONMapFlag)(&tagsFlag), "tag", "custom tag (key=value)")
			},
		},
		{
			Name:    "update-module-twin",
			Args:    []string{"DEVICE", "MODULE"},
			Desc:    "update the named module twin",
			Handler: wrap(ctx, updateModuleTwin),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&etagFlag, "etag", "", "specify etag to ensure consistency")
			},
		},
		{
			Name:    "configurations",
			Desc:    "list all configurations",
			Handler: wrap(ctx, listConfigurations),
		},
		{
			Name:    "create-configuration",
			Args:    []string{"CONFIGURATION"},
			Desc:    "add a configuration to the registry",
			Handler: wrap(ctx, createConfiguration),
			ParseFunc: func(f *flag.FlagSet) {
				f.UintVar(&priorityFlag, "priority", 10, "priority to resolve configuration conflicts")
				f.StringVar(&schemaVersionFlag, "schema-version", "1.0", "configuration schema version")
				f.Var((*internal.StringsMapFlag)(&labelsFlag), "label", "specific label (key=value)")
				f.Var((*internal.JSONMapFlag)(&devicesContentFlag), "device-prop", "device property (key=value)")
				f.Var((*internal.JSONMapFlag)(&modulesContentFlag), "module-prop", "module property (key=value)")
			},
		},
		{
			Name:    "configuration",
			Args:    []string{"CONFIGURATION"},
			Desc:    "retrieve the named configuration",
			Handler: wrap(ctx, getConfiguration),
		},
		{
			Name:    "update-configuration",
			Args:    []string{"CONFIGURATION"},
			Desc:    "update the named configuration",
			Handler: wrap(ctx, updateConfiguration),
			ParseFunc: func(f *flag.FlagSet) {
				f.UintVar(&priorityFlag, "priority", 0, "priority to resolve configuration conflicts")
				f.StringVar(&schemaVersionFlag, "schema-version", "", "configuration schema version")
				f.Var((*internal.StringsMapFlag)(&labelsFlag), "label", "specific labels in key=value format")
				f.Var((*internal.JSONMapFlag)(&devicesContentFlag), "device-prop", "device property (key=value)")
				f.Var((*internal.JSONMapFlag)(&modulesContentFlag), "module-prop", "module property (key=value)")
				f.StringVar(&etagFlag, "etag", "", "specify etag to ensure consistency")
			},
		},
		{
			Name:    "delete-configuration",
			Args:    []string{"CONFIGURATION"},
			Desc:    "delete the named configuration by id",
			Handler: wrap(ctx, deleteConfiguration),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&etagFlag, "etag", "", "specify etag to ensure consistency")
			},
		},
		{
			Name:    "apply-configuration",
			Args:    []string{"DEVICE"},
			Desc:    "applies configuration on the named device",
			Handler: wrap(ctx, applyConfiguration),
		},
		{
			Name:    "query",
			Args:    []string{"SQL"},
			Desc:    "execute sql query on devices",
			Handler: wrap(ctx, query),
			ParseFunc: func(f *flag.FlagSet) {
				f.UintVar(&pageSizeFlag, "page-size", 0, "number of records per request")
			},
		},
		{
			Name:    "stats",
			Desc:    "get statistics about the devices",
			Handler: wrap(ctx, stats),
		},
		{
			Name:    "jobs",
			Desc:    "list the last import/export jobs",
			Handler: wrap(ctx, listJobs),
		},
		{
			Name:    "job",
			Args:    []string{"JOB"},
			Desc:    "get the status of a import/export job",
			Handler: wrap(ctx, getJob),
		},
		{
			Name:    "cancel-job",
			Desc:    "cancel a import/export job",
			Handler: wrap(ctx, cancelJob),
		},
		{
			Name:    "connection-string",
			Args:    []string{"DEVICE"},
			Desc:    "get a device's connection string",
			Handler: wrap(ctx, connectionString),
			ParseFunc: func(f *flag.FlagSet) {
				f.BoolVar(&secondaryFlag, "secondary", false, "use the secondary key instead")
			},
		},
		{
			Name:    "access-signature",
			Args:    []string{"DEVICE"},
			Desc:    "generate a GenerateToken token",
			Handler: wrap(ctx, sas),
			ParseFunc: func(f *flag.FlagSet) {
				f.StringVar(&uriFlag, "uri", "", "storage resource uri")
				f.DurationVar(&durationFlag, "duration", time.Hour, "token validity time")
				f.BoolVar(&secondaryFlag, "secondary", false, "use the secondary key instead")
			},
		},
	}).Run(os.Args)
}

func wrap(
	ctx context.Context,
	fn func(context.Context, *iotservice.Client, []string) error,
) internal.HandlerFunc {
	return func(args []string) error {
		c, err := iotservice.New()
		if err != nil {
			return err
		}
		defer c.Close()
		return fn(ctx, c, args)
	}
}

func getDevice(ctx context.Context, c *iotservice.Client, args []string) error {
	return output(c.GetDevice(ctx, args[0]))
}

func listDevices(ctx context.Context, c *iotservice.Client, args []string) error {
	return output(c.ListDevices(ctx))
}

func createDevice(ctx context.Context, c *iotservice.Client, args []string) error {
	a, err := mkAuthentication()
	if err != nil {
		return err
	}
	return output(c.CreateDevice(ctx, &iotservice.Device{
		DeviceID:       args[0],
		Authentication: a,
		Status:         iotservice.DeviceStatus(statusFlag),
		StatusReason:   statusReasonFlag,
	}))
}

func updateDevice(ctx context.Context, c *iotservice.Client, args []string) error {
	a, err := mkAuthentication()
	if err != nil {
		return err
	}
	return output(c.UpdateDevice(ctx, &iotservice.Device{
		DeviceID:       args[0],
		Authentication: a,
		ETag:           etagFlag,
		Status:         iotservice.DeviceStatus(statusFlag),
		StatusReason:   statusReasonFlag,
	}))
}

func mkAuthentication() (*iotservice.Authentication, error) {
	switch {
	case sasPrimaryFlag != "" || sasSecondaryFlag != "":
		if x509PrimaryFlag != "" || x509SecondaryFlag != "" {
			return nil, errors.New("-x509-* options cannot be used along with sas authentication")
		} else if caFlag {
			return nil, errors.New("-ca option cannot be used along with sas authentication")
		}
		return &iotservice.Authentication{
			Type: iotservice.AuthSAS,
			SymmetricKey: &iotservice.SymmetricKey{
				PrimaryKey:   sasPrimaryFlag,
				SecondaryKey: sasSecondaryFlag,
			},
		}, nil
	case x509PrimaryFlag != "" || x509SecondaryFlag != "":
		if caFlag {
			return nil, errors.New("-ca option cannot be used along with x509 authentication")
		}
		return &iotservice.Authentication{
			Type: iotservice.AuthSelfSigned,
			X509Thumbprint: &iotservice.X509Thumbprint{
				PrimaryThumbprint:   x509PrimaryFlag,
				SecondaryThumbprint: x509SecondaryFlag,
			},
		}, nil
	case caFlag:
		return &iotservice.Authentication{
			Type: iotservice.AuthCA,
		}, nil
	default:
		return nil, nil
	}
}

func deleteDevice(ctx context.Context, c *iotservice.Client, args []string) error {
	return c.DeleteDevice(ctx, &iotservice.Device{
		DeviceID: args[0],
		ETag:     etagFlag,
	})
}

func listModules(ctx context.Context, c *iotservice.Client, args []string) error {
	return output(c.ListModules(ctx, args[0]))
}

func createModule(ctx context.Context, c *iotservice.Client, args []string) error {
	a, err := mkAuthentication()
	if err != nil {
		return err
	}
	return output(c.CreateModule(ctx, &iotservice.Module{
		DeviceID:       args[0],
		ModuleID:       args[1],
		Authentication: a,
	}))
}

func getModule(ctx context.Context, c *iotservice.Client, args []string) error {
	return output(c.GetModule(ctx, args[0], args[1]))
}

func deleteModule(ctx context.Context, c *iotservice.Client, args []string) error {
	return c.DeleteModule(ctx, &iotservice.Module{
		DeviceID: args[0],
		ModuleID: args[1],
		ETag:     etagFlag,
	})
}

func updateModule(ctx context.Context, c *iotservice.Client, args []string) error {
	a, err := mkAuthentication()
	if err != nil {
		return err
	}
	return output(c.UpdateModule(ctx, &iotservice.Module{
		DeviceID:       args[0],
		ModuleID:       args[1],
		ETag:           etagFlag,
		Authentication: a,

		// TODO: other fields
	}))
}

func listConfigurations(ctx context.Context, c *iotservice.Client, args []string) error {
	return output(c.ListConfigurations(ctx))
}

func createConfiguration(ctx context.Context, c *iotservice.Client, args []string) error {
	return output(c.CreateConfiguration(ctx, &iotservice.Configuration{
		ID:            args[0],
		SchemaVersion: schemaVersionFlag,
		Priority:      priorityFlag,
		Labels:        labelsFlag,
		Content: &iotservice.ConfigurationContent{
			ModulesContent: modulesContentFlag,
			DeviceContent:  devicesContentFlag,
		},
		// TODO: other fields
	}))
}

func getConfiguration(ctx context.Context, c *iotservice.Client, args []string) error {
	return output(c.GetConfiguration(ctx, args[0]))
}

func updateConfiguration(ctx context.Context, c *iotservice.Client, args []string) error {
	return output(c.UpdateConfiguration(ctx, &iotservice.Configuration{
		ID:            args[0],
		ETag:          etagFlag,
		SchemaVersion: schemaVersionFlag,
		Priority:      priorityFlag,
		Labels:        labelsFlag,
		Content: &iotservice.ConfigurationContent{
			ModulesContent: modulesContentFlag,
			DeviceContent:  devicesContentFlag,
		},
		// TODO: other fields
	}))
}

func deleteConfiguration(ctx context.Context, c *iotservice.Client, args []string) error {
	return c.DeleteConfiguration(ctx, &iotservice.Configuration{
		ID:   args[0],
		ETag: etagFlag,
	})
}

func applyConfiguration(ctx context.Context, c *iotservice.Client, args []string) error {
	return c.ApplyConfiguration(ctx, &iotservice.Configuration{
		// TODO
	}, args[0])
}

func query(ctx context.Context, c *iotservice.Client, args []string) error {
	return c.Query(ctx, &iotservice.Query{
		Query:    args[0],
		PageSize: pageSizeFlag,
	}, func(v map[string]interface{}) error {
		return output(v, nil)
	})
}

func stats(ctx context.Context, c *iotservice.Client, args []string) error {
	return output(c.Stats(ctx))
}

func getTwin(ctx context.Context, c *iotservice.Client, args []string) error {
	return output(c.GetTwin(ctx, args[0]))
}

func getModuleTwin(ctx context.Context, c *iotservice.Client, args []string) error {
	return output(c.GetModuleTwin(ctx, &iotservice.Module{
		DeviceID: args[0],
		ModuleID: args[1],
	}))
}

func updateTwin(ctx context.Context, c *iotservice.Client, args []string) error {
	var props *iotservice.Properties
	if len(twinPropsFlag) != 0 {
		props = &iotservice.Properties{
			Desired: twinPropsFlag,
		}
	}
	return output(c.UpdateTwin(ctx, &iotservice.Twin{
		DeviceID:   args[0],
		ETag:       etagFlag,
		Properties: props,
		Tags:       tagsFlag,
	}))
}

func updateModuleTwin(ctx context.Context, c *iotservice.Client, args []string) error {
	var props *iotservice.Properties
	if len(twinPropsFlag) != 0 {
		props = &iotservice.Properties{
			Desired: twinPropsFlag,
		}
	}
	return output(c.UpdateModuleTwin(ctx, &iotservice.ModuleTwin{
		DeviceID:   args[0],
		ETag:       etagFlag,
		Properties: props,
	}))
}

func call(ctx context.Context, c *iotservice.Client, args []string) error {
	var v map[string]interface{}
	if err := json.Unmarshal([]byte(args[2]), &v); err != nil {
		return err
	}
	return output(c.Call(ctx, args[0], args[1], v,
		iotservice.WithCallConnectTimeout(connectTimeoutFlag),
		iotservice.WithCallResponseTimeout(responseTimeoutFlag),
	))
}

func send(ctx context.Context, c *iotservice.Client, args []string) error {
	expiryTime := time.Time{}
	if expFlag != 0 {
		expiryTime = time.Now().Add(expFlag)
	}
	if err := c.SendEvent(ctx, args[0], []byte(args[1]),
		iotservice.WithSendMessageID(midFlag),
		iotservice.WithSendAck(ackFlag),
		iotservice.WithSendProperties(propsFlag),
		iotservice.WithSendUserID(uidFlag),
		iotservice.WithSendCorrelationID(cidFlag),
		iotservice.WithSentExpiryTime(expiryTime),
	); err != nil {
		return err
	}
	return nil
}

func watchEvents(ctx context.Context, c *iotservice.Client, args []string) error {
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

func watchFeedback(ctx context.Context, c *iotservice.Client, args []string) error {
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

func listJobs(ctx context.Context, c *iotservice.Client, args []string) error {
	return output(c.ListJobs(ctx))
}

func getJob(ctx context.Context, c *iotservice.Client, args []string) error {
	return output(c.GetJob(ctx, args[0]))
}

func cancelJob(ctx context.Context, c *iotservice.Client, args []string) error {
	return output(c.CancelJob(ctx, args[0]))
}

func connectionString(ctx context.Context, c *iotservice.Client, args []string) error {
	device, err := c.GetDevice(ctx, args[0])
	if err != nil {
		return err
	}
	cs, err := c.DeviceConnectionString(device, secondaryFlag)
	if err != nil {
		return err
	}
	return internal.OutputLine(cs)
}

func sas(ctx context.Context, c *iotservice.Client, args []string) error {
	device, err := c.GetDevice(ctx, args[0])
	if err != nil {
		return err
	}
	sas, err := c.DeviceSAS(device, durationFlag, secondaryFlag)
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
