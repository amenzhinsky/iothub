package iotdevice

import (
	"context"

	"github.com/dangeroushobo/iothub/common"
	"github.com/dangeroushobo/iothub/iotdevice/transport"
	"github.com/dangeroushobo/iothub/logger"
)

// structs

// ModuleClient is iothub device client adapted for use with a module connection
type ModuleClient struct {
	Client
}

// functions

// NewModuleFromConnectionString returns a ModuleClient struct with credentials based off of a supplied connection string
func NewModuleFromConnectionString(
	transport transport.Transport,
	cs, gatewayHostName, moduleGenerationID, workloadURI string,
	edge bool,
	opts ...ClientOption,
) (*ModuleClient, error) {
	creds, err := ParseModuleConnectionString(cs)
	if err != nil {
		return nil, err
	}

	creds.EdgeGateway = edge
	creds.WorkloadURI = workloadURI
	creds.Gateway = gatewayHostName
	creds.GenerationID = moduleGenerationID

	return NewModule(transport, creds, opts...)
}

func NewModuleFromEnvironment(
	transport transport.Transport,
	edge bool,
	opts ...ClientOption,
) (*ModuleClient, error) {
	creds, err := ParseModuleEnvironmentVariables()
	if err != nil {
		return nil, err
	}
	creds.EdgeGateway = edge
	return NewModule(transport, creds, opts...)
}

func ParseModuleEnvironmentVariables() (*ModuleSharedAccessKeyCredentials, error) {
	m, err := common.GetEdgeModuleEnvironmentVariables()
	if err != nil {
		return nil, err
	}
	return &ModuleSharedAccessKeyCredentials{
		SharedAccessKeyCredentials: SharedAccessKeyCredentials{
			DeviceID: m["DeviceID"],
			SharedAccessKey: common.SharedAccessKey{
				HostName: m["IOTHubHostName"],
			},
		},
		ModuleID:     m["ModuleID"],
		WorkloadURI:  m["WorkloadAPI"],
		GenerationID: m["GenerationID"],
		Gateway:      m["GatewayHostName"],
	}, nil
}

// ParseModuleConnectionString returns a ModuleSharedAccessKeyCredentials struct with some properties derived from a supplied connection string
func ParseModuleConnectionString(cs string) (*ModuleSharedAccessKeyCredentials, error) {
	m, err := common.ParseConnectionString(cs, "DeviceId", "ModuleId")
	if err != nil {
		return nil, err
	}
	return &ModuleSharedAccessKeyCredentials{
		SharedAccessKeyCredentials: SharedAccessKeyCredentials{
			DeviceID: m["DeviceId"],
			SharedAccessKey: common.SharedAccessKey{
				HostName:            m["HostName"],
				SharedAccessKeyName: m["SharedAccessKeyName"],
				SharedAccessKey:     m["SharedAccessKey"],
			},
		},
		ModuleID: m["ModuleId"],
	}, nil
}

// NewModule returns a new ModuleClient struct
func NewModule(
	transport transport.Transport, creds transport.Credentials, opts ...ClientOption,
) (*ModuleClient, error) {
	c := &ModuleClient{
		Client: Client{
			tr:    transport,
			creds: creds,

			ready:  make(chan struct{}),
			done:   make(chan struct{}),
			logger: logger.New(logger.LevelWarn, nil),

			evMux: newEventsMux(),
			tsMux: newTwinStateMux(),
			dmMux: newMethodMux(),
		},
	}

	for _, opt := range opts {
		opt(&c.Client)
	}

	// transport uses the same logger as the client
	c.tr.SetLogger(c.logger)
	return c, nil
}

// methods

// ModuleID returns module ID property from client's credential property
func (c *ModuleClient) ModuleID() string {
	return c.creds.GetModuleID()
}

// GenerationID returns generation ID property from client's credential property
func (c *ModuleClient) GenerationID() string {
	return c.creds.GetGenerationID()
}

// Gateway returns gateway hostname property from client's credential property
func (c *ModuleClient) Gateway() string {
	return c.creds.GetGateway()
}

// Broker returns broker property from client's credential property
func (c *ModuleClient) Broker() string {
	return c.creds.GetBroker()
}

// SubscribeTwinUpdates subscribes to module desired state changes.
// It returns a channel to read the twin updates from.
func (c *ModuleClient) SubscribeTwinUpdates(ctx context.Context) (*TwinStateSub, error) {
	if err := c.checkConnection(ctx); err != nil {
		return nil, err
	}
	if err := c.tsMux.once(func() error {
		return c.tr.SubscribeTwinUpdates(ctx, c.tsMux)
	}); err != nil {
		return nil, err
	}
	return c.tsMux.sub(), nil
}

// UnsubscribeTwinUpdates unsubscribes the given handler from twin state updates.
func (c *ModuleClient) UnsubscribeTwinUpdates(sub *TwinStateSub) {
	c.tsMux.unsub(sub)
}
