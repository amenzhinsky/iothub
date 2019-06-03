package iotservice

// Result is a direct-method call result.
type Result struct {
	Status  int                    `json:"status,omitempty"`
	Payload map[string]interface{} `json:"payload,omitempty"`
}

type Device struct {
	DeviceID                   string                 `json:"deviceId,omitempty"`
	GenerationID               string                 `json:"generationId,omitempty"`
	ETag                       string                 `json:"etag,omitempty"`
	ConnectionState            string                 `json:"connectionState,omitempty"`
	Status                     string                 `json:"status,omitempty"`
	StatusReason               string                 `json:"statusReason,omitempty"`
	ConnectionStateUpdatedTime string                 `json:"connectionStateUpdatedTime,omitempty"`
	StatusUpdatedTime          string                 `json:"statusUpdatedTime,omitempty"`
	LastActivityTime           string                 `json:"lastActivityTime,omitempty"`
	CloudToDeviceMessageCount  int                    `json:"cloudToDeviceMessageCount,omitempty"`
	Authentication             *Authentication        `json:"authentication,omitempty"`
	Capabilities               map[string]interface{} `json:"capabilities,omitempty"`
}

type Module struct {
	ModuleID                   string          `json:"moduleId,omitempty"`
	DeviceID                   string          `json:"deviceId,omitempty"`
	GenerationID               string          `json:"generationId,omitempty"`
	ETag                       string          `json:"etag,omitempty"`
	ConnectionState            string          `json:"connectionState,omitempty"`
	ConnectionStateUpdatedTime string          `json:"connectionStateUpdatedTime,omitempty"`
	LastActivityTime           string          `json:"lastActivityTime,omitempty"`
	CloudToDeviceMessageCount  int             `json:"cloudToDeviceMessageCount,omitempty"`
	Authentication             *Authentication `json:"authentication,omitempty"`
	ManagedBy                  string          `json:"managedBy,omitempty"`
}

type Authentication struct {
	SymmetricKey   *SymmetricKey   `json:"symmetricKey,omitempty"`
	X509Thumbprint *X509Thumbprint `json:"x509Thumbprint,omitempty"`
	Type           AuthType        `json:"type,omitempty"`
}

// AuthType device authentication type.
type AuthType string

const (
	// AuthSAS uses symmetric keys to sign requests.
	AuthSAS = "sas"

	// AuthSelfSigned self signed certificate with a thumbprint.
	AuthSelfSigned = "selfSigned"

	// AuthCA certificate signed by a registered certificate authority.
	AuthCA = "certificateAuthority"
)

type X509Thumbprint struct {
	PrimaryThumbprint   string `json:"primaryThumbprint,omitempty"`
	SecondaryThumbprint string `json:"secondaryThumbprint,omitempty"`
}

type SymmetricKey struct {
	PrimaryKey   string `json:"primaryKey,omitempty"`
	SecondaryKey string `json:"secondaryKey,omitempty"`
}

type Twin struct {
	DeviceID                  string                 `json:"deviceId,omitempty"`
	ETag                      string                 `json:"etag,omitempty"`
	DeviceETag                string                 `json:"deviceEtag,omitempty"`
	Status                    string                 `json:"status,omitempty"`
	StatusReason              string                 `json:"statusReason,omitempty"`
	StatusUpdateTime          string                 `json:"statusUpdateTime,omitempty"`
	ConnectionState           string                 `json:"connectionState,omitempty"`
	LastActivityTime          string                 `json:"lastActivityTime,omitempty"`
	CloudToDeviceMessageCount int                    `json:"cloudToDeviceMessageCount,omitempty"`
	AuthenticationType        string                 `json:"authenticationType,omitempty"`
	X509Thumbprint            *X509Thumbprint        `json:"x509Thumbprint,omitempty"`
	Version                   int                    `json:"version,omitempty"`
	Tags                      map[string]interface{} `json:"tags,omitempty"`
	Properties                *Properties            `json:"properties,omitempty"`
	Capabilities              map[string]interface{} `json:"capabilities,omitempty"`
}

type ModuleTwin struct {
	DeviceID           string          `json:"deviceId,omitempty"`
	ModuleID           string          `json:"moduleId,omitempty"`
	ETag               string          `json:"etag,omitempty"`
	DeviceETag         string          `json:"deviceEtag,omitempty"`
	Status             string          `json:"status,omitempty"`
	StatusUpdateTime   string          `json:"statusUpdateTime,omitempty"`
	ConnectionState    string          `json:"connectionState,omitempty"`
	LastActivityTime   string          `json:"lastActivityTime,omitempty"`
	AuthenticationType string          `json:"authenticationType,omitempty"`
	X509Thumbprint     *X509Thumbprint `json:"x509Thumbprint,omitempty"`
	Version            int             `json:"version,omitempty"`
	Properties         *Properties     `json:"properties,omitempty"`
}

type Properties struct {
	Desired  map[string]interface{} `json:"desired,omitempty"`
	Reported map[string]interface{} `json:"reported,omitempty"`
}

type Stats struct {
	DisabledDeviceCount int `json:"disabledDeviceCount,omitempty"`
	EnabledDeviceCount  int `json:"enabledDeviceCount,omitempty"`
	TotalDeviceCount    int `json:"totalDeviceCount,omitempty"`
}

type Configuration struct {
	ID                 string                `json:"id,omitempty"`
	SchemaVersion      string                `json:"schemaVersion,omitempty"`
	Labels             map[string]string     `json:"labels,omitempty"`
	Content            *ConfigurationContent `json:"configuration,omitempty"`
	TargetCondition    string                `json:"targetCondition,omitempty"`
	CreatedTimeUTC     string                `json:"createdTimeUtc,omitempty"`
	LastUpdatedTimeUTC string                `json:"lastUpdatedTimeUtc,omitempty"`
	Priority           int                   `json:"priority,omitempty"`
	SystemMetrics      *ConfigurationMetrics `json:"systemMetrics,omitempty"`
	Metrics            *ConfigurationMetrics `json:"metrics,omitempty"`
	ETag               string                `json:"etag,omitempty"`
}

type ConfigurationContent struct {
	ModulesContent map[string]interface{} `json:"modulesContent,omitempty"`
	DeviceContent  map[string]interface{} `json:"deviceContent,omitempty"`
}

type ConfigurationMetrics struct {
	Results map[string]interface{} `json:"results,omitempty"`
	Queries map[string]interface{} `json:"queries,omitempty"`
}
