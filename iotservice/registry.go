package iotservice

import (
	"errors"
	"time"
)

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
	ConnectionStateUpdatedTime MicrosoftTime          `json:"connectionStateUpdatedTime,omitempty"`
	StatusUpdatedTime          MicrosoftTime          `json:"statusUpdatedTime,omitempty"`
	LastActivityTime           MicrosoftTime          `json:"lastActivityTime,omitempty"`
	CloudToDeviceMessageCount  uint                   `json:"cloudToDeviceMessageCount,omitempty"`
	Authentication             *Authentication        `json:"authentication,omitempty"`
	Capabilities               map[string]interface{} `json:"capabilities,omitempty"`
}

type Module struct {
	ModuleID                   string          `json:"moduleId,omitempty"`
	DeviceID                   string          `json:"deviceId,omitempty"`
	GenerationID               string          `json:"generationId,omitempty"`
	ETag                       string          `json:"etag,omitempty"`
	ConnectionState            string          `json:"connectionState,omitempty"`
	ConnectionStateUpdatedTime MicrosoftTime   `json:"connectionStateUpdatedTime,omitempty"`
	LastActivityTime           MicrosoftTime   `json:"lastActivityTime,omitempty"`
	CloudToDeviceMessageCount  uint            `json:"cloudToDeviceMessageCount,omitempty"`
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
	StatusUpdateTime          MicrosoftTime          `json:"statusUpdateTime,omitempty"`
	ConnectionState           string                 `json:"connectionState,omitempty"`
	LastActivityTime          MicrosoftTime          `json:"lastActivityTime,omitempty"`
	CloudToDeviceMessageCount uint                   `json:"cloudToDeviceMessageCount,omitempty"`
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
	StatusUpdateTime   MicrosoftTime   `json:"statusUpdateTime,omitempty"`
	ConnectionState    string          `json:"connectionState,omitempty"`
	LastActivityTime   MicrosoftTime   `json:"lastActivityTime,omitempty"`
	AuthenticationType string          `json:"authenticationType,omitempty"`
	X509Thumbprint     *X509Thumbprint `json:"x509Thumbprint,omitempty"`
	Version            uint            `json:"version,omitempty"`
	Properties         *Properties     `json:"properties,omitempty"`
}

type Properties struct {
	Desired  map[string]interface{} `json:"desired,omitempty"`
	Reported map[string]interface{} `json:"reported,omitempty"`
}

type Stats struct {
	DisabledDeviceCount uint `json:"disabledDeviceCount,omitempty"`
	EnabledDeviceCount  uint `json:"enabledDeviceCount,omitempty"`
	TotalDeviceCount    uint `json:"totalDeviceCount,omitempty"`
}

type Configuration struct {
	ID                 string                `json:"id,omitempty"`
	SchemaVersion      string                `json:"schemaVersion,omitempty"`
	Labels             map[string]string     `json:"labels,omitempty"`
	Content            *ConfigurationContent `json:"content,omitempty"`
	TargetCondition    string                `json:"targetCondition,omitempty"`
	CreatedTimeUTC     time.Time             `json:"createdTimeUtc,omitempty"`
	LastUpdatedTimeUTC time.Time             `json:"lastUpdatedTimeUtc,omitempty"`
	Priority           uint                  `json:"priority,omitempty"`
	SystemMetrics      *ConfigurationMetrics `json:"systemMetrics,omitempty"`
	Metrics            *ConfigurationMetrics `json:"metrics,omitempty"`
	ETag               string                `json:"etag,omitempty"`
}

type ConfigurationContent struct {
	ModulesContent map[string]map[string]interface{} `json:"modulesContent,omitempty"`
	DeviceContent  map[string]map[string]interface{} `json:"deviceContent,omitempty"`
}

type ConfigurationMetrics struct {
	Results map[string]uint   `json:"results,omitempty"`
	Queries map[string]string `json:"queries,omitempty"`
}

type Query struct {
	Query    string `json:"query,omitempty"`
	PageSize uint   `json:"-"`
}

type MicrosoftTime struct {
	time.Time
}

func (t *MicrosoftTime) UnmarshalJSON(b []byte) error {
	if len(b) < 2 {
		return errors.New("malformed time")
	}
	n, err := time.Parse("2006-01-02T15:04:05", string(b[1:len(b)-1]))
	if err != nil {
		return err
	}
	t.Time = n
	return nil
}
