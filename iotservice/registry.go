package iotservice

// Result is a direct-method call result.
type Result struct {
	Status  int                    `json:"status"`
	Payload map[string]interface{} `json:"payload"`
}

type Device struct {
	DeviceID                   string                 `json:"deviceId"`
	GenerationID               string                 `json:"generationId"`
	ETag                       string                 `json:"etag"`
	ConnectionState            string                 `json:"connectionState"`
	Status                     string                 `json:"status"`
	StatusReason               string                 `json:"statusReason"`
	ConnectionStateUpdatedTime string                 `json:"connectionStateUpdatedTime"`
	StatusUpdatedTime          string                 `json:"statusUpdatedTime"`
	LastActivityTime           string                 `json:"lastActivityTime"`
	CloudToDeviceMessageCount  int                    `json:"cloudToDeviceMessageCount"`
	Authentication             Authentication         `json:"authentication"`
	Capabilities               map[string]interface{} `json:"capabilities"`
}

type Authentication struct {
	SymmetricKey   SymmetricKey   `json:"symmetricKey"`
	X509Thumbprint X509Thumbprint `json:"x509Thumbprint"`
	Type           string         `json:"type"`
}

type X509Thumbprint struct {
	PrimaryThumbprint   string `json:"primaryThumbprint"`
	SecondaryThumbprint string `json:"secondaryThumbprint"`
}

type SymmetricKey struct {
	PrimaryKey   string `json:"primaryKey"`
	SecondaryKey string `json:"secondaryKey"`
}

type Twin struct {
	DeviceID                  string         `json:"deviceId"`
	ETag                      string         `json:"etag"`
	DeviceETag                string         `json:"deviceEtag"`
	Status                    string         `json:"status"`
	StatusReason              string         `json:"statusReason"`
	StatusUpdateTime          string         `json:"statusUpdateTime"`
	ConnectionState           string         `json:"connectionState"`
	LastActivityTime          string         `json:"lastActivityTime"`
	CloudToDeviceMessageCount int            `json:"cloudToDeviceMessageCount"`
	AuthenticationType        string         `json:"authenticationType"`
	X509Thumbprint            X509Thumbprint `json:"x509Thumbprint"`
	Version                   int            `json:"version"`
	// TODO: "tags": {
	//        "$etag": "123",
	//        "deploymentLocation": {
	//            "building": "43",
	//            "floor": "1"
	//        }
	//    },
	Properties   Properties             `json:"properties"`
	Capabilities map[string]interface{} `json:"capabilities"`
}

type Properties struct {
	Desired  map[string]interface{} `json:"desired,omitempty"`
	Reported map[string]interface{} `json:"reported,omitempty"`
}

type Stats struct {
	DisabledDeviceCount int `json:"disabledDeviceCount"`
	EnabledDeviceCount  int `json:"enabledDeviceCount"`
	TotalDeviceCount    int `json:"totalDeviceCount"`
}
