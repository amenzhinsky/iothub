package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/dangeroushobo/iothub/common"
	"github.com/dangeroushobo/iothub/iotdevice/transport"
	"github.com/dangeroushobo/iothub/iotservice"
	"github.com/dangeroushobo/iothub/logger"
)

const apiVersion = "2020-09-30"

var (
	ErrNotImplemented = errors.New("not implemented")
	DefaultSASTTL     = 30 * time.Second
)

// TransportOption is a transport configuration option.
type TransportOption func(tr *Transport)

// WithLogger sets logger for errors and warnings
// plus debug messages when it's enabled.
func WithLogger(l logger.Logger) TransportOption {
	return func(tr *Transport) {
		tr.logger = l
	}
}

// WithClient sets client to use for HTTP requests.
func WithClient(c *http.Client) TransportOption {
	return func(tr *Transport) {
		tr.client = c
	}
}

// WithTTL configures the TTL used for SAS tokens.
func WithTTL(ttl time.Duration) TransportOption {
	return func(tr *Transport) {
		tr.ttl = ttl
	}
}

// WithTLSConfig sets TLS config that's used by REST HTTP and AMQP clients.
func WithTLSConfig(config *tls.Config) TransportOption {
	return func(tr *Transport) {
		tr.tls = config
	}
}

type Transport struct {
	logger logger.Logger
	client *http.Client
	creds  transport.Credentials
	ttl    time.Duration
	tls    *tls.Config
}

// New returns new Transport transport.
func New(opts ...TransportOption) *Transport {
	tr := &Transport{
		ttl: DefaultSASTTL,
	}
	for _, opt := range opts {
		opt(tr)
	}
	if tr.tls == nil {
		tr.tls = &tls.Config{RootCAs: common.RootCAs()}
	}
	if tr.client == nil {
		tr.client = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs: common.RootCAs(),
				},
			},
		}
	}
	return tr
}

func (tr *Transport) SetLogger(logger logger.Logger) {
	tr.logger = logger
}

func (tr *Transport) Connect(ctx context.Context, creds transport.Credentials) error {
	tr.creds = creds
	return nil
}

// Send is not available in the HTTP transport.
func (tr *Transport) Send(ctx context.Context, msg *common.Message) error {
	return ErrNotImplemented
}

// RegisterDirectMethods is not available in the HTTP transport.
func (tr *Transport) RegisterDirectMethods(ctx context.Context, mux transport.MethodDispatcher) error {
	return ErrNotImplemented
}

// SubscribeEvents is not available in the HTTP transport.
func (tr *Transport) SubscribeEvents(ctx context.Context, mux transport.MessageDispatcher) error {
	return ErrNotImplemented
}

// SubscribeTwinUpdates is not available in the HTTP transport.
func (tr *Transport) SubscribeTwinUpdates(ctx context.Context, mux transport.TwinStateDispatcher) error {
	return ErrNotImplemented
}

// RetrieveTwinProperties is not available in the HTTP transport.
func (tr *Transport) RetrieveTwinProperties(ctx context.Context) (payload []byte, err error) {
	return nil, ErrNotImplemented
}

// UpdateTwinProperties is not available in the HTTP transport.
func (tr *Transport) UpdateTwinProperties(ctx context.Context, payload []byte) (version int, err error) {
	return 0, ErrNotImplemented
}

// ListModules list all the registered modules on the device.
func (tr *Transport) ListModules(ctx context.Context) ([]*iotservice.Module, error) {
	target, err := url.Parse(
		fmt.Sprintf(
			"https://%s/devices/%s/modules?api-version=%s",
			tr.creds.GetHostName(),
			url.PathEscape(tr.creds.GetDeviceID()),
			apiVersion,
		),
	)
	if err != nil {
		return nil, err
	}

	resp, err := tr.getTokenAndSendRequest(http.MethodGet, target, nil, nil)
	if err != nil {
		return nil, err
	}

	err = tr.handleErrorResponse(resp)
	if err != nil {
		return nil, err
	}

	var res []*iotservice.Module
	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// CreateModule Creates adds the given module to the registry.
func (tr *Transport) CreateModule(ctx context.Context, m *iotservice.Module) (*iotservice.Module, error) {
	target, err := url.Parse(
		fmt.Sprintf(
			"https://%s/devices/%s/modules/%s?api-version=%s",
			tr.creds.GetHostName(),
			url.PathEscape(tr.creds.GetDeviceID()), url.PathEscape(m.ModuleID),
			apiVersion,
		),
	)
	if err != nil {
		return nil, err
	}
	requestPayloadBytes, err := json.Marshal(&m)
	if err != nil {
		return nil, err
	}

	resp, err := tr.getTokenAndSendRequest(http.MethodPut, target, requestPayloadBytes, map[string]string{})
	if err != nil {
		return nil, err
	}

	err = tr.handleErrorResponse(resp)
	if err != nil {
		return nil, err
	}

	var res iotservice.Module
	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return nil, err
	}

	return &res, nil
}

// GetModule retrieves the named module.
func (tr *Transport) GetModule(ctx context.Context, moduleID string) (*iotservice.Module, error) {
	target, err := url.Parse(
		fmt.Sprintf(
			"https://%s/devices/%s/modules/%s?api-version=%s",
			tr.creds.GetHostName(),
			url.PathEscape(tr.creds.GetDeviceID()), url.PathEscape(moduleID),
			apiVersion,
		),
	)
	if err != nil {
		return nil, err
	}

	resp, err := tr.getTokenAndSendRequest(http.MethodGet, target, nil, nil)
	if err != nil {
		return nil, err
	}

	err = tr.handleErrorResponse(resp)
	if err != nil {
		return nil, err
	}

	var res iotservice.Module
	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return nil, err
	}

	return &res, nil
}

// UpdateModule updates the given module.
func (tr *Transport) UpdateModule(ctx context.Context, m *iotservice.Module) (*iotservice.Module, error) {
	target, err := url.Parse(
		fmt.Sprintf(
			"https://%s/devices/%s/modules/%s?api-version=%s",
			tr.creds.GetHostName(),
			url.PathEscape(m.DeviceID), url.PathEscape(m.ModuleID),
			apiVersion,
		),
	)
	if err != nil {
		return nil, err
	}
	requestPayloadBytes, err := json.Marshal(&m)
	if err != nil {
		return nil, err
	}

	resp, err := tr.getTokenAndSendRequest(http.MethodPut, target, requestPayloadBytes, ifMatchHeader(m.ETag))
	if err != nil {
		return nil, err
	}

	var res iotservice.Module
	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return nil, err
	}

	return &res, nil
}

// DeleteModule removes the named device module.
func (tr *Transport) DeleteModule(ctx context.Context, m *iotservice.Module) error {
	target, err := url.Parse(
		fmt.Sprintf(
			"https://%s/devices/%s/modules/%s?api-version=%s",
			tr.creds.GetHostName(),
			url.PathEscape(m.DeviceID), url.PathEscape(m.ModuleID),
			apiVersion,
		),
	)
	if err != nil {
		return err
	}

	_, err = tr.getTokenAndSendRequest(http.MethodDelete, target, nil, ifMatchHeader(m.ETag))
	return err
}

func ifMatchHeader(etag string) map[string]string {
	if etag == "" {
		etag = "*"
	} else {
		etag = `"` + etag + `"`
	}
	return map[string]string{"If-Match": etag}
}

func (tr *Transport) handleErrorResponse(resp *http.Response) error {
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		var responsePayload ErrorResponse
		err := json.NewDecoder(resp.Body).Decode(&resp)
		if err != nil {
			return err
		}

		return fmt.Errorf("code = %d, message = %s, exception message = %s", resp.StatusCode, responsePayload.Message, responsePayload.ExceptionMessage)
	}
	return nil
}

func (tr *Transport) GetBlobSharedAccessSignature(ctx context.Context, blobName string) (string, string, error) {
	requestPayload := BlobSharedAccessSignatureRequest{
		BlobName: blobName,
	}
	requestPayloadBytes, err := json.Marshal(&requestPayload)
	if err != nil {
		return "", "", err
	}

	target, err := url.Parse(fmt.Sprintf("https://%s/devices/%s/files?api-version=2020-03-13", tr.creds.GetHostName(), url.PathEscape(tr.creds.GetDeviceID())))
	if err != nil {
		return "", "", err
	}

	response, err := tr.getTokenAndSendRequest(http.MethodPost, target, requestPayloadBytes, map[string]string{})
	if err != nil {
		return "", "", err
	}

	err = tr.handleErrorResponse(response)
	if err != nil {
		return "", "", err
	}
	var responsePayload BlobSharedAccessSignatureResponse
	err = json.NewDecoder(response.Body).Decode(&responsePayload)
	if err != nil {
		return "", "", err
	}

	return responsePayload.CorrelationID, responsePayload.SASURI(), nil
}

func (tr *Transport) getTokenAndSendRequest(method string, target *url.URL, requestPayloadBytes []byte, headers map[string]string) (*http.Response, error) {
	resourceURI := fmt.Sprintf("%s/%s", tr.creds.GetHostName(), tr.creds.GetDeviceID())
	sas, err := tr.creds.Token(resourceURI, tr.ttl)
	if err != nil {
		return nil, err
	}

	request, err := http.NewRequest(method, target.String(), bytes.NewReader(requestPayloadBytes))
	if err != nil {
		return nil, err
	}

	for key, val := range headers {
		request.Header.Add(key, val)
	}
	request.Header.Add("Content-Type", "application/json; charset=utf-8")
	request.Header.Add("Authorization", sas.String())

	response, err := tr.client.Do(request)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (tr *Transport) UploadToBlob(ctx context.Context, sasURI string, file io.Reader, size int64) error {
	request, err := http.NewRequest(http.MethodPut, sasURI, file)
	if err != nil {
		return err
	}
	request.ContentLength = size
	request.Header.Add("x-ms-blob-type", "BlockBlob")

	response, err := tr.client.Do(request)
	if err != nil {
		return err
	}

	if response.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected status code: %d", response.StatusCode)
	}

	return nil
}

func (tr *Transport) NotifyUploadComplete(ctx context.Context, correlationID string, success bool, statusCode int, statusDescription string) error {
	requestPayload := NotifyUploadCompleteRequest{
		IsSuccess:         success,
		StatusCode:        statusCode,
		StatusDescription: statusDescription,
	}
	requestPayloadBytes, err := json.Marshal(&requestPayload)
	if err != nil {
		return err
	}

	target, err := url.Parse(fmt.Sprintf("https://%s/devices/%s/files/notifications/%s?api-version=2020-03-13", tr.creds.GetHostName(), url.PathEscape(tr.creds.GetDeviceID()), url.PathEscape(correlationID)))
	if err != nil {
		return err
	}

	resourceURI := fmt.Sprintf("%s/%s", tr.creds.GetHostName(), tr.creds.GetDeviceID())
	sas, err := tr.creds.Token(resourceURI, tr.ttl)
	if err != nil {
		return err
	}

	request, err := http.NewRequest(http.MethodPost, target.String(), bytes.NewReader(requestPayloadBytes))
	if err != nil {
		return err
	}
	request.Header.Add("Content-Type", "application/json; charset=utf-8")
	request.Header.Add("Authorization", sas.String())

	response, err := tr.client.Do(request)
	if err != nil {
		return err
	}

	if response.StatusCode != http.StatusNoContent {
		var responsePayload ErrorResponse
		err = json.NewDecoder(response.Body).Decode(&responsePayload)
		if err != nil {
			return err
		}

		return fmt.Errorf("code = %d, message = %s, exception message = %s", response.StatusCode, responsePayload.Message, responsePayload.ExceptionMessage)
	}

	return nil
}

func (tr *Transport) Close() error {
	// NOOP
	return nil
}
