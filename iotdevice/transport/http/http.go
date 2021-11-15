package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/amenzhinsky/iothub/common"
	"github.com/amenzhinsky/iothub/iotdevice/transport"
	"github.com/amenzhinsky/iothub/logger"
)

var (
	ErrNotImplemented  = errors.New("not implemented")
	DefaultSASLifetime = 30 * time.Second
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

type Transport struct {
	logger logger.Logger
	client *http.Client
	creds  transport.Credentials
}

// New returns new Transport transport.
func New(opts ...TransportOption) *Transport {
	tr := &Transport{
		client: http.DefaultClient,
	}
	for _, opt := range opts {
		opt(tr)
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

	resourceURI := fmt.Sprintf("%s/%s", tr.creds.GetHostName(), tr.creds.GetDeviceID())
	sas, err := tr.creds.Token(resourceURI, DefaultSASLifetime)
	if err != nil {
		return "", "", err
	}

	request, err := http.NewRequest(http.MethodPost, target.String(), bytes.NewReader(requestPayloadBytes))
	if err != nil {
		return "", "", err
	}
	request.Header.Add("Content-Type", "application/json; charset=utf-8")
	request.Header.Add("Authorization", sas.String())

	response, err := tr.client.Do(request)
	if err != nil {
		return "", "", err
	}

	if response.StatusCode != http.StatusOK {
		var responsePayload ErrorResponse
		err = json.NewDecoder(response.Body).Decode(&response)
		if err != nil {
			return "", "", err
		}

		return "", "", fmt.Errorf("code = %d, message = %s, exception message = %s", response.StatusCode, responsePayload.Message, responsePayload.ExceptionMessage)
	}

	var responsePayload BlobSharedAccessSignatureResponse
	err = json.NewDecoder(response.Body).Decode(&responsePayload)
	if err != nil {
		return "", "", err
	}

	return responsePayload.CorrelationID, responsePayload.SASURI(), nil
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
	sas, err := tr.creds.Token(resourceURI, DefaultSASLifetime)
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
