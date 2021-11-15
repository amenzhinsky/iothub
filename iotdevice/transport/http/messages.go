package http

import (
	"fmt"
	"net/url"
)

type CreateFileUploadRequest struct {
	BlobName string `json:"blobName"`
}

type CreateFileUploadResponse struct {
	CorrelationID string `json:"correlationId"`
	HostName      string `json:"hostName"`
	ContainerName string `json:"containerName"`
	BlobName      string `json:"blobName"`
	SASToken      string `json:"sasToken"`
}

func (r *CreateFileUploadResponse) SASURI() string {
	return fmt.Sprintf("https://%s/%s/%s%s", r.HostName, url.PathEscape(r.ContainerName), url.PathEscape(r.BlobName), r.SASToken)
}

type NotifyFileUploadRequest struct {
	IsSuccess         bool   `json:"isSuccess"`
	StatusCode        int    `json:"statusCode"`
	StatusDescription string `json:"statusDescription"`
}

type ErrorResponse struct {
	Message          string
	ExceptionMessage string
}
