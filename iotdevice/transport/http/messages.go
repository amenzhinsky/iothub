package http

import (
	"fmt"
	"net/url"
)

type BlobSharedAccessSignatureRequest struct {
	BlobName string `json:"blobName"`
}

type BlobSharedAccessSignatureResponse struct {
	CorrelationID string `json:"correlationId"`
	HostName      string `json:"hostName"`
	ContainerName string `json:"containerName"`
	BlobName      string `json:"blobName"`
	SASToken      string `json:"sasToken"`
}

func (r *BlobSharedAccessSignatureResponse) SASURI() string {
	return fmt.Sprintf("https://%s/%s/%s%s", r.HostName, r.ContainerName, url.PathEscape(r.BlobName), r.SASToken)
}

type NotifyUploadCompleteRequest struct {
	IsSuccess         bool   `json:"isSuccess"`
	StatusCode        int    `json:"statusCode"`
	StatusDescription string `json:"statusDescription"`
}

type ErrorResponse struct {
	Message          string
	ExceptionMessage string
}
