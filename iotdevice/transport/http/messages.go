package http

import "fmt"

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
	return fmt.Sprintf("https://%s/%s/%s%s", r.HostName, r.ContainerName, r.BlobName, r.SASToken)
}

type NotifyFileUploadRequest struct {
	CorrelationID     string `json:"correlationId`
	IsSuccess         bool   `json:"isSuccess"`
	StatusCode        int    `json:"statusCode"`
	StatusDescription string `json:"statusDescription"`
}
