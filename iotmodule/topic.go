package iotmodule

import (
	"fmt"
	"net/url"
)

type topicGenerator struct {
	DeviceID string
	ModuleID string
}

func (tg *topicGenerator) base() string {
	return fmt.Sprintf("devices/%s/modules/%s", tg.DeviceID, tg.ModuleID)
}

// New - returns a new topicGenerator
func New(DeviceID, ModuleID string) *topicGenerator {
	return &topicGenerator{
		DeviceID: DeviceID,
		ModuleID: ModuleID,
	}
}

// SubscribeInput - subscribes to messages routed to module input
func (tg *topicGenerator) SubscribeInput() string {
	return fmt.Sprintf("%s/inputs/#", tg.base())
}

// PublishTelemetry - sends telemetry to IoT Hub
func (tg *topicGenerator) PublishTelemetry() string {
	return fmt.Sprintf("%s/messages/events/", tg.base())
}

// SubscribeC2D - subscribe to cloud-to-device messages - won't work on edge
func (tg *topicGenerator) SubscribeC2D() string {
	return fmt.Sprintf("%s/messages/devicebound/#", tg.base())
}

// SubscribeMethod - subscribe to all incoming methods
func (tg *topicGenerator) SubscribeMethod() string {
	return "$iothub/methods/POST/#"
}

// PublishMethodResult - publishes result of method
func (tg *topicGenerator) PublishMethodResult(requestID, status string) string {
	rid := url.QueryEscape(requestID)
	s := url.QueryEscape(status)
	return fmt.Sprintf("$iothub/methods/res/%s/?$rid=%s", s, rid)
}

// SubscribeTwinResponse
func (tg *topicGenerator) SubscribeTwinResponse() string {
	return "$iothub/twin/res/#"
}

// SubscribeTwinPatch
func (tg *topicGenerator) SubscribeTwinPatch() string {
	return "$iothub/twin/PATCH/properties/desired/#"
}

// PublishTwin - publishes result of twin request
func (tg *topicGenerator) PublishTwin(method, resourceLocation, requestID string) string {
	return fmt.Sprintf("$iothub/twin/%s%s?$rid=%s", method, resourceLocation, requestID)
}
