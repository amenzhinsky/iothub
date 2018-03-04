package iotutil

import (
	"fmt"
	"sync/atomic"
)

// NewRIDGenerator creates new rid generator.
func NewRIDGenerator() *RIDGenerator {
	return new(RIDGenerator)
}

// RIDGenerator generates unique request ids.
type RIDGenerator uint32

// Next returns a unique request id by incrementing numbers starting from 1.
func (r *RIDGenerator) Next() string {
	return fmt.Sprintf("%d", atomic.AddUint32((*uint32)(r), 1))
}
