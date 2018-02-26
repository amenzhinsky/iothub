package mqtt

import (
	"fmt"
	"sync/atomic"
)

var ridcu uint32

// GenRID generates request IDs by incrementing every next result.
func GenRID() string {
	return fmt.Sprintf("%d", atomic.AddUint32(&ridcu, 1))
}
