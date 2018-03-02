package iotutil

import (
	"crypto/rand"
	"fmt"
	"io"
)

// UUID generates UUIDs based on RFC 4122.
//
// We're not using github.com/satori/go.uuid because it's drastically
// changed API over few last versions and we don't want to mess up with user deps.
func UUID() string {
	u := make([]byte, 16)
	n, err := io.ReadFull(rand.Reader, u)
	if err != nil || n != 16 {
		panic(err)
	}
	// variant bits, section 4.1.1
	u[8] = u[8]&^0xc0 | 0x80
	// version 4 (pseudo-random), section 4.1.3
	u[6] = u[6]&^0xf0 | 0x40
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:])
}
