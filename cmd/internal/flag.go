package internal

import (
	"errors"
	"fmt"
	"strings"
)

type JSONMapFlag map[string]interface{}

func (f *JSONMapFlag) Set(s string) error {
	if len(*f) == 0 {
		*f = JSONMapFlag{}
	}
	c := strings.SplitN(s, "=", 2)
	if len(c) != 2 {
		return errors.New("malformed key-value flag")
	}
	(*f)[c[0]] = c[1]

	// TODO: add null, bool, numeric types
	return nil
}

func (f *JSONMapFlag) String() string {
	return fmt.Sprintf("%v", map[string]interface{}(*f))
}

type StringsMapFlag map[string]string

func (f *StringsMapFlag) Set(s string) error {
	if len(*f) == 0 {
		*f = StringsMapFlag{}
	}
	c := strings.SplitN(s, "=", 2)
	if len(c) != 2 {
		return errors.New("malformed key-value flag")
	}
	(*f)[c[0]] = c[1]
	return nil
}

func (f *StringsMapFlag) String() string {
	return fmt.Sprintf("%v", map[string]string(*f))
}
