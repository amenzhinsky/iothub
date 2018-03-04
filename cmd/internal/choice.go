package internal

import (
	"flag"
	"fmt"
	"strings"
)

// NewChoiceFlag creates new flag.Value instance that's
// value is limited to the given options list.
// Default value is the first from the list.
//
// Panics when opts are blank.
func NewChoiceFlag(opts ...string) flag.Value {
	if len(opts) == 0 {
		panic("values are empty")
	}
	return &choiceFlag{opts: opts, curr: opts[0]}
}

type choiceFlag struct {
	opts []string
	curr string
}

func (f *choiceFlag) Set(s string) error {
	for _, o := range f.opts {
		if s == o {
			f.curr = s
			return nil
		}
	}
	return fmt.Errorf("valid values: %s", strings.Join(f.opts, ", "))
}

func (f *choiceFlag) String() string {
	return f.curr
}
