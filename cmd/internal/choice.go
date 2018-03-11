package internal

import (
	"flag"
	"fmt"
	"strings"
)

// NewChoiceFlag creates new flag.Value instance that's
// value must be one of in the given ones.
func NewChoiceFlag(current string, other ...string) flag.Value {
	return &choiceFlag{opts: append(other, current), curr: current}
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
