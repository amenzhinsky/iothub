package internal

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
)

// ErrInvalidUsage when returned by a Handler the usage message is displayed.
var ErrInvalidUsage = errors.New("invalid usage")

// Command is a cli subcommand.
type Command struct {
	Help      string
	Desc      string
	Handler   HandlerFunc
	ParseFunc func(*flag.FlagSet)
}

// HandlerFunc is a subcommand handler.
type HandlerFunc func(context.Context, *flag.FlagSet) error

// Run runs one or the given commands based on argv.
// If ErrInvalidUsage is returned there's no need to print it, usage message is already sent to STDERR.
func Run(ctx context.Context, commands map[string]*Command, argv []string, fn func(*flag.FlagSet)) error {
	if len(argv) < 1 {
		panic("len(argv) < 1")
	}

	// sort subcommands alphabetically
	names := make([]string, 0, len(commands))
	for k := range commands {
		names = append(names, k)
	}
	sort.Strings(names)

	sm := flag.NewFlagSet(argv[0], flag.ContinueOnError)
	if fn != nil {
		fn(sm)
	}
	sm.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [FLAGS...] {COMMAND} [FLAGS...] [ARGS]...\n\ncommands:\n", argv[0])
		for _, name := range names {
			fmt.Fprintf(os.Stderr, "  %-15s %s\n", name, commands[name].Desc)
		}
		fmt.Println()
		fmt.Println("common flags: ")
		sm.PrintDefaults()
	}
	if err := sm.Parse(argv[1:]); err != nil {
		if err == flag.ErrHelp {
			return ErrInvalidUsage
		}
		return err
	}

	if sm.NArg() == 0 {
		sm.Usage()
		return ErrInvalidUsage
	}

	cmd := commands[sm.Arg(0)]
	if cmd == nil {
		sm.Usage()
		return ErrInvalidUsage
	}

	var args []string
	if sm.NArg() > 1 {
		args = sm.Args()[1:]
	}
	sc := flag.NewFlagSet(sm.Arg(0), flag.ContinueOnError)
	sc.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [FLAGS...] %s [FLAGS....] %s\n\nflags:\n",
			argv[0], sm.Arg(0), cmd.Help)
		sc.PrintDefaults()
		fmt.Println()
		fmt.Println("common flags: ")
		sm.PrintDefaults()
	}
	if cmd.ParseFunc != nil {
		cmd.ParseFunc(sc)
	}
	if err := sc.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return ErrInvalidUsage
		}
		return err
	}
	if err := cmd.Handler(ctx, sc); err != nil {
		if err == ErrInvalidUsage {
			sc.Usage()
		}
		return err
	}
	return nil
}

// NewChoiceFlag creates new flag.Value instance that's
// value is limited to the given options list.
// Default value is first in the list.
//
// Panics if opts are blank.
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
