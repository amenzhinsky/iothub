package internal

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
)

// ErrInvalidUsage when returned by a Handler the usage message is displayed.
var ErrInvalidUsage = errors.New("invalid usage")

// Command is a cli subcommand.
type Command struct {
	Name      string
	Alias     string
	Help      string
	Desc      string
	Handler   HandlerFunc
	ParseFunc func(*flag.FlagSet)
}

// HandlerFunc is a subcommand handler, fs is already parsed.
type HandlerFunc func(ctx context.Context, fs *flag.FlagSet) error

// FlagFunc prepares fs for parsing, setting flags.
type FlagFunc func(fs *flag.FlagSet)

// CLI is a cli subcommands executor.
type CLI struct {
	desc string
	cmds []*Command
	main FlagFunc
}

// New creates new cli executor.
func New(desc string, f FlagFunc, cmds []*Command) (*CLI, error) {
	r := &CLI{
		desc: desc,
		cmds: make([]*Command, len(cmds)),
		main: f,
	}
	copy(r.cmds, cmds)

	// sort subcommands alphabetically
	sort.Slice(r.cmds, func(i, j int) bool {
		return r.cmds[i].Name < r.cmds[j].Name
	})
	return r, nil
}

const (
	commonUsage  = "usage: %s [FLAGS...] {COMMAND} [FLAGS...] [ARGS]...\n\n%s\n\ncommands:\n"
	commandUsage = "usage: %s [FLAGS...] %s [FLAGS....] %s\n\nflags:\n"
)

// Run runs one or the given commands based on argv.
//
// Panics if argv is empty.
//
// If ErrInvalidUsage is returned there's no need to print it,
// the usage message is already sent to STDERR.
func (r *CLI) Run(ctx context.Context, argv ...string) error {
	if len(argv) == 0 {
		panic("empty argv")
	}

	sm := flag.NewFlagSet(argv[0], flag.ContinueOnError)
	if r.main != nil {
		r.main(sm)
	}
	sm.Usage = func() {
		fmt.Fprintf(os.Stderr, commonUsage, sm.Name(), r.desc)
		for _, cmd := range r.cmds {
			fmt.Fprintf(os.Stderr, "  %-22s %s\n", cmd.Name+","+cmd.Alias, cmd.Desc)
		}
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "common flags: ")
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

	cmd := r.findCommand(sm.Arg(0))
	if cmd == nil {
		sm.Usage()
		return ErrInvalidUsage
	}

	sc := flag.NewFlagSet(sm.Arg(0), flag.ContinueOnError)
	sc.Usage = func() {
		fmt.Fprintf(os.Stderr, commandUsage, sm.Name(), sm.Arg(0), cmd.Help)
		sc.PrintDefaults()
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "common flags: ")
		sm.PrintDefaults()
	}
	if cmd.ParseFunc != nil {
		cmd.ParseFunc(sc)
	}
	if err := sc.Parse(sm.Args()[1:]); err != nil {
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

func (r *CLI) findCommand(k string) *Command {
	for _, cmd := range r.cmds {
		if cmd.Name == k || cmd.Alias == k {
			return cmd
		}
	}
	return nil
}

// sliceToMap converts sequence of arguments into a key-value map.
// [a, b, c, d] => {a: b, c: d} or errors when number of args is not even.
func ArgsToMap(s []string) (map[string]string, error) {
	if len(s)%2 != 0 {
		return nil, errors.New("number of key-value arguments must be even")
	}
	m := make(map[string]string, len(s)%2)
	for i := 0; i < len(s); i += 2 {
		m[s[i]] = s[i+1]
	}
	return m, nil
}

// OutputLine prints the given string to stdout appending a new-line char.
func OutputLine(format string) error {
	_, err := fmt.Println(format)
	return err
}

// OutputJSON prints indented json to stdout.
func OutputJSON(v interface{}, compress bool) error {
	indent := "\t"
	if compress {
		indent = ""
	}
	b, err := json.MarshalIndent(v, "", indent)
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}
