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

// HandlerFunc is a subcommand handler.
type HandlerFunc func(context.Context, *flag.FlagSet) error

// Run runs one or the given commands based on argv.
// If ErrInvalidUsage is returned there's no need to print it, usage message is already sent to STDERR.
func Run(ctx context.Context, desc string, cmds []*Command, argv []string, fn func(*flag.FlagSet)) error {
	if len(argv) == 0 {
		panic("empty argv")
	}

	// sort subcommands alphabetically
	sort.Slice(cmds, func(i, j int) bool {
		return cmds[i].Name < cmds[j].Name
	})

	sm := flag.NewFlagSet(argv[0], flag.ContinueOnError)
	if fn != nil {
		fn(sm)
	}
	sm.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [FLAGS...] {COMMAND} [FLAGS...] [ARGS]...\n\n%s\n\ncommands:\n", argv[0], desc)
		for _, cmd := range cmds {
			fmt.Fprintf(os.Stderr, "  %-22s %s\n", cmd.Name+","+cmd.Alias, cmd.Desc)
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

	cmd := findCommand(cmds, sm.Arg(0))
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

func findCommand(cmds []*Command, k string) *Command {
	for _, cmd := range cmds {
		if cmd.Name == k || cmd.Alias == k {
			return cmd
		}
	}
	return nil
}

// sliceToMap converts sequence of arguments into a key-value map.
// [a, b, c, d] => {a: b, c: d} or errors when number of args is not even.
func ArgsToMap(s []string) (map[string]string, error) {
	m := map[string]string{}
	if len(s)%2 != 0 {
		return nil, errors.New("number of key-value arguments must be even")
	}
	for i := 0; i < len(s); i += 2 {
		m[s[i]] = s[i+1]
	}
	return m, nil
}

func OutputLine(format string) error {
	_, err := fmt.Println(format)
	return err
}

func OutputJSON(v interface{}) error {
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}
