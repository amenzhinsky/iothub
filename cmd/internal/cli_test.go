package internal

import (
	"context"
	"flag"
	"reflect"
	"testing"
)

func TestRun(t *testing.T) {
	t.Parallel()

	called := false
	commonFlag := ""
	subcommandFlag := ""

	if err := Run(
		context.Background(),
		"test desc",
		[]*Command{
			{
				"test", "t",
				"test A B",
				"just a test",
				func(_ context.Context, fs *flag.FlagSet) error {
					if fs.NArg() != 2 {
						t.Errorf("subcommand NArg = %d, want %d", fs.NArg(), 2)
					}
					called = true
					return nil
				},
				func(fs *flag.FlagSet) {
					fs.StringVar(&subcommandFlag, "s", "", "subcommand flag")
				},
			},
		},
		[]string{"run", "-c", "c", "test", "-s", "s", "a", "b"},
		func(fs *flag.FlagSet) {
			fs.StringVar(&commonFlag, "c", "", "common flag")
		},
	); err != nil {
		t.Fatal(err)
	}

	if !called {
		t.Error("subcomman is not called")
	}
	if commonFlag != "c" {
		t.Errorf("commonFlag = %q, want %q", commonFlag, "c")
	}
	if subcommandFlag != "s" {
		t.Errorf("subcommandFlag = %q, want %q", subcommandFlag, "s")
	}
}

func TestArgsToMap(t *testing.T) {
	t.Parallel()

	for _, s := range []struct {
		args []string
		want map[string]string
	}{
		{[]string{"a", "b", "c", "d"}, map[string]string{"a": "b", "c": "d"}},
		{[]string{}, map[string]string{}},
		{[]string{"a"}, nil}, // errors
	} {
		m, _ := ArgsToMap(s.args)
		if !reflect.DeepEqual(m, s.want) {
			t.Errorf("m, _ = ArgsToMap(%v); s = %v, want %v", s.args, m, s.want)
		}
	}
}
