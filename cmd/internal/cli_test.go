package internal

import (
	"context"
	"flag"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestRun(t *testing.T) {
	t.Parallel()

	commonFlag := ""
	commandFlag := ""

	cli, err := New("test desc",
		func(f *flag.FlagSet) {
			f.StringVar(&commonFlag, "c", "", "common flag")
		}, []*Command{
			{
				"test", "t",
				"test A B",
				"just a test",
				func(_ context.Context, f *flag.FlagSet) error {
					return OutputLine(strings.Join(f.Args(), ""))
				},
				func(fs *flag.FlagSet) {
					fs.StringVar(&commandFlag, "s", "", "command flag")
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	g, err := capture(func() error {
		return cli.Run(context.Background(), "run", "-c", "c", "test", "-s", "s", "a", "b", "c")
	})
	if err != nil {
		t.Fatal(err)
	}

	w := "abc\n"
	if string(g) != w {
		t.Errorf("output = %q, want %q", string(g), w)
	}
	if commonFlag != "c" {
		t.Errorf("commonFlag = %q, want %q", commonFlag, "c")
	}
	if commandFlag != "s" {
		t.Errorf("commandFlag = %q, want %q", commandFlag, "s")
	}
}

// capture stdout
func capture(fn func() error) ([]byte, error) {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		return nil, err
	}
	defer os.Remove(f.Name())

	tmp := os.Stdout
	os.Stdout = f
	if err := fn(); err != nil {
		return nil, err
	}
	os.Stdout = tmp

	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return ioutil.ReadAll(f)
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
