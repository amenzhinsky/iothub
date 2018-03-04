package internal

import (
	"reflect"
	"testing"
)

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
