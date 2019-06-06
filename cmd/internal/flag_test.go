package internal

import (
	"reflect"
	"testing"
)

func TestKVFlag(t *testing.T) {
	kv := JSONMapFlag{}
	for _, s := range []string{
		`a="b"`,
		`b=null`,
		`c=15`,
		`d=`,
		`e=[1, 2, 3]`,
		`f={"a": "b"}`,
	} {
		if err := kv.Set(s); err != nil {
			t.Fatal(err)
		}
	}
	want := JSONMapFlag{
		"a": "b",
		"b": nil,
		"c": float64(15),
		"d": nil,
		"e": []interface{}{float64(1), float64(2), float64(3)},
		"f": map[string]interface{}{
			"a": "b",
		},
	}
	if !reflect.DeepEqual(kv, want) {
		t.Fatalf("\n\thave: %#v\n\twant: %#v", kv, want)
	}
}
