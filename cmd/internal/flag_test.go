package internal

import (
	"reflect"
	"testing"
)

func TestKVFlag(t *testing.T) {
	kv := JSONMapFlag{}
	if err := kv.Set("a=b"); err != nil {
		t.Fatal(err)
	}
	want := JSONMapFlag{"a": "b"}
	if !reflect.DeepEqual(kv, want) {
		t.Fatalf("kv = %v, want %v", kv, want)
	}
}
