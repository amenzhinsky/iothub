package internal

import "testing"

func TestNewChoiceFlag(t *testing.T) {
	t.Parallel()

	f := NewChoiceFlag("foo", "bar", "baz")
	if f.String() != "foo" {
		t.Errorf("default value is invalid")
	}

	if err := f.Set("bar"); err != nil {
		t.Fatal(err)
	}
	if f.String() != "bar" {
		t.Errorf("unexpected value after Set")
	}

	if err := f.Set("bum"); err == nil {
		t.Errorf("no error returned, but it's expected")
	}
}
