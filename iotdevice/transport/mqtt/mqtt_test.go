package mqtt

import (
	"net/url"
	"reflect"
	"testing"
)

func TestParseCloudToDeviceTopic(t *testing.T) {
	s := "devices/mydev/messages/devicebound/%24.to=%2Fdevices%2Fmydev%2Fmessages%2FdeviceBound&a[]=b&b=c"
	g, err := parseCloudToDeviceTopic(s)
	if err != nil {
		t.Fatal(err)
	}

	w := map[string]string{
		"$.to": "/devices/mydev/messages/deviceBound",
		"a[]":  "b",
		"b":    "c",
	}
	if !reflect.DeepEqual(g, w) {
		t.Errorf("parseCloudToDeviceTopic(%q) = %v, _, want %v", s, g, w)
	}
}

func TestParseDirectMethodTopic(t *testing.T) {
	s := "$iothub/methods/POST/add/?$rid=666"
	m, r, err := parseDirectMethodTopic(s)
	if err != nil {
		t.Fatal(err)
	}

	if m != "add" || r != "666" {
		t.Errorf("parseDirectMethodTopic(%q) = %q, %q, want %q, %q", s, m, r, "add", 666)
	}
}

func TestParseTwinPropsTopic(t *testing.T) {
	s := "$iothub/twin/res/200/?$rid=12&$version=4"
	c, r, v, err := parseTwinPropsTopic(s)
	if err != nil {
		t.Fatal(err)
	}
	if c != 200 || r != 0x12 || v != 4 {
		t.Errorf("ParseTwinPropsTopic(%q) = %d, %d, %d, _, want %d, %d, %d, _",
			s, c, r, v, 200, 0x12, 4,
		)
	}
}

func TestEncodePropertiesHandleSpaces(t *testing.T) {
	cases := []struct {
		key      string
		value    string
		expected string
	}{
		{" ", " ", "%20=%20"},
		{"#", "#", "%23=%23"},
		{"+", "+", "%2B=%2B"},
	}

	for _, c := range cases {
		u := make(url.Values, 1)
		u.Add(c.key, c.value)

		enc := encodeProperties(u)

		if enc != c.expected {
			t.Errorf("encodeProperies(%v) = `%s`, want `%s`", u, enc, c.expected)
		}
	}
}
