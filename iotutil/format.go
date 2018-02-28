package iotutil

import (
	"bytes"
	"fmt"
	"sort"
	"unicode"
)

// IsPrintable reports whether the given slice
// of bytes can be safely printed to console.
func IsPrintable(b []byte) bool {
	for _, r := range string(b) {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

// FormatPayload converts b into sequence of hex words if it's not printable.
func FormatPayload(b []byte) string {
	if IsPrintable(b) {
		return string(b)
	}
	return fmt.Sprintf("[% x]", string(b))
}

// FormatProperties formats the given map of properties to a one-line string.
func FormatPropertiesShort(m map[string]string) string {
	f := false
	b := bytes.Buffer{} // TODO: strings.Builder
	for k, v := range m {
		if f {
			b.WriteByte(' ')
		}
		f = true
		b.WriteString(k + ":" + FormatPayload([]byte(v)))
	}
	return b.String()
}

// FormatProperties formats the given map of properties to a per key line string.
func FormatProperties(m map[string]string) string {
	p := 0
	b := &bytes.Buffer{} // TODO: strings.Builder
	o := make([]string, 0, len(m))
	for k := range m {
		if p < len(k) {
			p = len(k)
		}
		o = append(o, k)
	}
	sort.Strings(o)
	for i, k := range o {
		if i != 0 {
			b.WriteByte('\n')
		}
		b.WriteString(fmt.Sprintf("%-"+fmt.Sprint(p)+"s : %s", k, FormatPayload([]byte(m[k]))))
	}
	return b.String()
}
