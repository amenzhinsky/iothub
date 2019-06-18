package iotservice

import (
	"bytes"
	"net/http"
	"net/http/httputil"
)

type requestOutDump struct {
	req *http.Request
}

func (r *requestOutDump) String() string {
	b, err := httputil.DumpRequestOut(r.req, true)
	if err != nil {
		panic(err)
	}
	return prefix(b, "> ")
}

type responseDump struct {
	res *http.Response
}

func (r *responseDump) String() string {
	b, err := httputil.DumpResponse(r.res, true)
	if err != nil {
		panic(err)
	}
	return prefix(b, "< ")
}

func prefix(b []byte, prefix string) string {
	off := 0
	buf := bytes.NewBuffer(make([]byte, 0,
		len(b)+(bytes.Count(b, []byte{'\n'})*len(prefix)+len(prefix))),
	)
	buf.WriteString(prefix)
	for {
		i := bytes.Index(b[off:], []byte{'\n'})
		if i < 0 {
			buf.Write(b[off:])
			break
		}
		buf.Write(b[off : off+i+1])
		buf.WriteString(prefix)
		off += i + 1
	}
	return buf.String()
}
