package eventhub

import (
	"testing"
)

func TestParseConnectionString(t *testing.T) {
	have, err := ParseConnectionString(
		"Endpoint=sb://namespace.windows.net/;" +
			"SharedAccessKeyName=policy-name;" +
			"SharedAccessKey=abcNg==;" +
			"EntityPath=hub-name",
	)
	if err != nil {
		t.Fatal(err)
	}

	want := &Credentials{
		Endpoint:            "namespace.windows.net",
		SharedAccessKeyName: "policy-name",
		SharedAccessKey:     "abcNg==",
		EntityPath:          "hub-name",
	}
	if *want != *have {
		t.Fatalf("ParseConnectionString = %#v, want %#v", have, want)
	}
}
