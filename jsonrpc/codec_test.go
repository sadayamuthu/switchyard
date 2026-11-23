package jsonrpc

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

func TestParseKinds(t *testing.T) {
	cases := []struct {
		name string
		line string
		want Kind
	}{
		{"request", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{}}`, KindRequest},
		{"notification", `{"jsonrpc":"2.0","method":"tools/call","params":{}}`, KindNotification},
		{"response", `{"jsonrpc":"2.0","id":1,"result":{}}`, KindResponse},
		{"error response", `{"jsonrpc":"2.0","id":1,"error":{"code":-1,"message":"x"}}`, KindResponse},
		{"batch", `[{"jsonrpc":"2.0","id":1,"method":"x"}]`, KindOther},
		{"invalid", `not json`, KindOther},
		{"scalar", `42`, KindOther},
		{"empty object", `{}`, KindOther},
		{"id only", `{"id":1}`, KindOther},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Parse([]byte(c.line)).Kind(); got != c.want {
				t.Fatalf("Kind = %v, want %v", got, c.want)
			}
		})
	}
}

func TestParseFields(t *testing.T) {
	m := Parse([]byte(`{"jsonrpc":"2.0","id":7,"method":"m","params":{"a":1}}`))
	if string(m.ID) != "7" || m.Method != "m" || string(m.Params) != `{"a":1}` {
		t.Fatalf("unexpected fields: %+v", m)
	}
	e := Parse([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"nope"}}`))
	if e.Error == nil || e.Error.Code != -32601 || e.Error.Message != "nope" {
		t.Fatalf("unexpected error: %+v", e.Error)
	}
}

func TestRawPreserved(t *testing.T) {
	line := `{"method":"x", "jsonrpc":"2.0","id":"a"}`
	r := NewReader(strings.NewReader(line + "\n"))
	m, err := r.Read()
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := NewWriter(&out).Write(m); err != nil {
		t.Fatal(err)
	}
	if out.String() != line+"\n" {
		t.Fatalf("got %q", out.String())
	}
}

func TestReaderCRLFEmptyLinesEOF(t *testing.T) {
	r := NewReader(strings.NewReader("\n{\"method\":\"a\"}\r\n\r\n{\"method\":\"b\"}"))
	for _, want := range []string{`{"method":"a"}`, `{"method":"b"}`} {
		m, err := r.Read()
		if err != nil {
			t.Fatal(err)
		}
		if string(m.Raw) != want {
			t.Fatalf("got %q want %q", m.Raw, want)
		}
	}
	if _, err := r.Read(); err != io.EOF {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestLargeMessage(t *testing.T) {
	big := strings.Repeat("x", 5<<20)
	line := `{"jsonrpc":"2.0","id":1,"result":"` + big + `"}`
	m, err := NewReader(strings.NewReader(line + "\n")).Read()
	if err != nil {
		t.Fatal(err)
	}
	if string(m.Raw) != line || m.Kind() != KindResponse {
		t.Fatal("large message not intact")
	}
}

func TestIDKeyDistinguishesTypes(t *testing.T) {
	a := Parse([]byte(`{"id":1,"result":{}}`))
	b := Parse([]byte(`{"id":"1","result":{}}`))
	if a.IDKey() == b.IDKey() {
		t.Fatalf("IDKey collision: %q", a.IDKey())
	}
}

func TestNewResponses(t *testing.T) {
	r := NewResponse(json.RawMessage(`3`), json.RawMessage(`{"ok":true}`))
	if !json.Valid(r.Raw) || r.Kind() != KindResponse || string(r.ID) != "3" || string(r.Result) != `{"ok":true}` {
		t.Fatalf("bad response: %s %+v", r.Raw, r)
	}
	if p := Parse(r.Raw); p.Kind() != KindResponse || string(p.ID) != "3" {
		t.Fatalf("raw does not reparse: %s", r.Raw)
	}
	if !strings.Contains(string(r.Raw), `"jsonrpc":"2.0"`) {
		t.Fatalf("missing jsonrpc: %s", r.Raw)
	}
	e := NewErrorResponse(json.RawMessage(`"a"`), &Error{Code: -32000, Message: "denied"})
	p := Parse(e.Raw)
	if e.Kind() != KindResponse || p.Kind() != KindResponse || p.Error == nil || p.Error.Message != "denied" || e.Error == nil {
		t.Fatalf("bad error response: %s", e.Raw)
	}
	if !strings.Contains(string(e.Raw), `"jsonrpc":"2.0"`) {
		t.Fatalf("missing jsonrpc: %s", e.Raw)
	}
}
