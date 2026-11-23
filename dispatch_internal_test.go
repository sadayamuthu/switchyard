package switchyard

import (
	"encoding/json"
	"testing"

	"github.com/sadayamuthu/switchyard/jsonrpc"
)

func TestWithParamsSplices(t *testing.T) {
	for _, tc := range []struct {
		name, in, params, want string
	}{
		{"replace", `{"jsonrpc":"2.0","id":"<&>", "method":"m","params": {"a":1} ,"x":"<"}`, `{"b":2}`,
			`{"jsonrpc":"2.0","id":"<&>", "method":"m","params": {"b":2} ,"x":"<"}`},
		{"replace first", `{ "params":[1],"id":1,"method":"m"}`, `[2]`, `{ "params":[2],"id":1,"method":"m"}`},
		{"remove middle", `{"jsonrpc":"2.0","id":1, "params":{} ,"method":"m"}`, ``, `{"jsonrpc":"2.0","id":1 ,"method":"m"}`},
		{"remove first", `{"params":{}, "id":1,"method":"m"}`, ``, `{ "id":1,"method":"m"}`},
		{"remove last", `{"id":1,"method":"m","params":{}}`, ``, `{"id":1,"method":"m"}`},
		{"remove absent", `{"id":1,"method":"m"}`, ``, `{"id":1,"method":"m"}`},
		{"insert", `{"id":"<",  "method":"m" }`, `{"a":1}`, `{"id":"<",  "method":"m" ,"params":{"a":1}}`},
		{"duplicate", `{"id":1,"method":"m","params":1,"params":2}`, `3`, `{"id":1,"method":"m","params":3,"params":3}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := jsonrpc.Parse([]byte(tc.in))
			got := withParams(m, json.RawMessage(tc.params))
			if string(got.Raw) != tc.want {
				t.Errorf("Raw = %s, want %s", got.Raw, tc.want)
			}
			if !json.Valid(got.Raw) {
				t.Errorf("Raw %s is not valid JSON", got.Raw)
			}
			if p := jsonrpc.Parse(got.Raw); string(p.Params) != tc.params || string(p.ID) != string(m.ID) {
				t.Errorf("reparsed params = %s id = %s, want %s and %s", p.Params, p.ID, tc.params, m.ID)
			}
		})
	}
}
