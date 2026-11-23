package switchyard

import (
	"bytes"
	"encoding/json"
	"maps"
	"slices"
	"strings"

	"github.com/sadayamuthu/switchyard/jsonrpc"
)

// MCP methods with typed helpers.
const (
	methodToolsCall = "tools/call"
	methodToolsList = "tools/list"
)

// ToolCall is the params of a tools/call request.
type ToolCall struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
	// Seq numbers the session's tools/call requests for this tool name from
	// 1, in the order the proxy read them, like Request.Seq. The name is the
	// one the client sent, before any hook's ContinueWith. It is 0 in a
	// ToolCall not obtained from Request.ToolCall.
	Seq uint64 `json:"-"`
}

// ToolCall decodes r's params as a tools/call. It returns false if r is not a
// tools/call request or its params are not a JSON object.
func (r *Request) ToolCall() (*ToolCall, bool) {
	if r.Method != methodToolsCall {
		return nil, false
	}
	var tc ToolCall
	if err := json.Unmarshal(r.Params, &tc); err != nil {
		return nil, false
	}
	tc.Seq = r.toolSeq
	return &tc, true
}

// ToolResult is the result of a tools/call. Content holds each content block
// as raw JSON; Rest holds every other member (such as structuredContent and
// _meta) so that a decoded result marshals back without losing them.
type ToolResult struct {
	Content []json.RawMessage          `json:"content"`
	IsError bool                       `json:"isError,omitempty"`
	Rest    map[string]json.RawMessage `json:"-"`
}

// ToolResult decodes r as a tools/call result. It returns false if r is an
// error or its result is not an object with a content array. It does not
// know which method r answers: check the Request's Method first.
func (r *Response) ToolResult() (*ToolResult, bool) {
	if r.Error != nil {
		return nil, false
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(r.Result, &obj); err != nil || obj == nil {
		return nil, false
	}
	tr := &ToolResult{}
	if err := json.Unmarshal(obj["content"], &tr.Content); err != nil || tr.Content == nil {
		return nil, false
	}
	if v, ok := obj["isError"]; ok {
		if err := json.Unmarshal(v, &tr.IsError); err != nil {
			return nil, false
		}
	}
	delete(obj, "content")
	delete(obj, "isError")
	if len(obj) > 0 {
		tr.Rest = obj
	}
	return tr, true
}

// Marshal encodes tr as a tools/call result: content, isError if true, and
// the members in Rest (other than content and isError) in key order. Nil
// Content is encoded as [], and an empty value as null. Content and Rest
// values are written as their raw bytes, unchanged and not HTML-escaped; if one is not valid JSON neither is
// the result, and the proxy answers the client with a -32603 error naming
// the hook that returned it.
func (tr *ToolResult) Marshal() json.RawMessage {
	var b bytes.Buffer
	b.WriteString(`{"content":[`)
	for i, c := range tr.Content {
		if i > 0 {
			b.WriteByte(',')
		}
		writeRaw(&b, c)
	}
	b.WriteByte(']')
	if tr.IsError {
		b.WriteString(`,"isError":true`)
	}
	for _, k := range slices.Sorted(maps.Keys(tr.Rest)) {
		if k == "content" || k == "isError" {
			continue
		}
		b.WriteByte(',')
		b.Write(marshalString(k))
		b.WriteByte(':')
		writeRaw(&b, tr.Rest[k])
	}
	b.WriteByte('}')
	return b.Bytes()
}

// writeRaw writes v's bytes, or null if v is empty (as encoding/json does).
func writeRaw(b *bytes.Buffer, v json.RawMessage) {
	if len(v) == 0 {
		b.WriteString("null")
		return
	}
	b.Write(v)
}

// ToolInfo is one tool in a tools/list result: its name and its full JSON
// definition.
type ToolInfo struct {
	Name string
	Raw  json.RawMessage
}

// ToolList decodes r as a tools/list result. It returns false if r is an
// error or its result is not an object with a tools array of objects. It does
// not know which method r answers: check the Request's Method first.
func (r *Response) ToolList() ([]ToolInfo, bool) {
	if r.Error != nil {
		return nil, false
	}
	var res struct {
		Tools []json.RawMessage `json:"tools"`
	}
	if err := json.Unmarshal(r.Result, &res); err != nil || res.Tools == nil {
		return nil, false
	}
	tools := make([]ToolInfo, len(res.Tools))
	for i, raw := range res.Tools {
		var t struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &t); err != nil || raw[0] != '{' {
			return nil, false
		}
		tools[i] = ToolInfo{Name: t.Name, Raw: raw}
	}
	return tools, true
}

// ToolListResult returns the tools/list result orig with its tools member
// replaced by tools; every other byte of orig, such as nextCursor, is kept.
// Each tool is written as its raw bytes, or as {"name":Name} if Raw is empty;
// an invalid Raw makes the result invalid, which the proxy reports as a
// -32603 error naming the hook. If orig is not a JSON object, the result is
// {"tools":[...]}.
func ToolListResult(orig json.RawMessage, tools []ToolInfo) json.RawMessage {
	var arr bytes.Buffer
	arr.WriteByte('[')
	for i, t := range tools {
		if i > 0 {
			arr.WriteByte(',')
		}
		raw := t.Raw
		if len(raw) == 0 {
			raw = append(append([]byte(`{"name":`), marshalString(t.Name)...), '}')
		}
		arr.Write(raw)
	}
	arr.WriteByte(']')
	out, ok := setMember(orig, "tools", func(k string) bool { return strings.EqualFold(k, "tools") }, arr.Bytes())
	if !ok {
		out, _ = setMember([]byte("{}"), "tools", func(string) bool { return false }, arr.Bytes())
	}
	return out
}

// marshalString encodes s as a JSON string without HTML escaping.
func marshalString(s string) []byte {
	b, _ := jsonrpc.Marshal(s) // a string always encodes
	return b
}

type textContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// toolErrorResult returns {"content":[{"type":"text","text":message}],"isError":true}.
func toolErrorResult(message string) json.RawMessage {
	b, _ := jsonrpc.Marshal(struct {
		Content []textContent `json:"content"`
		IsError bool          `json:"isError"`
	}{[]textContent{{Type: "text", Text: message}}, true}) // strings always encode
	return b
}
