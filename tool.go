package switchyard

import "encoding/json"

// methodToolsCall is the MCP method for calling a tool.
const methodToolsCall = "tools/call"

// ToolCall is the params of a tools/call request.
type ToolCall struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
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
	return &tc, true
}

type textContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// toolErrorResult returns {"content":[{"type":"text","text":message}],"isError":true}.
func toolErrorResult(message string) json.RawMessage {
	b, _ := json.Marshal(struct {
		Content []textContent `json:"content"`
		IsError bool          `json:"isError"`
	}{[]textContent{{Type: "text", Text: message}}, true})
	return b
}
