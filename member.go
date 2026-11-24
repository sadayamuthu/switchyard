package switchyard

import (
	"bytes"
	"encoding/json"
	"io"
	"slices"
)

// setMember returns a copy of the JSON object raw with every member whose
// name matches replaced by value, or removed if value is empty. If none
// matches and value is not empty, the member name:value is appended. All
// other bytes of raw are kept exactly. ok is false if raw is not a JSON
// object.
func setMember(raw []byte, name string, match func(string) bool, value []byte) (out []byte, ok bool) {
	spans, members, closing, ok := memberSpans(raw, match)
	if !ok {
		return nil, false
	}
	out = append([]byte(nil), raw...)
	switch {
	case len(spans) == 0 && len(value) > 0:
		key, _ := json.Marshal(name)
		member := append(append(key, ':'), value...)
		if members > 0 {
			member = append([]byte(","), member...)
		}
		out = slices.Insert(out, closing, member...)
	case len(value) > 0:
		for i := len(spans) - 1; i >= 0; i-- {
			out = slices.Replace(out, spans[i].valueStart, spans[i].valueEnd, value...)
		}
	default:
		for i := len(spans) - 1; i >= 0; i-- {
			start, end := spans[i].memberStart, spans[i].valueEnd
			if spans[i].first {
				// No comma precedes the first member; drop the one after it.
				j := end
				for j < len(out) && isSpace(out[j]) {
					j++
				}
				if j < len(out) && out[j] == ',' {
					end = j + 1
				}
			}
			out = slices.Delete(out, start, end)
		}
	}
	return out, true
}

// memberSpan locates one member in a raw JSON object. memberStart is just
// after the preceding value (or the opening brace), so the span from
// memberStart to valueEnd includes the separating comma unless first.
type memberSpan struct {
	memberStart, valueStart, valueEnd int
	first                             bool
}

// memberSpans returns the spans of raw's members whose names match, raw's
// member count and the offset of raw's closing brace. ok is false if raw is
// not a JSON object.
func memberSpans(raw []byte, match func(string) bool) (spans []memberSpan, members, closing int, ok bool) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return nil, 0, 0, false
	}
	for ; dec.More(); members++ {
		start := int(dec.InputOffset())
		t, err := dec.Token()
		if err != nil {
			return nil, 0, 0, false
		}
		key, _ := t.(string)
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, 0, 0, false
		}
		end := int(dec.InputOffset())
		if match(key) {
			spans = append(spans, memberSpan{memberStart: start, valueStart: end - len(v), valueEnd: end, first: members == 0})
		}
	}
	if _, err := dec.Token(); err != nil { // the closing brace
		return nil, 0, 0, false
	}
	closing = int(dec.InputOffset()) - 1
	if _, err := dec.Token(); err != io.EOF { // nothing may follow
		return nil, 0, 0, false
	}
	return spans, members, closing, true
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }
