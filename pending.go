package switchyard

import (
	"encoding/json"
	"sync"
)

// pendingEntry is a client→server request awaiting its upstream response.
type pendingEntry struct {
	id json.RawMessage
}

// pendingTable tracks client→server requests by IDKey. Once closed (upstream
// gone), it accepts no new entries.
type pendingTable struct {
	mu      sync.Mutex
	entries map[string]*pendingEntry
	closed  bool
}

func newPendingTable() *pendingTable {
	return &pendingTable{entries: make(map[string]*pendingEntry)}
}

// add records a request. It returns false if the table is closed.
func (t *pendingTable) add(key string, e *pendingEntry) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return false
	}
	t.entries[key] = e
	return true
}

// remove deletes and returns the entry for key, if present.
func (t *pendingTable) remove(key string) (*pendingEntry, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.entries[key]
	if ok {
		delete(t.entries, key)
	}
	return e, ok
}

// close marks the table closed and returns every remaining entry.
func (t *pendingTable) close() []*pendingEntry {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	out := make([]*pendingEntry, 0, len(t.entries))
	for k, e := range t.entries {
		out = append(out, e)
		delete(t.entries, k)
	}
	return out
}

func (t *pendingTable) len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.entries)
}
