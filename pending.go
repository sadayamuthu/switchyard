package switchyard

import (
	"context"
	"encoding/json"
	"sync"
)

// pendingEntry is a client→server request awaiting a response: one being
// hooked, forwarded upstream, or held by Hang.
type pendingEntry struct {
	id json.RawMessage
	// req is the request as hooks saw it; nil when there are no hooks.
	req *Request
	// cancel cancels the request's hook context; nil when there are no hooks.
	cancel context.CancelFunc
	// hung is set, under the table's lock, once a hook returned Hang: the
	// request must never be answered.
	hung bool
}

// release cancels the entry's hook context, if it has one.
func (e *pendingEntry) release() {
	if e.cancel != nil {
		e.cancel()
	}
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

// removeIf deletes the entry for key if it is still e. It reports whether it
// did, which gives the caller sole ownership of answering the request.
func (t *pendingTable) removeIf(key string, e *pendingEntry) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.entries[key] != e {
		return false
	}
	delete(t.entries, key)
	return true
}

// has reports whether the entry for key is still e.
func (t *pendingTable) has(key string, e *pendingEntry) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.entries[key] == e
}

// markHung marks e, if it is still the entry for key, as never to be
// answered. It reports whether it did.
func (t *pendingTable) markHung(key string, e *pendingEntry) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.entries[key] != e {
		return false
	}
	e.hung = true
	return true
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
