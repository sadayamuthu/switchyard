package switchyard

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
)

// Session is the state of one Serve call: one client connected to one
// upstream server. Hooks share it across all of the session's requests and
// responses. Its methods are safe for concurrent use.
type Session struct {
	id      string
	chainID string

	mu    sync.Mutex
	vals  map[string]any
	tools []ToolInfo
}

func newSession(chainID string) *Session {
	var b [8]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error.
	return &Session{id: hex.EncodeToString(b[:]), chainID: chainID, vals: make(map[string]any)}
}

// ID returns the session's random 16-hex-character identifier.
func (s *Session) ID() string { return s.id }

// ChainID returns the chain ID shared by every hop proxying this session.
func (s *Session) ChainID() string { return s.chainID }

// Get returns the value stored under key, and whether there is one.
func (s *Session) Get(key string) (any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.vals[key]
	return v, ok
}

// Set stores v under key.
func (s *Session) Set(key string, v any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vals[key] = v
}

// Update atomically replaces the value under key with fn(old), where old is
// the current value or nil if there is none, and returns the new value. It
// suits counters and other read-modify-write state. fn runs with the
// session locked, so it must be quick and must not call the Session's
// methods.
func (s *Session) Update(key string, fn func(old any) any) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := fn(s.vals[key])
	s.vals[key] = v
	return v
}

// Tools returns the tools of the most recent successful tools/list response
// sent to the client, as rewritten by hooks; nil before the first. With a
// paginated list, it is the most recent page. It is updated only when the
// proxy has hooks.
func (s *Session) Tools() []ToolInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ToolInfo(nil), s.tools...)
}

func (s *Session) setTools(tools []ToolInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tools = tools
}
