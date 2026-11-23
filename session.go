package switchyard

import (
	"crypto/rand"
	"encoding/hex"
)

// Session is the state of one Serve call: one client connected to one
// upstream server.
type Session struct {
	id      string
	chainID string
}

func newSession(chainID string) *Session {
	var b [8]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error.
	return &Session{id: hex.EncodeToString(b[:]), chainID: chainID}
}

// ID returns the session's random 16-hex-character identifier.
func (s *Session) ID() string { return s.id }

// ChainID returns the chain ID shared by every hop proxying this session.
func (s *Session) ChainID() string { return s.chainID }
