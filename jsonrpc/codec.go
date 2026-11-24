package jsonrpc

import (
	"bufio"
	"bytes"
	"io"
	"sync"
)

// Reader reads newline-delimited messages with no line length cap.
type Reader struct {
	br *bufio.Reader
}

// NewReader returns a Reader over r.
func NewReader(r io.Reader) *Reader { return &Reader{br: bufio.NewReader(r)} }

// Read returns the next non-empty message, or io.EOF at end of input.
func (r *Reader) Read() (*Message, error) {
	for {
		line, err := r.br.ReadBytes('\n')
		line = bytes.TrimSuffix(line, []byte("\n"))
		line = bytes.TrimSuffix(line, []byte("\r"))
		if len(line) > 0 {
			return Parse(line), nil
		}
		if err != nil {
			return nil, err
		}
	}
}

// Writer writes messages as raw bytes plus a newline.
type Writer struct {
	mu sync.Mutex
	w  io.Writer
}

// NewWriter returns a Writer to w.
func NewWriter(w io.Writer) *Writer { return &Writer{w: w} }

// Write writes m.Raw followed by a newline, atomically with respect to other writers.
func (w *Writer) Write(m *Message) error {
	buf := make([]byte, 0, len(m.Raw)+1)
	buf = append(append(buf, m.Raw...), '\n')
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.w.Write(buf)
	return err
}
