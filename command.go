package switchyard

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"
)

// upstreamGrace is how long RunCommand lets the upstream exit after its stdin
// is closed before killing it, and how long it waits after the upstream exits
// for the upstream's stdout to reach EOF before treating it as ended.
const upstreamGrace = 2 * time.Second

// RunCommand starts argv as the upstream server and Serves one session
// between the client (down and downOut) and the upstream's stdin and stdout.
// The upstream's stderr is the current process's, and its environment is the
// current process's with ChainEnv set to the proxy's chain ID. If the proxy
// has no Options.ChainID, RunCommand sets it to ChainIDFromEnv(), so the
// upstream and this proxy's events use the same ID. RunCommand must not be
// called while p is serving another session.
//
// Serve's rules apply, with these additions. When down reaches EOF the
// upstream's stdin is closed; if the upstream has not exited 2s later it is
// killed. When ctx is done the upstream is killed. When the upstream exits,
// RunCommand returns within about 2s, even if down never returns from Read
// and has no Close method: the goroutine reading down is then left behind,
// blocked, and the caller is expected to exit soon. In every case RunCommand
// waits for the upstream to exit and reaps it before returning.
//
// RunCommand returns an error naming argv[0] if the upstream cannot be
// started. If the upstream ends first, the error wraps ErrUpstreamClosed and
// includes the upstream's exit status when it was not a clean exit.
func RunCommand(ctx context.Context, p *Proxy, argv []string, down io.Reader, downOut io.Writer) error {
	if len(argv) == 0 {
		return errors.New("switchyard: no upstream command")
	}
	if p.opts.ChainID == "" {
		p.opts.ChainID = ChainIDFromEnv()
	}

	// Pipes made here, rather than by exec.Cmd, are not closed by cmd.Wait,
	// so Wait can run while Serve still reads the upstream's last output.
	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("switchyard: starting upstream %q: %w", argv[0], err)
	}
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		_ = stdinR.Close()
		_ = stdinW.Close()
		return fmt.Errorf("switchyard: starting upstream %q: %w", argv[0], err)
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = append(os.Environ(), ChainEnv+"="+p.opts.ChainID) // the last value of a key wins
	cmd.Stdin, cmd.Stdout, cmd.Stderr = stdinR, stdoutW, os.Stderr
	err = cmd.Start()
	_ = stdinR.Close() // the child has its own copies of these ends
	_ = stdoutW.Close()
	if err != nil {
		_ = stdinW.Close()
		_ = stdoutR.Close()
		return fmt.Errorf("switchyard: starting upstream %q: %w", argv[0], err)
	}

	var waitErr error
	exited := make(chan struct{})
	go func() { waitErr = cmd.Wait(); close(exited) }()

	up := &upstreamOutput{f: stdoutR}
	upOut := &upstreamInput{f: stdinW, closed: make(chan struct{})}
	stop := make(chan struct{})
	watched := make(chan struct{})
	go func() {
		defer close(watched)
		watchUpstream(ctx, cmd.Process, upOut.closed, exited, up, stop)
	}()

	err = p.Serve(ctx, newDetachedReader(down), downOut, up, upOut)

	_ = upOut.Close() // starts the kill timer, if Serve has not already
	<-exited
	close(stop)
	<-watched
	_ = stdoutR.Close()
	if errors.Is(err, ErrUpstreamClosed) && waitErr != nil {
		err = fmt.Errorf("%w (%s: %v)", err, argv[0], waitErr)
	}
	return err
}

// watchUpstream kills proc when ctx is done or when it has not exited
// upstreamGrace after stdinClosed is closed, and ends up's output
// upstreamGrace after the process exits, in case a process it started still
// holds its stdout open. It returns when stop is closed.
func watchUpstream(ctx context.Context, proc *os.Process, stdinClosed, exited <-chan struct{}, up *upstreamOutput, stop <-chan struct{}) {
	kill := func() { _ = proc.Kill() } // harmless once the process is done
	ctxDone := ctx.Done()
	var killAfter, endAfter <-chan time.Time
	for {
		select {
		case <-stop:
			return
		case <-ctxDone:
			kill()
			ctxDone = nil
		case <-stdinClosed:
			killAfter = time.After(upstreamGrace)
			stdinClosed = nil
		case <-killAfter:
			kill()
			killAfter = nil
		case <-exited:
			killAfter = nil
			endAfter = time.After(upstreamGrace)
			exited = nil
		case <-endAfter:
			up.end()
			endAfter = nil
		}
	}
}

// upstreamInput is the upstream's stdin. Close reports itself on closed.
type upstreamInput struct {
	f      *os.File
	once   sync.Once
	closed chan struct{}
}

func (u *upstreamInput) Write(p []byte) (int, error) { return u.f.Write(p) }

func (u *upstreamInput) Close() error {
	err := os.ErrClosed
	u.once.Do(func() {
		err = u.f.Close()
		close(u.closed)
	})
	return err
}

// upstreamOutput is the upstream's stdout. After end, reads report EOF.
type upstreamOutput struct {
	f     *os.File
	ended atomic.Bool
}

func (u *upstreamOutput) Read(p []byte) (int, error) {
	n, err := u.f.Read(p)
	if err != nil && u.ended.Load() {
		err = io.EOF
	}
	return n, err
}

func (u *upstreamOutput) Close() error { return u.f.Close() }

// end makes the output reach EOF, unblocking a Read in progress.
func (u *upstreamOutput) end() {
	u.ended.Store(true)
	_ = u.f.Close()
}

// detachedReader reads src on its own goroutine, so that Close always
// unblocks a Read in progress, even when src is blocked in Read and cannot
// be closed. That goroutine is then left behind until src's Read returns.
type detachedReader struct {
	pr   *io.PipeReader
	src  io.Reader
	once sync.Once
}

func newDetachedReader(src io.Reader) *detachedReader {
	pr, pw := io.Pipe()
	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				if _, werr := pw.Write(buf[:n]); werr != nil {
					return // the reader is closed
				}
			}
			if err != nil {
				_ = pw.CloseWithError(err) // io.EOF reaches the reader as io.EOF
				return
			}
		}
	}()
	return &detachedReader{pr: pr, src: src}
}

func (d *detachedReader) Read(p []byte) (int, error) { return d.pr.Read(p) }

// Close closes the reader, and src too if it is an io.Closer.
func (d *detachedReader) Close() error {
	var err error
	d.once.Do(func() {
		_ = d.pr.Close()
		if c, ok := d.src.(io.Closer); ok {
			err = c.Close()
		}
	})
	return err
}
