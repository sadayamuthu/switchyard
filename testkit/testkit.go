// Package testkit wires a real MCP SDK client and server through a switchyard
// proxy, for tests of switchyard itself and of tools built on it.
package testkit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sadayamuthu/switchyard"
)

// cleanupTimeout bounds how long cleanup waits for the proxy and server to stop.
const cleanupTimeout = 5 * time.Second

// ToolSpec describes a tool served by Server.
type ToolSpec struct {
	Name        string
	Description string
	// Handler receives the call's arguments decoded as a JSON object.
	Handler func(ctx context.Context, args map[string]any) (*mcp.CallToolResult, error)
}

// Server returns an MCP SDK server offering tools. Tools accept any JSON
// object as input.
func Server(t testing.TB, tools ...ToolSpec) *mcp.Server {
	t.Helper()
	s := mcp.NewServer(&mcp.Implementation{Name: "testkit-server", Version: "v0.0.0"}, nil)
	for _, spec := range tools {
		s.AddTool(&mcp.Tool{
			Name:        spec.Name,
			Description: spec.Description,
			InputSchema: map[string]any{"type": "object"},
		}, toolHandler(spec))
	}
	return s
}

// ResourceSpec describes a text resource served by ServerWithResources.
type ResourceSpec struct {
	URI  string
	Text string
}

// ServerWithResources is Server that also offers resources, each read as one
// text/plain content block holding its Text.
func ServerWithResources(t testing.TB, resources []ResourceSpec, tools ...ToolSpec) *mcp.Server {
	t.Helper()
	s := Server(t, tools...)
	for _, spec := range resources {
		s.AddResource(&mcp.Resource{URI: spec.URI, Name: spec.URI, MIMEType: "text/plain"},
			func(context.Context, *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
				return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{
					{URI: spec.URI, MIMEType: "text/plain", Text: spec.Text},
				}}, nil
			})
	}
	return s
}

func toolHandler(spec ToolSpec) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := map[string]any{}
		if raw := req.Params.Arguments; len(raw) > 0 {
			if err := json.Unmarshal(raw, &args); err != nil {
				return nil, fmt.Errorf("tool %s: decoding arguments: %w", spec.Name, err)
			}
		}
		return spec.Handler(ctx, args)
	}
}

// Connect connects an SDK client to server through a switchyard proxy built
// from opts, over in-process pipes. Everything is shut down by t.Cleanup.
func Connect(t testing.TB, server *mcp.Server, opts switchyard.Options) *mcp.ClientSession {
	t.Helper()
	return ConnectWith(t, server, opts, nil)
}

// ConnectWith is Connect with SDK client options, for tests that need client
// handlers such as LoggingMessageHandler.
func ConnectWith(t testing.TB, server *mcp.Server, opts switchyard.Options, clientOpts *mcp.ClientOptions) *mcp.ClientSession {
	t.Helper()
	c2pR, c2pW := io.Pipe() // client → proxy
	p2cR, p2cW := io.Pipe() // proxy → client
	p2sR, p2sW := io.Pipe() // proxy → server
	s2pR, s2pW := io.Pipe() // server → proxy

	ss := connectServer(t, server, p2sR, s2pW)

	ctx, cancel := context.WithCancel(context.Background())
	proxy := switchyard.New(opts)
	served := make(chan error, 1)
	go func() {
		err := proxy.Serve(ctx, c2pR, p2cW, s2pR, p2sW)
		_ = p2cW.Close() // the client sees EOF once the proxy is gone
		served <- err
	}()

	cs, err := mcp.NewClient(clientImpl(), clientOpts).Connect(context.Background(), &mcp.IOTransport{Reader: p2cR, Writer: c2pW}, nil)
	if err != nil {
		cancel()
		<-served
		_ = ss.Close()
		t.Fatalf("testkit: connecting client through proxy: %v", err)
	}

	t.Cleanup(func() {
		_ = cs.Close()
		select {
		case err := <-served:
			if err != nil && !errors.Is(err, switchyard.ErrUpstreamClosed) {
				t.Errorf("testkit: Serve: %v", err)
			}
		case <-time.After(cleanupTimeout):
			t.Errorf("testkit: Serve did not return within %v of the client closing", cleanupTimeout)
			cancel()
			<-served
		}
		cancel()
		_ = ss.Close()
		_ = ss.Wait()
	})
	return cs
}

// ConnectDirect connects an SDK client straight to server over in-process
// pipes, with no proxy, for comparison with Connect.
func ConnectDirect(t testing.TB, server *mcp.Server) *mcp.ClientSession {
	t.Helper()
	c2sR, c2sW := io.Pipe()
	s2cR, s2cW := io.Pipe()

	ss := connectServer(t, server, c2sR, s2cW)
	cs, err := mcp.NewClient(clientImpl(), nil).Connect(context.Background(), &mcp.IOTransport{Reader: s2cR, Writer: c2sW}, nil)
	if err != nil {
		_ = ss.Close()
		t.Fatalf("testkit: connecting client directly: %v", err)
	}
	t.Cleanup(func() {
		_ = cs.Close()
		_ = ss.Close()
		_ = ss.Wait()
	})
	return cs
}

func connectServer(t testing.TB, server *mcp.Server, r io.ReadCloser, w io.WriteCloser) *mcp.ServerSession {
	t.Helper()
	ss, err := server.Connect(context.Background(), &mcp.IOTransport{Reader: r, Writer: w}, nil)
	if err != nil {
		t.Fatalf("testkit: connecting server: %v", err)
	}
	return ss
}

func clientImpl() *mcp.Implementation {
	return &mcp.Implementation{Name: "testkit-client", Version: "v0.0.0"}
}
