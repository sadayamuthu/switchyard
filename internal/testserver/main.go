// Command testserver is a stdio MCP server for switchyard's end-to-end tests.
// Its tools are echo, which returns its text argument, and env, which returns
// the SWITCHYARD_CHAIN_ID it sees.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type echoArgs struct {
	Text string `json:"text"`
}

func text(s string) *mcp.CallToolResult {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: s}}}
}

func main() {
	s := mcp.NewServer(&mcp.Implementation{Name: "testserver", Version: "v0.0.0"}, nil)
	mcp.AddTool(s, &mcp.Tool{Name: "echo", Description: "returns its text argument"},
		func(_ context.Context, _ *mcp.CallToolRequest, in echoArgs) (*mcp.CallToolResult, any, error) {
			return text(in.Text), nil, nil
		})
	mcp.AddTool(s, &mcp.Tool{Name: "env", Description: "returns the SWITCHYARD_CHAIN_ID it sees"},
		func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, any, error) {
			return text(os.Getenv("SWITCHYARD_CHAIN_ID")), nil, nil
		})
	if err := s.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		fmt.Fprintln(os.Stderr, "testserver:", err)
		os.Exit(1)
	}
}
