package app

import (
	"fmt"

	mcpserver "github.com/hop-/cachydb/internal/mcp"
)

type Builder struct {
	dbName    string
	rootDir   string
	transport string
	port      int
}

func NewBuilder() *Builder {
	return &Builder{}
}

func (b *Builder) WithDBName(name string) *Builder {
	b.dbName = name
	return b
}

func (b *Builder) WithRootDir(dir string) *Builder {
	b.rootDir = dir
	return b
}

func (b *Builder) WithTransport(transport string) *Builder {
	b.transport = transport
	return b
}

func (b *Builder) WithPort(port int) *Builder {
	b.port = port
	return b
}

func (b *Builder) Build() (*App, error) {
	httpAddr := fmt.Sprintf(":%d", b.port)
	mcpServer, err := mcpserver.NewServer(b.dbName, b.rootDir, b.transport, httpAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create MCP server: %w", err)
	}

	return &App{mcpServer: mcpServer}, nil
}
