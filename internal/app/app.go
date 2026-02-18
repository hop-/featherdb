package app

import (
	"context"

	mcpserver "github.com/hop-/cachydb/internal/mcp"
)

type App struct {
	mcpServer *mcpserver.Server
}

func (a *App) Start(ctx context.Context) error {
	return a.mcpServer.Start(ctx)
}

func (a *App) Stop() error {
	// TODO: implement graceful shutdown
	return nil
}
