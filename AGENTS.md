# CachyDB — AI Agent Context

> Read by AI coding assistants (Copilot, Cursor, Claude Code, Windsurf, Aider) to understand the project.

## Project Overview

CachyDB is a lightweight document-based database with built-in MCP (Model Context Protocol) support. Written in Go. Single binary, no external dependencies at runtime.

## Quick Reference

- **Language**: Go 1.25+
- **Module**: `github.com/hop-/cachydb`
- **Build**: `go build -o cachydb`
- **Test**: `go test ./...`
- **Run**: `./cachydb` (stdio) or `./cachydb -t http` (HTTP)
- **Data dir**: `~/.cachydb` (override with `-R` or `ROOT_DIR` env)

## Developer Documentation

Detailed docs live in `dev-docs/`. **Read before modifying code.**

| Document | Contents |
|----------|----------|
| [`dev-docs/PROJECT.md`](dev-docs/PROJECT.md) | **Main doc** — project overview, architecture, components, data flow |
| [`dev-docs/DATABASE_ENGINE.md`](dev-docs/DATABASE_ENGINE.md) | Full `pkg/db` reference — types, query engine, schema, indexing, binary storage, WAL, compression, migrations |
| [`dev-docs/MCP_SERVER.md`](dev-docs/MCP_SERVER.md) | All 13 MCP tools — parameters, behavior, error handling, write consistency |
| [`dev-docs/CLI.md`](dev-docs/CLI.md) | CLI commands, flags, configuration, build instructions |
| [`dev-docs/DEVELOPMENT.md`](dev-docs/DEVELOPMENT.md) | Development guide — patterns, thread safety, testing, known limitations |

Start with `dev-docs/PROJECT.md` for orientation, then `dev-docs/DEVELOPMENT.md` for coding patterns and known issues.

## Key Source Locations

| Path | Role |
|------|------|
| `internal/mcp/server.go` | MCP tool registrations and handlers |
| `pkg/db/` | Database engine (public library) |
| `internal/cmd/` | CLI command definitions |
| `internal/config/` | Configuration loading |
| `internal/app/` | Application lifecycle |
