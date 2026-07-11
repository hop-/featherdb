# Development Guide

Guide for AI agents and developers working on CachyDB.

## Prerequisites

- Go 1.25+ (see `go.mod` for exact version)
- No external services required — CachyDB is fully self-contained

## Quick Start

```bash
cd cachydb

# Build
go build -o cachydb

# Run (stdio transport — for MCP client integration)
./cachydb

# Run (HTTP transport — for development/testing)
./cachydb -t http -p 7601

# Run tests
go test ./...

# Run with custom data directory
./cachydb -R /tmp/cachydb-dev
```

## Project Layout & Ownership

| Path | Purpose | When to modify |
|------|---------|----------------|
| `main.go` | Entry point | Almost never |
| `internal/cmd/` | CLI commands & flags | Adding new commands or flags |
| `internal/config/` | Configuration loading | Adding new config options |
| `internal/app/` | App lifecycle (builder) | Changing startup/shutdown behavior |
| `internal/mcp/server.go` | MCP tool definitions | Adding/modifying database operations exposed to AI |
| `pkg/db/types.go` | Core data structures | Adding new fields to documents, collections, etc. |
| `pkg/db/query.go` | CRUD operations | Modifying query behavior, adding operators |
| `pkg/db/schema.go` | Schema validation | Adding new field types or validation rules |
| `pkg/db/index.go` | Indexing system | Changing index behavior or adding index types |
| `pkg/db/storage.go` | Persistence orchestration | Modifying sync behavior or storage format |
| `pkg/db/binary_storage.go` | Binary format | Changing on-disk format (requires migration!) |
| `pkg/db/wal.go` | Write-ahead log | Modifying durability guarantees |
| `pkg/db/migration.go` | Schema migrations | Adding new migration versions |
| `pkg/db/compression.go` | Data compression | Changing compression algorithm |

## Key Patterns to Follow

### Adding a New MCP Tool

1. Add the tool registration in `internal/mcp/server.go` inside `registerTools()`
2. Define input parameters using `mcp.NewTool()` with JSON schema
3. Implement the handler function
4. Follow the write pattern: **in-memory mutation → WAL log → mark dirty**
5. Return results as JSON-serialized `mcp.TextContent`

### Adding a New Query Operator

1. Add the operator string constant (e.g., `"regex"`)
2. Implement comparison logic in `matchFilter()` in `pkg/db/query.go`
3. Consider whether it can leverage indexes

### Adding a New Field Type

1. Add the constant in `pkg/db/types.go` (e.g., `TypeUUID FieldType = "uuid"`)
2. Add type validation in `ValidateType()` in `pkg/db/schema.go`
3. Update documentation

### Modifying Binary Storage Format

**Warning**: Changes to binary format require incrementing `BinaryFormatVersion` and implementing backward-compatible reading of older formats.

1. Increment `BinaryFormatVersion` in `pkg/db/binary_storage.go`
2. Update write logic for new format
3. Keep read logic that handles both old and new versions
4. Add a schema migration if needed

### Adding a New CLI Command

1. Create a new file in `internal/cmd/` (e.g., `export.go`)
2. Define the cobra command with `Use`, `Short`, `Run`
3. Register it with `rootCmd.AddCommand()` or a parent command in `init()`
4. Add flags as needed

## Error Handling Conventions

- Return `fmt.Errorf("context: %w", err)` for wrapped errors
- MCP handlers return error text to the client (no panics)
- Storage errors are logged but don't crash the server
- WAL write failures are critical — operations should fail if WAL write fails

## Thread Safety Rules

1. Always acquire locks in this order to avoid deadlocks: `DatabaseManager → Database → Collection → Index`
2. Use `RLock()` for read-only operations, `Lock()` for mutations
3. Never hold a lock while performing I/O (release lock, do I/O, re-acquire if needed)
4. The WAL batch mutex is independent of the data mutexes

## Testing

```bash
# Run all tests
go test ./...

# Run with verbose output
go test -v ./...

# Run specific package tests
go test ./pkg/db/...

# Run with race detector
go test -race ./...
```

See [TESTING.md](../TESTING.md) for detailed testing guidelines.

## Common Development Tasks

### Reset local data

```bash
rm -rf ~/.cachydb
```

### Inspect WAL files

WAL files are in the root directory (`~/.cachydb/wal-*.log`). Each entry is:
```
[4 bytes: length][4 bytes: CRC32 checksum][JSON payload]
```

### Inspect binary storage

Collection data files are at `~/.cachydb/<db>/<collection>/collection.data` with the `CACH` magic header.

### Debug with HTTP transport

```bash
# Start in HTTP mode
./cachydb -t http -p 7601

# Test with curl (MCP protocol over HTTP)
curl -X POST http://localhost:7601/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"tools/list","id":1}'
```

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `github.com/spf13/cobra` | v1.9.1 | CLI framework |
| `github.com/kelseyhightower/envconfig` | v1.4.0 | Environment variable config |
| `github.com/google/uuid` | v1.6.0 | UUID generation for document IDs |
| `github.com/modelcontextprotocol/go-sdk` | latest | MCP server/client SDK |

## Known Limitations & Areas for Improvement

1. **Index limitation**: Hash indexes map value→single docID. Multiple documents with the same indexed value only store the last one. Consider adding multi-value index support.
2. **Query operators**: Only `eq` leverages indexes. Other operators always trigger full scans.
3. **No transactions**: Operations are atomic at the single-document level but there's no multi-document transaction support.
4. **Schema evolution**: Schemas are enforced on write but existing documents aren't retroactively validated when a schema changes.
5. **Error handling TODOs**: Several `// TODO: handle error` comments in `internal/cmd/` indicate incomplete error handling.
6. **No authentication**: The HTTP transport has no auth mechanism.
7. **Single-node only**: No replication or clustering support.
