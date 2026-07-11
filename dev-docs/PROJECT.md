# CachyDB — Project Overview

## Purpose

CachyDB is a lightweight document-based database with built-in Model Context Protocol (MCP) support. It stores JSON-like documents in collections within databases, supports schema validation, hash-based indexing, and exposes all functionality as MCP tools for AI assistant integration.

## Related docs

- [DATABASE_ENGINE.md](DATABASE_ENGINE.md) — Storage, indexing, WAL, query engine
- [MCP_SERVER.md](MCP_SERVER.md) — MCP tools, transports, write consistency
- [CLI.md](CLI.md) — Commands, flags, configuration
- [DEVELOPMENT.md](DEVELOPMENT.md) — Coding patterns, testing, known issues

## High-Level Architecture

```
┌───────────────────────────────────────────────────────────┐
│                       CLI (cobra)                          │
│  cachydb [app | version | utils list | utils migrate]     │
└──────────────────────────┬────────────────────────────────┘
                           │
┌──────────────────────────▼────────────────────────────────┐
│                   Application Layer                        │
│           internal/app (Builder pattern)                   │
└──────────────────────────┬────────────────────────────────┘
                           │
┌──────────────────────────▼────────────────────────────────┐
│                  MCP Server Layer                          │
│          internal/mcp (13 registered tools)                │
│          Transports: stdio | HTTP/SSE                      │
└──────────────────────────┬────────────────────────────────┘
                           │
┌──────────────────────────▼────────────────────────────────┐
│                  Database Engine (pkg/db)                  │
│  ┌─────────┐  ┌──────────┐  ┌────────┐  ┌────────────┐  │
│  │  Query  │  │  Schema  │  │ Index  │  │  Storage   │  │
│  │ Engine  │  │Validation│  │ (Hash) │  │  Manager   │  │
│  └─────────┘  └──────────┘  └────────┘  └─────┬──────┘  │
│                                                │          │
│                  ┌─────────────────────────────┼───┐      │
│                  │           WAL               │   │      │
│                  │   (Write-Ahead Log)         │   │      │
│                  └─────────────────────────────┘   │      │
│                                                    │      │
│                  ┌─────────────────────────────────▼─┐    │
│                  │      Binary Storage               │    │
│                  │  (.data + .idx + compression)     │    │
│                  └───────────────────────────────────┘    │
└───────────────────────────────────────────────────────────┘
```

## Directory Structure

```
cachydb/
├── main.go                     # Entry point → cmd.Execute()
├── go.mod                      # Module: github.com/hop-/cachydb
├── internal/
│   ├── app/
│   │   ├── app.go             # App struct wrapping MCP server (Start/Stop)
│   │   └── builder.go         # Fluent builder for App configuration
│   ├── cmd/
│   │   ├── root.go            # Root cobra command, config init
│   │   ├── app.go             # `app` subcommand + shared flag setup
│   │   ├── list.go            # `utils list` — list databases
│   │   ├── migrate.go         # `utils migrate` — schema migrations
│   │   ├── utils.go           # `utils` command group
│   │   ├── variables.go       # Shared CLI variables
│   │   └── version.go         # `version` command
│   ├── config/
│   │   ├── config.go          # Config struct + envconfig loading
│   │   └── config_windows.go  # Windows-specific root dir name
│   └── mcp/
│       └── server.go          # MCP server: all tool registrations + handlers
├── pkg/db/                     # Public database engine library
│   ├── types.go               # Core types: Document, Collection, Database, etc.
│   ├── schema.go              # Schema validation logic
│   ├── index.go               # Hash index CRUD + disk persistence
│   ├── query.go               # Query engine (Insert, Find, Update, Delete)
│   ├── storage.go             # StorageManager + background sync
│   ├── binary_storage.go      # Binary format reader/writer + offset index
│   ├── wal.go                 # Write-Ahead Log (batched, checksummed)
│   ├── compression.go         # gzip compress/decompress
│   └── migration.go           # Schema version migration system
└── examples/
    ├── basic/main.go          # Direct Go library usage
    └── mcp-client/main.go     # Programmatic MCP client usage
```

## Design Principles

1. **Layered architecture** — `pkg/db` is a standalone reusable library; `internal/mcp` wraps it as MCP tools; `internal/cmd` provides CLI orchestration.
2. **Builder pattern** — `app.Builder` provides a fluent API for configuring the application before starting.
3. **Write-ahead durability** — All mutations are logged to WAL synchronously before in-memory state is modified, ensuring crash recovery.
4. **Background persistence** — A background goroutine lazily syncs dirty data to binary storage every 5 seconds, avoiding write amplification.
5. **Thread safety** — `sync.RWMutex` protects all shared state (DatabaseManager, Database, Collection, Index).
6. **Dual storage formats** — Binary (default, compressed) for production; JSON (legacy) for backward compatibility.
7. **Checkpoint-based recovery** — WAL replay starts from the last checkpoint offset, avoiding full replay on restart.

## Data Flow

### Write Path (e.g., Insert Document)

```
MCP Tool Handler
  │
  ├─1─► In-memory: Collection.Insert(doc)
  │      - Schema validation
  │      - Index update
  │      - Documents map insertion
  │
  ├─2─► WAL: storage.LogInsert(db, coll, doc)
  │      - Append to WAL batch → flush (sync for writes)
  │      - CRC32 checksummed entry
  │
  └─3─► MarkDirty(db, collection)
         - Background syncer picks this up every 5s
         - Writes binary format + offset index
         - Creates WAL checkpoint
```

### Read Path (e.g., Find Documents)

```
MCP Tool Handler
  │
  └──► Collection.Find(query)
       - Check index for eq filters (O(1) lookup)
       - Full scan for non-indexed filters
       - Apply remaining filters, skip, limit
       - Return cloned documents
```

### Startup / Recovery

```
1. NewStorageManager(rootDir) → creates WAL manager
2. storage.LoadAllDatabases()
   - Reads binary storage files (.data + .idx)
   - Falls back to JSON if binary not found
   - Replays WAL entries after last checkpoint
3. storage.StartBackgroundSync(dbManager)
4. Ensure default database exists
5. Create MCP server, register tools
6. Start transport (stdio or HTTP)
```

## Concurrency Model

- **DatabaseManager.mu** — Guards the databases map (add/remove databases)
- **Database.mu** — Guards the collections map within a database
- **Collection.mu** — Guards documents and indexes within a collection
- **Index.mu** — Guards index data map
- **WALManager.batchMu** — Guards the write batch buffer
- **StorageManager.dirtyMu** — Guards the dirty entries map

All read operations use `RLock()`, write operations use `Lock()`.
