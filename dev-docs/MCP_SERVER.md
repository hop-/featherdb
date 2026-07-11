# MCP Server Reference

The MCP (Model Context Protocol) server is implemented in `internal/mcp/server.go` and exposes CachyDB operations as tools that AI assistants can call.

## Server Configuration

| Parameter | Source | Default | Description |
|-----------|--------|---------|-------------|
| Transport | `TRANSPORT` env / `-t` flag | `stdio` | Transport type |
| Port | `PORT` env / `-p` flag | `7601` | HTTP transport port |
| Default DB | `DB_NAME` env | `main` | Initial database name |
| Root Dir | `ROOT_DIR` env / `-R` flag | `~/.cachydb` | Data directory |

## Transports

### stdio (default)

Communication via stdin/stdout. Used by local AI clients (e.g., Claude Desktop).

```json
// mcp-config.json
{
  "mcpServers": {
    "cachydb": {
      "command": "cachydb"
    }
  }
}
```

### HTTP (Streamable HTTP + SSE)

Network-accessible endpoint at `/mcp`. Supports multiple concurrent clients.

```bash
cachydb -t http -p 7601
# Listens on http://localhost:7601/mcp
```

```json
// mcp-config.json
{
  "mcpServers": {
    "cachydb": {
      "url": "http://localhost:7601/mcp"
    }
  }
}
```

## Startup Sequence

1. Create `StorageManager` with configured root directory
2. Load all databases from disk (binary storage + WAL replay)
3. Start background sync goroutine (5s interval)
4. Ensure default database exists (create if missing, log to WAL)
5. Create MCP `Server` with implementation info (`name: "cachydb"`, `version: "1.0.0"`)
6. Register all 13 tools
7. Start selected transport

## Graceful Shutdown

- HTTP transport: context cancellation triggers `httpServer.Shutdown()` with 5s timeout
- stdio transport: context cancellation stops the transport loop
- Background syncer performs a final sync before stopping

---

## Registered Tools (13)

### Database Management

#### `create_database`

Create a new database.

**Parameters**:
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | yes | Database name |

**Behavior**: Creates database in memory → logs `create_database` to WAL.

---

#### `list_databases`

List all databases.

**Parameters**: None

**Returns**: JSON array of database names.

---

#### `delete_database`

Delete a database and all its collections.

**Parameters**:
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | yes | Database name |

**Behavior**: Removes from memory → logs `delete_database` to WAL → marks dirty.

---

#### `use_database`

Switch the default database for subsequent operations.

**Parameters**:
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | yes | Database name to switch to |

**Behavior**: Updates server's `defaultDBName`. Does not persist (session-level only).

---

#### `current_database`

Get the name of the current default database.

**Parameters**: None

**Returns**: Current default database name.

---

### Collection Management

#### `create_collection`

Create a new collection, optionally with a schema.

**Parameters**:
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | yes | Collection name |
| `database` | string | no | Target database (uses default if omitted) |
| `schema` | object | no | Schema definition with field types and required flags |

**Schema format**:
```json
{
  "fields": {
    "fieldName": { "type": "string", "required": true },
    "age": { "type": "number", "required": false }
  }
}
```

**Valid types**: `string`, `number`, `boolean`, `object`, `array`, `date`

---

#### `list_collections`

List all collections in a database.

**Parameters**:
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `database` | string | no | Target database (uses default if omitted) |

---

### Document Operations

#### `insert_document`

Insert a document into a collection.

**Parameters**:
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `collection` | string | yes | Collection name |
| `document` | object | yes | Document data (key-value pairs) |
| `database` | string | no | Target database |

**Behavior**:
1. If `_id` not provided, generates UUID
2. Validates against collection schema (if defined)
3. Inserts into in-memory documents map
4. Updates all indexes
5. Logs `insert` to WAL (sync)
6. Marks collection dirty

**Returns**: The inserted document with its `_id`.

---

#### `find_documents`

Query documents with filters.

**Parameters**:
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `collection` | string | yes | Collection name |
| `database` | string | no | Target database |
| `filters` | array | no | Array of filter objects |
| `limit` | number | no | Max documents to return |
| `skip` | number | no | Documents to skip |

**Filter format**:
```json
{
  "filters": [
    { "field": "status", "operator": "eq", "value": "active" },
    { "field": "age", "operator": "gte", "value": 18 }
  ]
}
```

**Supported operators**: `eq`, `ne`, `gt`, `lt`, `gte`, `lte`, `in`

**Index usage**: If the first filter uses `eq` and an index exists for that field, performs O(1) lookup instead of full scan.

---

#### `update_document`

Update a document by ID.

**Parameters**:
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `collection` | string | yes | Collection name |
| `id` | string | yes | Document `_id` |
| `updates` | object | yes | Fields to update (merged into existing data) |
| `database` | string | no | Target database |

**Behavior**: Partial update — only specified fields are changed, others preserved. Schema validation runs on the updated document.

---

#### `delete_document`

Delete a document by ID.

**Parameters**:
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `collection` | string | yes | Collection name |
| `id` | string | yes | Document `_id` |
| `database` | string | no | Target database |

---

### Index Management

#### `create_index`

Create an index on a collection field.

**Parameters**:
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `collection` | string | yes | Collection name |
| `index_name` | string | yes | Name for the index |
| `field_name` | string | yes | Field to index |
| `database` | string | no | Target database |

**Behavior**: Scans all existing documents to build the index, then persists it. Logs `create_index` to WAL.

---

## Error Handling

All tools return errors as MCP tool error responses. Common error patterns:
- Database/collection not found
- Document not found (for update/delete by ID)
- Schema validation failure
- Duplicate document ID
- Duplicate index name

## Write Consistency Model

All write operations follow this pattern for durability:

```
1. Apply to in-memory state (with mutex lock)
2. Log to WAL synchronously (fsync'd to disk)
3. Mark dirty for background persistence
4. Return success to caller
```

This means:
- Writes are durable immediately (WAL)
- Binary storage is eventually consistent (within 5s)
- On crash, WAL replay restores all committed writes
