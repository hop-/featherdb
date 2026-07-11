# Database Engine Reference

The database engine lives in `pkg/db/` and is usable as a standalone Go library.

## Core Types

### Document

```go
type Document struct {
    ID   string         `json:"_id"`
    Data map[string]any `json:"data"`
}
```

Documents are the fundamental unit of storage. Each has a unique `_id` (auto-generated UUID if not provided) and a flexible `Data` map holding arbitrary JSON-compatible values.

**Custom JSON marshaling**: Documents serialize as a flat JSON object with `_id` at the top level alongside data fields (not nested under a `data` key).

### Collection

```go
type Collection struct {
    Name      string
    Schema    *Schema              // Optional schema for validation
    Documents map[string]*Document // ID → Document (in-memory)
    Indexes   map[string]*Index    // Index name → Index
}
```

### Database

```go
type Database struct {
    Name          string
    SchemaVersion int                    // For migration system
    Collections   map[string]*Collection
}
```

### DatabaseManager

```go
type DatabaseManager struct {
    Databases map[string]*Database
}
```

Top-level container managing all databases. Thread-safe for concurrent access.

---

## Query Engine (`query.go`)

### Collection Operations

| Method | Description |
|--------|-------------|
| `Insert(doc *Document) error` | Insert document (validates schema, updates indexes) |
| `FindByID(id string) (*Document, error)` | Find single document by ID |
| `Find(query *Query) ([]*Document, error)` | Find documents matching filters |
| `Update(id string, updates map[string]any) error` | Update document fields by ID |
| `Delete(id string) error` | Delete document by ID |
| `Count() int` | Return document count |

### Database Operations

| Method | Description |
|--------|-------------|
| `CreateDatabase(name string) *Database` | Create new database |
| `GetDatabase(name string) *Database` | Get database by name |
| `DeleteDatabase(name string) error` | Delete database |
| `ListDatabases() []string` | List all database names |
| `CreateCollection(name string, schema *Schema) error` | Create collection |
| `DropCollection(name string) error` | Drop collection |
| `GetCollection(name string) (*Collection, error)` | Get collection by name |
| `ListCollections() []string` | List collection names |

### Query Filters

```go
type QueryFilter struct {
    Field    string // Field path (supports dot notation)
    Operator string // "eq", "ne", "gt", "lt", "gte", "lte", "in"
    Value    any    // Comparison value
}

type Query struct {
    Filters []QueryFilter
    Limit   int  // 0 = no limit
    Skip    int  // Number of results to skip
}
```

**Index acceleration**: Only `eq` operator on the first filter can leverage an index. If an index exists for the filter's field, it performs an O(1) hash lookup instead of a full collection scan.

---

## Schema Validation (`schema.go`)

### Field Types

| Type | Go types accepted |
|------|-------------------|
| `string` | `string` |
| `number` | `float64`, `int`, `int64`, `float32` |
| `boolean` | `bool` |
| `object` | `map[string]any` |
| `array` | `[]any` |
| `date` | `string` (ISO format expected) |

### Schema Definition

```go
type Schema struct {
    Fields map[string]Field `json:"fields"`
}

type Field struct {
    Type     FieldType `json:"type"`     // Required: field type
    Required bool      `json:"required"` // If true, field must exist in document
}
```

### Validation Rules

- If a field is marked `Required` and missing from the document, insertion/update fails.
- If a field is present, its value must match the declared type.
- The field name `_id` is reserved and cannot be used in schemas.
- A schema must have at least one field.
- Validation runs on both `Insert` and `Update` operations.

---

## Indexing (`index.go`)

### Hash-Based Index

```go
type Index struct {
    Name      string            // Index name
    FieldName string            // Field this index covers
    Data      map[string]string // field_value → document_id (1:1 mapping)
}
```

**Important**: Current implementation maps each unique field value to exactly one document ID. This means indexes work best for unique or near-unique fields. If multiple documents share the same field value, only the last-indexed one is accessible via the index.

### Index API

```go
// Create an index on a collection field
func (c *Collection) CreateIndex(indexName, fieldName string) error

// Drop an index
func (c *Collection) DropIndex(indexName string) error

// Look up a document ID by field value
func (idx *Index) Find(value any) (string, bool)

// Persist index to disk
func (idx *Index) SaveToDisk(dataDir, dbName, collName string) error

// Load index from disk
func LoadIndexFromDisk(dataDir, dbName, collName, indexName string) (*Index, error)
func LoadAllIndexes(dataDir, dbName, collName string) (map[string]*Index, error)
```

### Index Persistence

Indexes are saved as JSON files at: `<rootDir>/<dbName>/<collName>/indexes/<indexName>.json`

### Index Maintenance

- On `Insert`: Index is updated with the new document's field value.
- On `Update`: Old value is removed from index, new value is added.
- On `Delete`: Document's value is removed from index.

---

## Storage System

### Binary Storage Format (`binary_storage.go`)

The default storage format uses a custom binary layout:

```
┌─────────────── collection.data ──────────────────┐
│ Header (8 bytes)                                  │
│   Magic: 0x43414348 ("CACH") - 4 bytes           │
│   Version: uint16 - 2 bytes                      │
│   Flags: uint16 - 2 bytes (bit 0 = compressed)   │
├───────────────────────────────────────────────────┤
│ Document Entry 1                                  │
│   Entry Header (20 bytes):                        │
│     Offset: int64 (8 bytes)                       │
│     Size: uint32 (original size, 4 bytes)         │
│     CompressedSize: uint32 (4 bytes)              │
│     Checksum: uint32 (CRC32, 4 bytes)             │
│   Data: gzip-compressed JSON (variable)           │
├───────────────────────────────────────────────────┤
│ Document Entry 2 ...                              │
└───────────────────────────────────────────────────┘

┌─────────────── collection.idx ───────────────────┐
│ Binary offset index                               │
│   Maps document IDs → file offsets                │
│   Enables O(1) random-access reads                │
└───────────────────────────────────────────────────┘
```

**Constants**:
- `CollectionMagic = 0x43414348`
- `BinaryFormatVersion = 1`
- `HeaderSize = 8` bytes
- `DocEntryHeaderSize = 20` bytes

### Storage Manager (`storage.go`)

Orchestrates all persistence with background sync:

```go
const StorageSyncInterval = 5 * time.Second
```

**Lifecycle**:
1. `NewStorageManager(rootDir)` — initializes WAL, creates directories
2. `LoadAllDatabases()` — reads persisted data + replays WAL
3. `StartBackgroundSync(dbManager)` — starts 5s periodic syncer
4. On each sync cycle: saves dirty collections → creates checkpoint
5. `Stop()` — final sync + cleanup

**Dirty tracking**: When a collection is modified, `MarkDirty(db, collection)` records it. The background syncer picks up all dirty entries each cycle.

### Write-Ahead Log (`wal.go`)

Provides crash-recovery durability:

| Constant | Value |
|----------|-------|
| `WALMaxSize` | 64 MB |
| `WALRetentionCount` | 2 (files kept) |
| `WALBatchSize` | 100 entries |
| `WALFlushInterval` | 100ms |

**WAL Entry Format** (on disk):
```
[length: 4 bytes][checksum: 4 bytes][JSON-encoded WALEntry]
```

**Operations logged**:
- `insert` — document insertion
- `update` — document update
- `delete` — document deletion
- `create_database` — database creation
- `delete_database` — database deletion
- `create_collection` — collection creation
- `delete_collection` — collection deletion
- `create_index` — index creation

**Key APIs**:
```go
func NewWALManager(rootDir string) (*WALManager, error)
func (wm *WALManager) AppendEntry(entry *WALEntry) error      // Batched (async)
func (wm *WALManager) AppendEntrySync(entry *WALEntry) error  // Immediate flush + fsync
func (wm *WALManager) Replay(dm *DatabaseManager, storage *StorageManager) error
func (wm *WALManager) Checkpoint(offset uint64) error
```

**Recovery**: `Replay()` reads all WAL entries after the last checkpoint offset and re-applies them to the in-memory state.

**File rotation**: When a WAL file exceeds 64MB, a new file is created. Old files beyond `WALRetentionCount` are deleted.

### Compression (`compression.go`)

```go
func Compress(data []byte) ([]byte, error)   // gzip compression
func Decompress(data []byte) ([]byte, error) // gzip decompression
```

Used by binary storage to compress document data before writing to disk.

---

## Migration System (`migration.go`)

```go
const CurrentSchemaVersion = 1
```

### Registration

```go
type MigrationFunc func(db *Database) error

func RegisterMigration(fromVersion int, migrationFunc MigrationFunc)
```

Migrations are registered globally, keyed by `fromVersion`. Each function upgrades a database from `fromVersion` to `fromVersion + 1`.

### Execution

```go
func NewMigrationManager(storage *StorageManager) *MigrationManager
func (mm *MigrationManager) MigrateDatabase(dbName string, targetVersion int) error
func (mm *MigrationManager) MigrateAllDatabases(targetVersion int) error
```

Migrations are applied iteratively: if a database is at version 0 and target is 3, it applies migration 0→1, then 1→2, then 2→3.

---

## On-Disk Layout

```
~/.cachydb/                          # Root directory (configurable)
├── wal-<timestamp>.log              # Write-ahead log files
├── wal.checkpoint                   # Last checkpoint offset
├── <database_name>/
│   ├── metadata.json                # Database metadata (schema version)
│   └── <collection_name>/
│       ├── collection.data          # Binary document storage
│       ├── collection.idx           # Binary offset index
│       └── indexes/
│           └── <index_name>.json    # Persisted index data
```
