# CLI Reference

CachyDB uses [cobra](https://github.com/spf13/cobra) for CLI command handling.

## Commands

### `cachydb` (root)

Start the MCP server with default configuration.

```bash
cachydb [flags]
```

**Flags**:
| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--port` | `-p` | `7601` | HTTP transport port |
| `--root` | `-R` | `~/.cachydb` | Root directory for data |
| `--transport` | `-t` | (env or `stdio`) | Transport: `stdio` or `http` |

---

### `cachydb app`

Same as root command — starts the MCP server. Exists as an explicit subcommand alternative.

```bash
cachydb app [flags]
```

Accepts the same flags as root.

---

### `cachydb version`

Print the application version.

```bash
cachydb version
```

**Version resolution order**:
1. Build-time `-ldflags` injection (`cmd.Version` variable)
2. Go `debug.ReadBuildInfo()` module version
3. Fallback: `v0.0.0-dev`

---

### `cachydb utils`

Parent command for utility subcommands.

```bash
cachydb utils <subcommand>
```

---

### `cachydb utils list`

List databases and their schema versions.

```bash
cachydb utils list [flags]
```

**Flags**:
| Flag | Short | Description |
|------|-------|-------------|
| `--collections` | `-c` | Also list collections within each database |

---

### `cachydb utils migrate`

Run schema migrations on databases.

```bash
cachydb utils migrate [flags]
```

**Flags**:
| Flag | Description |
|------|-------------|
| `--database` | Migrate a specific database |
| `--all` | Migrate all databases |
| `--target` | Target schema version |
| `--show-version` | Show current schema version |
| `--list` | List available migrations |

---

## Configuration

Configuration is loaded from environment variables (via `envconfig`) and can be overridden by CLI flags.

| Environment Variable | Config Field | Default | Description |
|---------------------|--------------|---------|-------------|
| `PORT` | Port | `7601` | Server port for HTTP transport |
| `ROOT_DIR` | RootDir | `~/.cachydb` | Data storage root directory |
| `DB_NAME` | DBName | `main` | Default database name |
| `TRANSPORT` | Transport | `stdio` | Transport type (`stdio` or `http`) |

**Platform note**: On Windows, the default root directory name is `CachyDB` (instead of `.cachydb`).

## Execution Flow

```
main.go
  └── cmd.Execute()
        ├── config.Init()           # Load env vars
        ├── createRootDirIfNotExists()
        └── rootCmd.Execute()       # Cobra dispatch
              └── executeApp()
                    └── app.Builder{}.Build().Start(ctx)
```

## Build & Install

```bash
# Build locally
go build -o cachydb

# Install globally
go install github.com/hop-/cachydb@latest

# Build with version injection
go build -ldflags "-X github.com/hop-/cachydb/internal/cmd.Version=$(git describe --tags --always)" -o cachydb
```
