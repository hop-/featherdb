package db

import (
	"fmt"
	"sync"
)

// CurrentSchemaVersion is the latest schema version
const CurrentSchemaVersion = 1

// MigrationFunc is a function that migrates from one version to the next
type MigrationFunc func(dbManager *DatabaseManager, storage *StorageManager) error

// Example of registering a migration:
//
// func init() {
//     // Migration from version 1 to version 2
//     db.RegisterMigration(1, func(dbManager *db.DatabaseManager, storage *db.StorageManager) error {
//         fmt.Println("Running migration 1 -> 2")
//         // Iterate through all databases
//         for _, database := range dbManager.Databases {
//             // Perform migration operations on each database
//             // Example: Add a new field, transform data, etc.
//         }
//         return nil
//     })
//
//     // Migration from version 2 to version 3
//     db.RegisterMigration(2, func(dbManager *db.DatabaseManager, storage *db.StorageManager) error {
//         fmt.Println("Running migration 2 -> 3")
//         // Perform version 2 to 3 migration
//         return nil
//     })
// }

// MigrationRegistry holds all registered migration functions
type MigrationRegistry struct {
	migrations map[int]MigrationFunc // maps from_version -> migration function to reach from_version+1
	mu         sync.RWMutex
}

var globalRegistry = &MigrationRegistry{
	migrations: make(map[int]MigrationFunc),
}

// RegisterMigration registers a migration function for a specific version transition
// fromVersion -> toVersion (toVersion must be fromVersion + 1)
func RegisterMigration(fromVersion int, migrationFunc MigrationFunc) {
	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()
	globalRegistry.migrations[fromVersion] = migrationFunc
}

// GetMigration retrieves a migration function for a specific version
func GetMigration(fromVersion int) (MigrationFunc, bool) {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()
	fn, exists := globalRegistry.migrations[fromVersion]
	return fn, exists
}

// MigrationManager handles database schema migrations
type MigrationManager struct {
	storage *StorageManager
}

// NewMigrationManager creates a new migration manager
func NewMigrationManager(storage *StorageManager) *MigrationManager {
	return &MigrationManager{
		storage: storage,
	}
}

// MigrateDatabase migrates a database from its current version to the target version
func (mm *MigrationManager) MigrateDatabase(dbName string, targetVersion int) error {
	fmt.Printf("Starting migration for database '%s'...\n", dbName)

	// Load database
	db, err := mm.storage.LoadDatabase(dbName)
	if err != nil {
		return fmt.Errorf("failed to load database: %w", err)
	}

	currentVersion := db.SchemaVersion
	if currentVersion == 0 {
		// Default to version 1 for databases without version info
		currentVersion = 1
		db.SchemaVersion = 1
	}

	fmt.Printf("Database '%s' is at version %d, target version: %d\n", dbName, currentVersion, targetVersion)

	if currentVersion == targetVersion {
		fmt.Printf("Database '%s' is already at version %d, no migration needed\n", dbName, targetVersion)
		return nil
	}

	if currentVersion > targetVersion {
		return fmt.Errorf("cannot downgrade database from version %d to %d", currentVersion, targetVersion)
	}

	// Create a temporary DatabaseManager with just this database
	dbManager := NewDatabaseManager()
	dbManager.Databases[dbName] = db

	// Apply migrations iteratively from currentVersion to targetVersion
	for version := currentVersion; version < targetVersion; version++ {
		fmt.Printf("Applying migration from version %d to %d...\n", version, version+1)

		migrationFunc, exists := GetMigration(version)
		if !exists {
			return fmt.Errorf("no migration found from version %d to %d", version, version+1)
		}

		if err := migrationFunc(dbManager, mm.storage); err != nil {
			return fmt.Errorf("migration from version %d to %d failed: %w", version, version+1, err)
		}

		// Update version
		db.SchemaVersion = version + 1

		// Save database with new version
		if err := mm.storage.SaveDatabase(db); err != nil {
			return fmt.Errorf("failed to save database after migration to version %d: %w", version+1, err)
		}

		fmt.Printf("Successfully migrated to version %d\n", version+1)
	}

	fmt.Printf("Database '%s' successfully migrated to version %d\n", dbName, targetVersion)
	return nil
}

// MigrateAllDatabases migrates all databases to the target version
func (mm *MigrationManager) MigrateAllDatabases(targetVersion int) error {
	fmt.Printf("Starting migration of all databases to version %d...\n", targetVersion)

	// Load all databases
	dbManager, err := mm.storage.LoadAllDatabases()
	if err != nil {
		return fmt.Errorf("failed to load databases: %w", err)
	}

	migratedCount := 0
	for dbName := range dbManager.Databases {
		if err := mm.MigrateDatabase(dbName, targetVersion); err != nil {
			return fmt.Errorf("failed to migrate database '%s': %w", dbName, err)
		}
		migratedCount++
	}

	fmt.Printf("Successfully migrated %d database(s) to version %d\n", migratedCount, targetVersion)
	return nil
}

// GetDatabaseVersion returns the schema version of a database
func (mm *MigrationManager) GetDatabaseVersion(dbName string) (int, error) {
	db, err := mm.storage.LoadDatabase(dbName)
	if err != nil {
		return 0, fmt.Errorf("failed to load database: %w", err)
	}

	version := db.SchemaVersion
	if version == 0 {
		// Default to version 1 for databases without version info
		version = 1
	}

	return version, nil
}

// ListMigrations returns a list of all registered migrations
func (mm *MigrationManager) ListMigrations() []int {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()

	versions := make([]int, 0, len(globalRegistry.migrations))
	for version := range globalRegistry.migrations {
		versions = append(versions, version)
	}

	return versions
}
