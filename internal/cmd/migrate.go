package cmd

import (
	"fmt"

	"github.com/hop-/cachydb/pkg/db"
	"github.com/spf13/cobra"
)

// migrateCmd represents the migrate command
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate databases to a specific schema version",
	Long: `Migrate databases from their current schema version to a target version.
The migration system applies schema updates iteratively.

Each migration step is registered in code and can perform schema transformations,
data migrations, or any other necessary updates.`,
	RunE: runMigrate,
}

var (
	migrateDatabase string
	migrateAll      bool
	targetVersion   int
	showVersion     bool
	listMigrations  bool
)

func init() {
	utilsCmd.AddCommand(migrateCmd)

	migrateCmd.Flags().StringVarP(&migrateDatabase, "database", "d", "", "Database name to migrate")
	migrateCmd.Flags().BoolVarP(&migrateAll, "all", "a", false, "Migrate all databases")
	migrateCmd.Flags().IntVarP(&targetVersion, "target", "t", db.CurrentSchemaVersion, "Target schema version (default: latest)")
	migrateCmd.Flags().BoolVarP(&showVersion, "show-version", "v", false, "Show current schema version of database")
	migrateCmd.Flags().BoolVarP(&listMigrations, "list", "l", false, "List all registered migrations")
}

func runMigrate(cmd *cobra.Command, args []string) error {
	// List migrations (doesn't require storage)
	if listMigrations {
		migrator := db.NewMigrationManager(nil)
		versions := migrator.ListMigrations()
		if len(versions) == 0 {
			fmt.Println("No migrations registered yet")
			return nil
		}
		fmt.Println("Registered migrations:")
		for _, version := range versions {
			fmt.Printf("  Version %d -> %d\n", version, version+1)
		}
		fmt.Printf("\nCurrent schema version: %d\n", db.CurrentSchemaVersion)
		return nil
	}

	// Show version requires database parameter
	if showVersion {
		if migrateDatabase == "" {
			return fmt.Errorf("--database is required when using --show-version. Use 'cachydb utils list' to see available databases")
		}
	}

	// Validate flags for migration operations
	if !showVersion && !migrateAll && migrateDatabase == "" {
		return fmt.Errorf("either --database or --all must be specified. Use 'cachydb utils list' to see available databases")
	}

	if migrateAll && migrateDatabase != "" {
		return fmt.Errorf("cannot specify both --database and --all")
	}

	// Create storage manager
	storage, err := db.NewStorageManager(generalRootDir)
	if err != nil {
		return fmt.Errorf("failed to create storage manager: %w", err)
	}
	defer storage.Close()

	migrator := db.NewMigrationManager(storage)

	// Show version
	if showVersion {
		version, err := migrator.GetDatabaseVersion(migrateDatabase)
		if err != nil {
			return fmt.Errorf("failed to get database version: %w", err)
		}
		fmt.Printf("Database '%s' is at schema version %d\n", migrateDatabase, version)
		fmt.Printf("Current schema version: %d\n", db.CurrentSchemaVersion)
		return nil
	}

	// Migrate all databases
	if migrateAll {
		fmt.Printf("Migrating all databases to version %d...\n", targetVersion)
		if err := migrator.MigrateAllDatabases(targetVersion); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
		fmt.Println("All databases migrated successfully!")
		return nil
	}

	// Migrate single database
	if err := migrator.MigrateDatabase(migrateDatabase, targetVersion); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	fmt.Printf("Database '%s' migrated successfully!\n", migrateDatabase)
	return nil
}
