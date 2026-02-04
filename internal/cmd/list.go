package cmd

import (
	"fmt"

	"github.com/hop-/cachydb/pkg/db"
	"github.com/spf13/cobra"
)

// listCmd represents the list command
var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List databases and their schema versions",
	Long: `List all available databases in the data directory along with their schema versions.
This command helps you see what databases exist and their current state.`,
	RunE: runList,
}

var (
	showCollections bool
)

func init() {
	utilsCmd.AddCommand(listCmd)

	listCmd.Flags().BoolVarP(&showCollections, "collections", "c", false, "Show collections for each database")
}

func runList(cmd *cobra.Command, args []string) error {
	storage, err := db.NewStorageManager(generalRootDir)
	if err != nil {
		return fmt.Errorf("failed to create storage manager: %w", err)
	}
	defer storage.Close()

	dbManager, err := storage.LoadAllDatabases()
	if err != nil {
		return fmt.Errorf("failed to load databases: %w", err)
	}

	databases := dbManager.ListDatabases()
	if len(databases) == 0 {
		fmt.Println("No databases found")
		return nil
	}

	fmt.Printf("Found %d database(s):\n\n", len(databases))
	for _, dbName := range databases {
		database := dbManager.GetDatabase(dbName)
		if database != nil {
			fmt.Printf("  %s (schema version: %d)\n", dbName, database.SchemaVersion)

			if showCollections {
				collections := database.ListCollections()
				if len(collections) > 0 {
					for _, collName := range collections {
						coll, err := database.GetCollection(collName)
						if err == nil {
							docCount := len(coll.Documents)
							fmt.Printf("    └─ %s (%d documents)\n", collName, docCount)
						}
					}
				} else {
					fmt.Printf("    └─ (no collections)\n")
				}
			}
		} else {
			fmt.Printf("  %s\n", dbName)
		}
	}

	return nil
}
