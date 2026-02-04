package cmd

import (
	"github.com/hop-/cachydb/internal/config"
	"github.com/spf13/cobra"
)

// utilsCmd represents the utils command group
var utilsCmd = &cobra.Command{
	Use:   "utils",
	Short: "Utility commands for database maintenance and management",
	Long: `Utility commands for database maintenance and management.
This includes migration, backup, repair, and other administrative tasks.`,
}

func init() {
	// Add root directory flag to utils command (inherited by all subcommands)
	utilsCmd.PersistentFlags().StringVarP(
		&generalRootDir,
		"root", "R",
		config.GetConfig().RootDir,
		"root directory for application data and configurations",
	)

	rootCmd.AddCommand(utilsCmd)
}
