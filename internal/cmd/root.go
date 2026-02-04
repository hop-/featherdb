package cmd

import (
	"os"

	"github.com/hop-/cachydb/internal/config"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "cachydb",
		Short: "A lightweight document-based database with MCP support",
		Long:  `CachyDB is a lightweight document-based database similar to MongoDB, with Model Context Protocol (MCP) support for AI integration.`,
		Run: func(cmd *cobra.Command, args []string) {
			executeApp()
		},
	}
)

// autorun: This function is called automatically to initialize the root command
func init() {
	config.Init()

	// Flags for root command
	setAllFlagsToCmd(rootCmd)
}

func Execute() {

	err := createRootDirIfNotExists()
	if err != nil {
		// TODO: handle error properly
		panic(err)
	}

	// Execute the root command
	cobra.CheckErr(rootCmd.Execute())
}

func createRootDirIfNotExists() error {
	rootDir := config.GetConfig().RootDir

	return os.MkdirAll(rootDir, 0755)
}
