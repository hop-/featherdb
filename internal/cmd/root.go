package cmd

import (
	"os"

	"github.com/hop-/featherdb/internal/config"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "gotchat",
		Short: "A simple chat application",
		Long:  `A simple terminal based chat application built with Go.`,
		Run: func(cmd *cobra.Command, args []string) {
			executeApp()
		},
	}
)

// autorun: This function is called automatically to initialize the root command
func init() {
	// Flags for root command
	setAllFlagsToCmd(rootCmd)

	// Add subcommands
	rootCmd.AddCommand(appCmd)
	rootCmd.AddCommand(versionCmd)
}

func Execute() {
	config.Init()

	err := createRootDirIfNotExists()
	if err != nil {
		// TODO: handle error properly
		panic(err)
	}
	cobra.CheckErr(rootCmd.Execute())
}

func createRootDirIfNotExists() error {
	rootDir := config.GetConfig().RootDir

	return os.MkdirAll(rootDir, 0755)
}
