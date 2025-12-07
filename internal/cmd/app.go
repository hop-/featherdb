package cmd

import (
	"github.com/hop-/featherdb/internal/app"
	"github.com/hop-/featherdb/internal/config"
	"github.com/spf13/cobra"
)

var appCmd = &cobra.Command{
	Use:   "app",
	Short: "Run the application (same as default)",
	Run: func(cmd *cobra.Command, args []string) {
		executeApp()
	},
}

// autorun: This function is called automatically to initialize the command
func init() {
	// Flags for app command
	setAllFlagsToCmd(appCmd)
}

func setAllFlagsToCmd(cmd *cobra.Command) {
	cmd.Flags().IntVarP(
		&generalServerPort,
		"port", "p",
		config.GetConfig().Port,
		"port on which connection listener will be started",
	)
	cmd.Flags().StringVarP(
		&generalRootDir,
		"root", "R",
		config.GetConfig().RootDir,
		"root directory for application data and configurations",
	)
}

func executeApp() {
	application, err := buildApp()
	if err != nil {
		// TODO: handle error appropriately
		return
	}

	err = application.Start()
	if err != nil {
		// TODO: handle error appropriately
		return
	}

	defer func() {
		err := application.Stop()
		if err != nil {
			// TODO: handle error appropriately
		}
	}()
}

func buildApp() (*app.App, error) {
	builder := app.NewBuilder()

	return builder.Build()
}
