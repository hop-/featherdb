package cmd

import (
	"fmt"
	"runtime/debug"

	"github.com/spf13/cobra"
)

var (
	versionCmd     = &cobra.Command{
		Use:   "version",
		Short: "Show the application version",
		Long:  `Display the current version of the application.`,
		Run: func(cmd *cobra.Command, args []string) {
			showApplicationVersion()
		},
	}
)

func init() {
	// No flags for version command
}

func showApplicationVersion() {
	fmt.Printf("featherdb version %s\n", getVersion())
}

func getVersion() string {
	if Version != "" {
		return Version
	}

	info, ok := debug.ReadBuildInfo()
	if ok && info.Main.Version != "(devel)" && info.Main.Version != "" {
		return info.Main.Version
	}

	return defaultVersion
}
