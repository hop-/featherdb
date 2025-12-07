package cmd

var (
	Version           = "" // This will be set during build time using -ldflags "-X github.com/hop-/featherdb/internal/cmd.Version=$(git describe --tags --always)"
	defaultVersion    = "v0.0.0-dev"
	generalRootDir    string
	generalServerPort int
)
