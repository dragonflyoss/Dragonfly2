package version

import (
	"fmt"
	"github.com/spf13/cobra"
)

var version = "0.0.0"
var revision = "0.0.0"
var buildDate = "00000000-00:00:00"


var VersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of dragonfly",
	Long:  `Print the version number of dragonfly`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("version: %s \nreversion: %s \nbuildDate: %s\n",
			version, revision, buildDate)
	},
}