/*
 *     Copyright 2022 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/cmd/dependency"
)

const (
	// DfstoreScheme if the scheme of object storage.
	DfstoreScheme = "dfs"
)

// Initialize default dfstore config.
var cfg = config.NewDfstore()

var dfstoreDescription = `
dfstore is a storage client for dragonfly. It can rely on different types of object storage,
such as S3 or OSS, to provide stable object storage capabilities.

dfstore uses the entire P2P network as a cache when storing objects.
Rely on S3 or OSS as the backend to ensure storage reliability.
In the process of object storage, P2P Cache is effectively used for fast read and write storage.
`

// rootCmd represents the commonv1 command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:                "dfstore <command> [flags]",
	Short:              "object storage client of dragonfly.",
	Long:               dfstoreDescription,
	Args:               cobra.MaximumNArgs(1),
	DisableAutoGenTag:  true,
	SilenceUsage:       true,
	FParseErrWhitelist: cobra.FParseErrWhitelist{UnknownFlags: true},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	// Bind more cache specific persistent flags.
	flags := rootCmd.PersistentFlags()
	flags.StringVarP(&cfg.Endpoint, "endpoint", "e", cfg.Endpoint, "endpoint of object storage service")

	// Bind common flags.
	if err := viper.BindPFlags(flags); err != nil {
		panic(err)
	}

	// Add sub command.
	rootCmd.AddCommand(copyCmd)
	rootCmd.AddCommand(removeCmd)
	rootCmd.AddCommand(dependency.VersionCmd)
}
