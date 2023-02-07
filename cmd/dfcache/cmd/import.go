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
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/dfcache"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
)

const importDesc = "import file into P2P cache system"

// importCmd represents the cache import command
var importCmd = &cobra.Command{
	Use:                "import <-i cid> <file>|<-I file> [flags]",
	Short:              importDesc,
	Long:               importDesc,
	Args:               cobra.MaximumNArgs(1),
	DisableAutoGenTag:  true,
	SilenceUsage:       true,
	FParseErrWhitelist: cobra.FParseErrWhitelist{UnknownFlags: true},
	RunE: func(cmd *cobra.Command, args []string) error {
		return runDfcacheSubcmd(config.CmdImport, args)
	},
}

func initImport() {
	// Add the command to parent
	rootCmd.AddCommand(importCmd)

	flags := importCmd.Flags()
	flags.StringVarP(&dfcacheConfig.Path, "input", "I", "", "import the given file into P2P network")
	if err := viper.BindPFlags(flags); err != nil {
		panic(fmt.Errorf("bind cache import flags to viper: %w", err))
	}
}

func runImport(cfg *config.DfcacheConfig, client client.V1) error {
	return dfcache.Import(cfg, client)
}
