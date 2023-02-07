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

const exportDesc = "export file from P2P cache system"

// exportCmd represents the cache export command
var exportCmd = &cobra.Command{
	Use:                "export <-i cid> <output>|<-O output> [flags]",
	Short:              exportDesc,
	Long:               exportDesc,
	Args:               cobra.MaximumNArgs(1),
	DisableAutoGenTag:  true,
	SilenceUsage:       true,
	FParseErrWhitelist: cobra.FParseErrWhitelist{UnknownFlags: true},
	RunE: func(cmd *cobra.Command, args []string) error {
		return runDfcacheSubcmd(config.CmdExport, args)
	},
}

func initExport() {
	// Add the command to parent
	rootCmd.AddCommand(exportCmd)

	flags := exportCmd.Flags()
	flags.StringVarP(&dfcacheConfig.Output, "output", "O", "", "export file path")
	flags.BoolVarP(&dfcacheConfig.LocalOnly, "local", "l", false, "only export file from local cache")
	if err := viper.BindPFlags(flags); err != nil {
		panic(fmt.Errorf("bind cache export flags to viper: %w", err))
	}
}

func runExport(cfg *config.DfcacheConfig, client client.V1) error {
	return dfcache.Export(cfg, client)
}
