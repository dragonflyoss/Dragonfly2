/*
 *     Copyright 2020 The Dragonfly Authors
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

	"d7y.io/dragonfly/v2/cmd/common"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/server"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/dflog/logcore"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var (
	cfgFile = config.DefaultConfigPath
	// Initialize default manager config
	cfg = config.New()
)

const (
	managerEnvPrefix = "manager"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "manager",
	Short: "the central manager of dragonfly",
	Long: `manager is a long-running process and is mainly responsible 
for managing schedulers and cdns, offering http apis and portal, etc.`,
	Args:              cobra.NoArgs,
	DisableAutoGenTag: true,
	SilenceUsage:      true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := logcore.InitManager(cfg.Console); err != nil {
			return errors.Wrap(err, "init manager logger")
		}

		if err := common.InitConfig(cmd, managerEnvPrefix, cfg); err != nil {
			return errors.Wrapf(err, "init cdn manager config")
		}
		return runManager()
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		logger.Error(err)
		os.Exit(1)
	}
}

func init() {

	// Initialize cobra
	common.SetupFlags(rootCmd, cfgFile)
}

func runManager() error {
	// manager config values
	s, _ := yaml.Marshal(cfg)
	logger.Infof("manager configuration:\n%s", string(s))

	// initialize verbose mode
	common.InitVerboseMode(cfg.Verbose, cfg.PProfPort)

	if svr, err := server.New(cfg); err != nil {
		return err
	} else {
		return svr.Serve()
	}
}
