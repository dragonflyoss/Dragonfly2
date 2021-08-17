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

	"d7y.io/dragonfly/v2/cmd/dependency"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dflog/logcore"
	"d7y.io/dragonfly/v2/manager"
	"d7y.io/dragonfly/v2/manager/config"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

var (
	cfg *config.Config
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "manager",
	Short: "The central manager of dragonfly.",
	Long: `manager is a long-running process and is mainly responsible 
for managing schedulers and cdns, offering http apis and portal, etc.`,
	Args:              cobra.NoArgs,
	DisableAutoGenTag: true,
	SilenceUsage:      true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := logcore.InitManager(cfg.Console); err != nil {
			return errors.Wrap(err, "init manager logger")
		}

		// Validate config
		if err := cfg.Validate(); err != nil {
			return err
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
	// Initialize default manager config
	cfg = config.New()

	// Initialize cobra
	dependency.InitCobra(rootCmd, true, cfg)

	// Add flags
	flagSet := rootCmd.Flags()
	flagSet.StringP("public-path", "p", cfg.Server.PublicPath,
		"Console dist path")

	// Bind cmd flags
	if err := viper.BindPFlags(flagSet); err != nil {
		panic(errors.Wrap(err, "bind dfget flags to viper"))
	}
}

func runManager() error {
	// manager config values
	s, err := yaml.Marshal(cfg)

	if err != nil {
		return err
	}

	logger.Infof("manager configuration:\n%s", string(s))

	ff := dependency.InitMonitor(cfg.Verbose, cfg.PProfPort, cfg.Telemetry)
	defer ff()

	svr, err := manager.New(cfg)
	if err != nil {
		return err
	}
	return svr.Serve()
}
