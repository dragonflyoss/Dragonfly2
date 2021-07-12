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

	"d7y.io/dragonfly.v2/cmd/dependency"
	logger "d7y.io/dragonfly.v2/internal/dflog"
	"d7y.io/dragonfly.v2/internal/dflog/logcore"
	"d7y.io/dragonfly.v2/scheduler/config"
	"d7y.io/dragonfly.v2/scheduler/server"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var (
	cfg *config.Config
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "scheduler",
	Short: "the scheduler of dragonfly",
	Long: `scheduler is a long-running process and is mainly responsible 
for deciding which peers transmit blocks to each other.`,
	Args:              cobra.NoArgs,
	DisableAutoGenTag: true,
	SilenceUsage:      true,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Initialize logger
		if err := logcore.InitScheduler(cfg.Console); err != nil {
			return errors.Wrap(err, "init scheduler logger")
		}

		// Validate config
		if err := cfg.Validate(); err != nil {
			return err
		}

		return runScheduler()
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
	// Initialize default scheduler config
	cfg = config.New()
	// Initialize cobra
	dependency.InitCobra(rootCmd, true, cfg)
}

func runScheduler() error {
	// scheduler config values
	s, _ := yaml.Marshal(cfg)
	logger.Infof("scheduler configuration:\n%s", string(s))

	ff := dependency.InitMonitor(cfg.Verbose, cfg.PProfPort, cfg.Telemetry.Jaeger)
	defer ff()

	svr, err := server.New(cfg)
	if err != nil {
		logger.Errorf("get scheduler server error: %s", err)
		return err
	}

	return svr.Serve()
}
