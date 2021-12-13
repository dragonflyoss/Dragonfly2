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
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"d7y.io/dragonfly/v2/cdn"
	"d7y.io/dragonfly/v2/cdn/config"
	"d7y.io/dragonfly/v2/cmd/dependency"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dflog/logcore"
	"d7y.io/dragonfly/v2/internal/dfpath"
	"d7y.io/dragonfly/v2/version"
)

var (
	deprecatedConfig *config.DeprecatedConfig
	cfg              *config.Config
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "cdn",
	Short: "the cdn system of dragonfly",
	Long: `cdn system is a long-running process and is mainly responsible
for caching downloaded data to avoid downloading the same files
from remote source repeatedly.`,
	Args:              cobra.NoArgs,
	DisableAutoGenTag: true,
	SilenceUsage:      true,
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg = deprecatedConfig.Convert()
		// Initialize dfpath
		d, err := initDfpath(cfg.LogDir)
		if err != nil {
			return err
		}

		// Initialize logger
		if err := logcore.InitCdnSystem(cfg.Console, d.LogDir()); err != nil {
			return errors.Wrap(err, "init cdn system logger")
		}

		return runCdnSystem()
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
	// Initialize default cdn system config
	deprecatedConfig = config.NewDeprecatedConfig()
	// Initialize cobra
	dependency.InitCobra(rootCmd, true, deprecatedConfig)
}

func initDfpath(logDir string) (dfpath.Dfpath, error) {
	options := []dfpath.Option{}
	if logDir != "" {
		options = append(options, dfpath.WithLogDir(logDir))
	}

	return dfpath.New(options...)
}

func runCdnSystem() error {
	logger.Infof("Version:\n%s", version.Version())
	// validate config
	if errs := cfg.Validate(); len(errs) != 0 {
		return fmt.Errorf("failed to validate cdn config:\n%s", errs)
	}
	// cdn system config values
	logger.Infof("cdn system configuration:\n%s", cfg)

	ff := dependency.InitMonitor(cfg.Verbose, cfg.PProfPort, cfg.Telemetry)
	defer ff()

	svr, err := cdn.New(cfg)
	if err != nil {
		return err
	}

	dependency.SetupQuitSignalHandler(func() { logger.Fatalf("stop server failed: %v", svr.Stop()) })
	return svr.Serve()
}
