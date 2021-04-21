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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"d7y.io/dragonfly/v2/cmd/common"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/dflog/logcore"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/server"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string
	cfg     *config.Config
)

const (
	SchedulerEnvPrefix = "scheduler"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "scheduler",
	Short: "P2P scheduler",
	Long: `scheduler is a long-running process and is mainly responsible for 
deciding which peers transmit blocks to each other.`,
	Args:              cobra.NoArgs,
	DisableAutoGenTag: true,
	SilenceUsage:      true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := logcore.InitScheduler(cfg.Console); err != nil {
			return errors.Wrap(err, "init scheduler logger")
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
	cobra.OnInitialize(initConfig)

	// Add flags
	flagSet := rootCmd.Flags()
	flagSet.BoolVar(&cfg.Console, "console", cfg.Console, "whether print log info on the terminal")
	flagSet.BoolVar(&cfg.Verbose, "verbose", cfg.Verbose, "whether use debug level logger and enable pprof")
	flagSet.IntVar(&cfg.PProfPort, "pprofPort", cfg.PProfPort, "listen port for pprof, only valid when the verbose option is true, default is random port")
	flagSet.IntVarP(&cfg.Server.Port, "port", "p", cfg.Server.Port, "port is the port that scheduler server listens on")
	flagSet.StringVarP(&cfgFile, "config", "f", "", "the path of scheduler config file")

	common.AddCommonSubCmds(rootCmd)
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		viper.AddConfigPath(filepath.Dir(config.DefaultConfigFilePath))
		viper.SetConfigFile(filepath.Base(config.DefaultConfigFilePath))
	}

	viper.SetEnvPrefix(SchedulerEnvPrefix)
	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}

	if err := viper.Unmarshal(&cfg); err != nil {
		panic(errors.Wrap(err, "unmarshal config to struct"))
	}
}

func runScheduler() error {
	// scheduler config values
	s, _ := json.MarshalIndent(cfg, "", "\t")
	logger.Debugf("scheduler configuration: %s", string(s))

	// initialize verbose mode
	common.InitVerboseMode(cfg.Verbose, cfg.PProfPort)

	svr := server.New(cfg)

	return svr.Serve()
}
