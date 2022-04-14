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
	"context"
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/cmd/dependency"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/pkg/dfpath"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	"d7y.io/dragonfly/v2/version"
)

var (
	dfcacheConfig *config.DfcacheConfig
)

var dfcacheDescription = `
dfcache is the cache client to of dragonfly that communicates with dfdaemon and operates
on files in P2P network, where the P2P network acts as a cache system.

The difference between dfcache and dfget is that, dfget downloads file from a given URL,
the file might be on other peers in P2P network or a CDN peer, it's the P2P network's
responsibility to download file from source; but dfcache could only export or download a
file that has been imported or added into P2P network by other peer, it's the user's
responsibility to go back to source and add file into P2P network.
`

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:                "dfcache <command> [flags]",
	Short:              "the P2P cache client of dragonfly",
	Long:               dfcacheDescription,
	Args:               cobra.MaximumNArgs(1),
	DisableAutoGenTag:  true,
	SilenceUsage:       true,
	FParseErrWhitelist: cobra.FParseErrWhitelist{UnknownFlags: true},
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
	// Initialize default dfcache config
	dfcacheConfig = config.NewDfcacheConfig()
	// Initialize cobra
	dependency.InitCobra(rootCmd, false, dfcacheConfig)

	// Bind more cache specific persistent flags
	flags := rootCmd.PersistentFlags()

	flags.StringP("cid", "i", "", "content or cache ID, e.g. sha256 digest of the content")
	flags.StringP("tag", "t", "", "different tags for the same cid will be recognized as different  files in P2P network")
	flags.Duration("timeout", dfcacheConfig.Timeout, "Timeout for this cache operation, 0 is infinite")
	flags.String("callsystem", dfcacheConfig.CallSystem, "The caller name which is mainly used for statistics and access control")
	flags.String("workhome", dfcacheConfig.WorkHome, "Dfcache working directory")
	flags.String("logdir", dfcacheConfig.LogDir, "Dfcache log directory")

	// Bind common flags
	if err := viper.BindPFlags(flags); err != nil {
		panic(errors.Wrap(err, "bind cache common flags to viper"))
	}

	initStat()
	initImport()
	initExport()
	initDelete()
}

func initDfcacheDfpath(cfg *config.CacheOption) (dfpath.Dfpath, error) {
	options := []dfpath.Option{}
	if cfg.WorkHome != "" {
		options = append(options, dfpath.WithWorkHome(cfg.WorkHome))
	}

	if cfg.LogDir != "" {
		options = append(options, dfpath.WithLogDir(cfg.LogDir))
	}

	return dfpath.New(options...)
}

// runDfcache does some init operations and starts to download.
func runDfcacheSubcmd(cmdName string, args []string) error {
	var (
		daemonClient client.DaemonClient
		err          error
	)

	// Initialize daemon dfpath
	d, err := initDfcacheDfpath(dfcacheConfig)
	if err != nil {
		return err
	}

	// Initialize logger
	if err := logger.InitDfcache(dfcacheConfig.Console, d.LogDir()); err != nil {
		return errors.Wrap(err, "init client dfcache logger")
	}
	logger.Infof("Version:\n%s", version.Version())

	// Convert config
	if err := dfcacheConfig.Convert(cmdName, args); err != nil {
		return err
	}

	// Validate config
	if err := dfcacheConfig.Validate(cmdName); err != nil {
		return err
	}

	// Dfcache config values
	s, _ := yaml.Marshal(dfcacheConfig)
	logger.Infof("client dfcache configuration:\n%s", string(s))

	ff := dependency.InitMonitor(dfcacheConfig.PProfPort, dfcacheConfig.Telemetry)
	defer ff()

	if daemonClient, err = checkDaemon(d.DaemonSockPath()); err != nil {
		logger.Errorf("check daemon error: %v", err)
		return err
	}

	runCmd := runStat
	switch cmdName {
	case config.CmdStat:
		runCmd = runStat
	case config.CmdImport:
		runCmd = runImport
	case config.CmdExport:
		runCmd = runExport
	case config.CmdDelete:
		runCmd = runDelete
	default:
		msg := fmt.Sprintf("unknown sub-command %s", cmdName)
		logger.Error(msg)
		return errors.New(msg)
	}

	return runCmd(dfcacheConfig, daemonClient)
}

// checkDaemon checks if daemon is running
func checkDaemon(daemonSockPath string) (client.DaemonClient, error) {
	target := dfnet.NetAddr{Type: dfnet.UNIX, Addr: daemonSockPath}
	daemonClient, err := client.GetClientByAddr([]dfnet.NetAddr{target})
	if err != nil {
		return nil, err
	}

	if daemonClient.CheckHealth(context.Background(), target) == nil {
		return daemonClient, nil
	}
	return nil, errors.New("daemon not running")
}
