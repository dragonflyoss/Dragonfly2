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
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/cmd/dependency"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfnet"
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
the file might be on other peers in P2P network or a seed peer, it's the P2P network's
responsibility to download file from source; but dfcache could only export or download a
file that has been imported or added into P2P network by other peer, it's the user's
responsibility to go back to source and add file into P2P network.
`

// rootCmd represents the commonv1 command when called without any subcommands
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
	// Initialize command and config
	dependency.InitCommandAndConfig(rootCmd, false, dfcacheConfig)

	// Bind more cache specific persistent flags
	flags := rootCmd.PersistentFlags()

	flags.StringP("cid", "i", "", "content or cache ID, e.g. sha256 digest of the content")
	flags.StringP("tag", "t", "", "different tags for the same cid will be recognized as different  files in P2P network")
	flags.Duration("timeout", dfcacheConfig.Timeout, "Timeout for this cache operation, 0 is infinite")
	flags.String("workhome", dfcacheConfig.WorkHome, "Dfcache working directory")
	flags.String("logdir", dfcacheConfig.LogDir, "Dfcache log directory")
	flags.String("daemon-sock", dfcacheConfig.DaemonSock, "Dfdaemon socket path to connect")

	// Bind common flags
	if err := viper.BindPFlags(flags); err != nil {
		panic(fmt.Errorf("bind cache common flags to viper: %w", err))
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

	if cfg.DaemonSock != "" {
		options = append(options, dfpath.WithDownloadUnixSocketPath(cfg.DaemonSock))
	}

	return dfpath.New(options...)
}

// runDfcache does some init operations and starts to download.
func runDfcacheSubcmd(cmdName string, args []string) error {
	// Convert config
	if err := dfcacheConfig.Convert(cmdName, args); err != nil {
		return err
	}

	// Validate config
	if err := dfcacheConfig.Validate(cmdName); err != nil {
		return err
	}

	var (
		dfdaemonClient client.V1
		err            error
	)

	// Initialize daemon dfpath
	d, err := initDfcacheDfpath(dfcacheConfig)
	if err != nil {
		return err
	}

	rotateConfig := logger.LogRotateConfig{
		MaxSize:    dfcacheConfig.LogMaxSize,
		MaxAge:     dfcacheConfig.LogMaxAge,
		MaxBackups: dfcacheConfig.LogMaxBackups}

	// Initialize logger
	if err := logger.InitDfcache(dfcacheConfig.Console, d.LogDir(), rotateConfig); err != nil {
		return fmt.Errorf("init client dfcache logger: %w", err)
	}
	logger.Infof("version:\n%s", version.Version())

	ff := dependency.InitMonitor(dfcacheConfig.PProfPort, dfcacheConfig.Telemetry)
	defer ff()

	if dfdaemonClient, err = checkDaemon(d.DaemonSockPath()); err != nil {
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

	return runCmd(dfcacheConfig, dfdaemonClient)
}

// checkDaemon checks if daemon is running
func checkDaemon(daemonSockPath string) (client.V1, error) {
	netAddr := &dfnet.NetAddr{Type: dfnet.UNIX, Addr: daemonSockPath}
	dfdaemonClient, err := client.GetInsecureV1(context.Background(), netAddr.String())
	if err != nil {
		return nil, err
	}

	if dfdaemonClient.CheckHealth(context.Background()) == nil {
		return dfdaemonClient, nil
	}
	return nil, errors.New("daemon not running")
}
