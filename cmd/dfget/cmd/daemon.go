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
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/gofrs/flock"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"d7y.io/dragonfly/v2/client/config"
	server "d7y.io/dragonfly/v2/client/daemon"
	"d7y.io/dragonfly/v2/cmd/dependency"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/dfpath"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/version"
)

var (
	cfg *config.DaemonConfig
)

// daemonCmd represents the daemon command
var daemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "start the client daemon of dragonfly",
	Long: `client daemon is mainly responsible for transmitting blocks between peers 
and putting the completed file into the specified target path. at the same time, 
it supports container engine, wget and other downloading tools through proxy function.`,
	Args:               cobra.NoArgs,
	DisableAutoGenTag:  true,
	SilenceUsage:       true,
	FParseErrWhitelist: cobra.FParseErrWhitelist{UnknownFlags: true},
	RunE: func(cmd *cobra.Command, args []string) error {
		// Convert config
		if err := cfg.Convert(); err != nil {
			return err
		}

		// Validate config
		if err := cfg.Validate(); err != nil {
			return err
		}

		// Initialize daemon dfpath
		d, err := initDaemonDfpath(cfg)
		if err != nil {
			return err
		}

		rotateConfig := logger.LogRotateConfig{
			MaxSize:    cfg.LogMaxSize,
			MaxAge:     cfg.LogMaxAge,
			MaxBackups: cfg.LogMaxBackups}

		// Initialize logger
		if err := logger.InitDaemon(cfg.Verbose, cfg.Console, d.LogDir(), rotateConfig); err != nil {
			return fmt.Errorf("init client daemon logger: %w", err)
		}
		logger.RedirectStdoutAndStderr(cfg.Console, path.Join(d.LogDir(), types.DaemonName))

		return runDaemon(d)
	},
}

func init() {
	// Add the command to parent
	rootCmd.AddCommand(daemonCmd)

	if len(os.Args) > 1 && os.Args[1] == daemonCmd.Name() {
		// Initialize default daemon config
		cfg = config.NewDaemonConfig()
		// Initialize cobra
		dependency.InitCommandAndConfig(daemonCmd, true, cfg)

		flags := daemonCmd.Flags()
		flags.Int("launcher", -1, "pid of process launching daemon, a negative number implies that the daemon is started directly by the user")
		flags.Lookup("launcher").Hidden = true
		_ = viper.BindPFlags(flags)
	}
}

func initDaemonDfpath(cfg *config.DaemonOption) (dfpath.Dfpath, error) {
	var options []dfpath.Option
	if cfg.WorkHome != "" {
		options = append(options, dfpath.WithWorkHome(cfg.WorkHome))
	}

	if os.FileMode(cfg.WorkHomeMode) != os.FileMode(0) {
		options = append(options, dfpath.WithWorkHomeMode(os.FileMode(cfg.WorkHomeMode)))
	}

	if cfg.CacheDir != "" {
		options = append(options, dfpath.WithCacheDir(cfg.CacheDir))
	}

	if os.FileMode(cfg.CacheDirMode) != os.FileMode(0) {
		options = append(options, dfpath.WithCacheDirMode(os.FileMode(cfg.CacheDirMode)))
	}

	if cfg.LogDir != "" {
		options = append(options, dfpath.WithLogDir(cfg.LogDir))
	}

	if cfg.PluginDir != "" {
		options = append(options, dfpath.WithPluginDir(cfg.PluginDir))
	}

	if cfg.Download.DownloadGRPC.UnixListen.Socket != "" {
		options = append(options, dfpath.WithDownloadUnixSocketPath(cfg.Download.DownloadGRPC.UnixListen.Socket))
	}

	if cfg.DataDir != "" {
		options = append(options, dfpath.WithDataDir(cfg.DataDir))
	}

	if os.FileMode(cfg.DataDirMode) != os.FileMode(0) {
		options = append(options, dfpath.WithDataDirMode(os.FileMode(cfg.DataDirMode)))
	}

	return dfpath.New(options...)
}

func runDaemon(d dfpath.Dfpath) error {
	logger.Infof("Version:\n%s", version.Version())
	netAddr := &dfnet.NetAddr{Type: dfnet.UNIX, Addr: d.DaemonSockPath()}
	daemonClient, err := client.GetInsecureV1(context.Background(), netAddr.String())
	if err != nil {
		return err
	}

	// Checking Steps:
	//
	// 1. Try to lock
	//
	// 2. If lock successfully, start the client daemon and then return
	//
	// 3. If lock fail, checking whether the daemon has been started. If true, return directly.
	//    Otherwise, wait 50 ms and execute again from 1
	// 4. Checking timeout about 5s
	lock := flock.New(d.DaemonLockPath())
	timeout := time.After(5 * time.Second)
	first := time.After(1 * time.Millisecond)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-timeout:
			return errors.New("the daemon is unhealthy")
		case <-first:
		case <-tick.C:
		}

		if ok, err := lock.TryLock(); err != nil {
			return err
		} else if !ok {
			if daemonClient.CheckHealth(context.Background()) == nil {
				return errors.New("the daemon is running, so there is no need to start it again")
			}
		} else {
			break
		}
	}
	defer func() {
		if err := lock.Unlock(); err != nil {
			logger.Errorf("flock unlock failed %s", err)
		}
	}()

	logger.Infof("daemon is launched by pid: %d", viper.GetInt("launcher"))

	ff := dependency.InitMonitor(cfg.PProfPort, cfg.Telemetry)
	defer ff()

	svr, err := server.New(cfg, d)
	if err != nil {
		return err
	}
	dependency.SetupQuitSignalHandler(func() { svr.Stop() })
	return svr.Serve()
}
