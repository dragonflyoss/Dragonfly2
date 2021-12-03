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
	"os"
	"time"

	"github.com/gofrs/flock"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"

	"d7y.io/dragonfly/v2/client/config"
	server "d7y.io/dragonfly/v2/client/daemon"
	"d7y.io/dragonfly/v2/cmd/dependency"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dflog/logcore"
	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/internal/dfpath"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
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
		if err := logcore.InitDaemon(cfg.Console); err != nil {
			return errors.Wrap(err, "init client daemon logger")
		}

		// Convert config
		if err := cfg.Convert(); err != nil {
			return err
		}

		// Validate config
		if err := cfg.Validate(); err != nil {
			return err
		}

		return runDaemon()
	},
}

func init() {
	// Add the command to parent
	rootCmd.AddCommand(daemonCmd)

	if len(os.Args) > 1 && os.Args[1] == daemonCmd.Name() {
		// Initialize default daemon config
		cfg = config.NewDaemonConfig()
		// Initialize cobra
		dependency.InitCobra(daemonCmd, true, cfg)

		flags := daemonCmd.Flags()
		flags.Int("launcher", -1, "pid of process launching daemon, a negative number implies that the daemon is started directly by the user")
		flags.Lookup("launcher").Hidden = true
		_ = viper.BindPFlags(flags)
	}
}

func runDaemon() error {
	logger.Infof("Version:\n%s", version.Version())

	target := dfnet.NetAddr{Type: dfnet.UNIX, Addr: dfpath.DaemonSockPath}
	daemonClient, err := client.GetClientByAddr([]dfnet.NetAddr{target})
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
	lock := flock.New(dfpath.DaemonLockPath)
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
			if daemonClient.CheckHealth(context.Background(), target) == nil {
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

	// daemon config values
	s, _ := yaml.Marshal(cfg)
	logger.Infof("client daemon configuration:\n%s", string(s))

	ff := dependency.InitMonitor(cfg.Verbose, cfg.PProfPort, cfg.Telemetry)
	defer ff()

	svr, err := server.New(cfg)
	if err != nil {
		return err
	}
	dependency.SetupQuitSignalHandler(func() { svr.Stop() })
	return svr.Serve()
}
