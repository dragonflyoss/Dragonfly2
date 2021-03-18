/*
 * Copyright The Dragonfly Authors.
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
	"os/signal"
	"strconv"
	"syscall"

	"github.com/go-echarts/statsview"
	"github.com/go-echarts/statsview/viewer"
	"github.com/gofrs/flock"
	"github.com/google/uuid"
	"github.com/phayes/freeport"
	"github.com/spf13/cobra"
	"go.uber.org/zap/zapcore"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon"
	"d7y.io/dragonfly/v2/client/pidfile"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/dflog/logcore"
	_ "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/server"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

var daemonConfig *config.PeerHostOption
var daemonConfigPath string

var daemonCmd = &cobra.Command{
	Use:          "daemon",
	Short:        "Launch a peer daemon for downloading and uploading files.",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Validate daemon config
		if err := daemonConfig.Validate(); err != nil {
			return err
		}

		// Initialize logger
		logcore.InitDaemon(daemonConfig.Console)

		// Start daemon
		return runDaemon(daemonConfig)
	},
}

func init() {
	// Initialize default config
	daemonConfig = &config.PeerHostConfig

	// Initialize cobra
	cobra.OnInitialize(initDaemonConfig)

	// Add flags
	addDaemonFlags()

	// Add command
	rootCmd.AddCommand(daemonCmd)
}

// initConfig reads in config file if set
func initDaemonConfig() {
	// Initialize config path
	if daemonConfigPath == "" {
		daemonConfigPath = config.PeerHostConfigPath
	}

	_, err := os.Stat(daemonConfigPath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Load from config file
	if err := daemonConfig.Load(daemonConfigPath); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func runDaemon(cfg *config.PeerHostOption) error {
	// TODO(Gaius): remove
	s, _ := json.MarshalIndent(cfg, "", "  ")
	logger.Debugf("daemon option(debug only, can not use as config):\n%s", string(s))

	// Initialize lock file
	lock := flock.New(cfg.LockFile)
	if ok, err := lock.TryLock(); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("lock file %s failed, other daemon is already running", daemonConfig.LockFile)
	}
	defer lock.Unlock()

	// Initialize pid file
	pid, err := pidfile.New(cfg.PidFile)
	if err != nil {
		return fmt.Errorf("check pid failed: %s, please check %s", err, cfg.PidFile)
	}
	defer pid.Remove()

	// Initialize verbose mode
	initVerboseMode(cfg.Verbose)

	ph, err := daemon.NewPeerHost(&scheduler.PeerHost{
		Uuid:           uuid.New().String(),
		Ip:             cfg.Host.AdvertiseIP,
		RpcPort:        int32(cfg.Download.PeerGRPC.TCPListen.PortRange.Start),
		DownPort:       0,
		HostName:       iputils.HostName,
		SecurityDomain: cfg.Host.SecurityDomain,
		Location:       cfg.Host.Location,
		Idc:            cfg.Host.IDC,
		NetTopology:    cfg.Host.NetTopology,
	}, *cfg)
	if err != nil {
		logger.Errorf("init peer host failed: %s", err)
		return err
	}

	setupSignalHandler(ph)
	return ph.Serve()
}

func initVerboseMode(verbose bool) {
	if !verbose {
		return
	}

	logcore.SetCoreLevel(zapcore.DebugLevel)
	logcore.SetGrpcLevel(zapcore.DebugLevel)

	go func() {
		// enable go pprof and statsview
		port, _ := strconv.Atoi(os.Getenv("D7Y_PPROF_PORT"))
		if port == 0 {
			port, _ = freeport.GetFreePort()
		}

		debugListen := fmt.Sprintf("localhost:%d", port)
		viewer.SetConfiguration(viewer.WithAddr(debugListen))

		logger.With("pprof", fmt.Sprintf("http://%s/debug/pprof", debugListen),
			"statsview", fmt.Sprintf("http://%s/debug/statsview", debugListen)).
			Infof("enable debug at http://%s", debugListen)

		if err := statsview.New().Start(); err != nil {
			logger.Warnf("serve go pprof error: %s", err)
		}
	}()
}

func setupSignalHandler(ph daemon.PeerHost) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		var done bool
		for {
			select {
			case sig := <-sigs:
				logger.Infof("receive %s signal", sig)
				if !done {
					ph.Stop()
					done = true
				}
			}
		}
	}()
}
