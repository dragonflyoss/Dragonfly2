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
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-echarts/statsview"
	"github.com/go-echarts/statsview/viewer"
	"github.com/gofrs/flock"
	"github.com/google/uuid"
	"github.com/phayes/freeport"
	"github.com/spf13/cobra"
	"go.uber.org/zap/zapcore"

	"github.com/dragonflyoss/Dragonfly/v2/client/daemon"
	"github.com/dragonflyoss/Dragonfly/v2/pkg/basic/dfnet"
	logger "github.com/dragonflyoss/Dragonfly/v2/pkg/dflog"
	_ "github.com/dragonflyoss/Dragonfly/v2/pkg/rpc/dfdaemon/server"
	"github.com/dragonflyoss/Dragonfly/v2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly/v2/pkg/util/pidfile"
)

var daemonCmd = &cobra.Command{
	Use:          "daemon",
	Short:        "Launch a peer daemon for downloading and uploading files.",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		// load from default config file
		_, err := os.Stat(peerHostConfigPath)
		if err == nil {
			err := flagDaemonOpt.Load(peerHostConfigPath)
			if err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		}

		logger.InitDaemon()
		lock := flock.New(flagDaemonOpt.LockFile)
		if ok, err := lock.TryLock(); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("lock file %s failed, other daemon is already running", flagDaemonOpt.LockFile)
		}
		defer lock.Unlock()
		return runDaemon()
	},
}

func init() {
	initDaemonFlags()
	rootCmd.AddCommand(daemonCmd)
}

func runDaemon() error {
	s, _ := json.MarshalIndent(flagDaemonOpt, "", "  ")
	logger.Debugf("daemon option(debug only, can not use as config):\n%s", string(s))

	if flagDaemonOpt.Verbose {
		logger.SetCoreLevel(zapcore.DebugLevel)
		go func() {
			// enable go pprof and statsview
			port, _ := freeport.GetFreePort()
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

	pid, err := pidfile.New(flagDaemonOpt.PidFile)
	if err != nil {
		return fmt.Errorf("check pid failed: %s, please check %s", err, flagDaemonOpt.PidFile)
	}
	defer pid.Remove()

	var ip string
	if !net.IPv4zero.Equal(net.ParseIP(flagDaemonOpt.Host.AdvertiseIP)) {
		ip = flagDaemonOpt.Host.AdvertiseIP
	} else {
		ip = dfnet.HostIp
	}
	if ip == "" || ip == "0.0.0.0" {
		return fmt.Errorf("unable to autodetect peer ip for scheduler, please set it via --advertise-ip")
	}
	logger.Infof("use %s as peer ip", ip)

	host := &scheduler.PeerHost{
		Uuid:           uuid.New().String(),
		Ip:             ip,
		RpcPort:        int32(flagDaemonOpt.Download.PeerGRPC.TCPListen.PortRange.Start),
		DownPort:       0,
		HostName:       dfnet.HostName,
		SecurityDomain: flagDaemonOpt.Host.SecurityDomain,
		Location:       flagDaemonOpt.Host.Location,
		Idc:            flagDaemonOpt.Host.IDC,
		NetTopology:    flagDaemonOpt.Host.NetTopology,
	}

	ph, err := daemon.NewPeerHost(host, flagDaemonOpt)
	if err != nil {
		logger.Errorf("init peer host failed: %s", err)
		return err
	}
	setupSignalHandler(ph)
	return ph.Serve()
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
