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
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"

	"d7y.io/dragonfly/v2/client/daemon"
	"d7y.io/dragonfly/v2/client/pidfile"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/dflog/logcore"
	_ "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/server"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
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

		logcore.InitDaemon(false)
		if err := checkDaemonOptions(); err != nil {
			return err
		}

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

func checkDaemonOptions() error {
	if len(flagDaemonOpt.Schedulers) == 0 {
		return errors.New("empty schedulers")
	}
	return nil
}

func runDaemon() error {
	s, _ := json.MarshalIndent(flagDaemonOpt, "", "  ")
	logger.Debugf("daemon option(debug only, can not use as config):\n%s", string(s))

	if flagDaemonOpt.Verbose {
		// TODO (jim): update json marshal function
		s, _ := json.MarshalIndent(flagDaemonOpt, "", "  ")
		logger.Debugf("daemon json option(debug only, should not use as config):\n%s", string(s))

		// TODO (jim): update yaml marshal function
		s, _ = yaml.Marshal(flagDaemonOpt)
		logger.Debugf("daemon yaml option(debug only, should not use as config):\n%s", string(s))

		logcore.SetCoreLevel(zapcore.DebugLevel)
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
		ip = iputils.HostIp
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
		HostName:       iputils.HostName,
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
