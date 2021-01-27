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
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/docker/go-units"
	"github.com/go-echarts/statsview"
	"github.com/go-echarts/statsview/viewer"
	"github.com/gofrs/flock"
	"github.com/google/uuid"
	"github.com/phayes/freeport"
	"github.com/spf13/cobra"
	"go.uber.org/zap/zapcore"
	"golang.org/x/time/rate"

	"github.com/dragonflyoss/Dragonfly2/client/config"
	"github.com/dragonflyoss/Dragonfly2/client/daemon"
	"github.com/dragonflyoss/Dragonfly2/client/daemon/storage"
	"github.com/dragonflyoss/Dragonfly2/pkg/basic/dfnet"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	_ "github.com/dragonflyoss/Dragonfly2/pkg/rpc/dfdaemon/server"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/pidfile"
)

var daemonCmd = &cobra.Command{
	Use:          "daemon",
	Short:        "Launch a peer daemon for downloading and uploading files.",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		logger.InitDaemon()
		lock := flock.New(flagDaemonOpt.lockFile)
		if ok, err := lock.TryLock(); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("lock file %s failed, other daemon is already running", flagDaemonOpt.lockFile)
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
	if flagDaemonOpt.verbose {
		logger.LogLevel.SetLevel(zapcore.DebugLevel)
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

	pid, err := pidfile.New(flagDaemonOpt.pidFile)
	if err != nil {
		return fmt.Errorf("check pid failed: %s, please check %s", err, flagDaemonOpt.pidFile)
	}
	defer pid.Remove()

	option, err := initDaemonOption()
	if err != nil {
		return err
	}

	var ip string
	if !net.IPv4zero.Equal(flagDaemonOpt.advertiseIP) {
		ip = flagDaemonOpt.advertiseIP.String()
	} else if !net.IPv4zero.Equal(flagDaemonOpt.listenIP) {
		ip = flagDaemonOpt.advertiseIP.String()
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
		RpcPort:        int32(flagDaemonOpt.peerPort),
		DownPort:       0,
		HostName:       dfnet.HostName,
		SecurityDomain: flagDaemonOpt.securityDomain,
		Location:       flagDaemonOpt.location,
		Idc:            flagDaemonOpt.idc,
		NetTopology:    flagDaemonOpt.netTopology,
	}
	ph, err := daemon.NewPeerHost(host, *option)
	if err != nil {
		logger.Errorf("init peer host failed: %s", err)
		return err
	}
	setupSignalHandler(ph)
	return ph.Serve()
}

func initDaemonOption() (*daemon.PeerHostOption, error) {
	dr, err := units.FromHumanSize(flagDaemonOpt.downloadRate)
	if err != nil {
		return nil, fmt.Errorf("download rate %q parse error: %s", flagDaemonOpt.downloadRate, err)
	}

	ur, err := units.FromHumanSize(flagDaemonOpt.uploadRate)
	if err != nil {
		return nil, fmt.Errorf("upload rate %q parse error: %s", flagDaemonOpt.uploadRate, err)
	}

	exp, _ := config.NewRegexp("blobs/sha256.*")

	option := &daemon.PeerHostOption{
		AliveTime:   flagDaemonOpt.daemonAliveTime,
		GCInterval:  flagDaemonOpt.gcInterval,
		KeepStorage: flagDaemonOpt.keepStorage,
		// FIXME(jim): parse []basic.NetAddr from flagDaemonOpt.schedulers
		Schedulers: []dfnet.NetAddr{
			{
				Type: dfnet.TCP,
				Addr: flagDaemonOpt.schedulers[0],
			},
		},
		Server: daemon.ServerOption{
			RateLimit: rate.Limit(dr),
			DownloadGRPC: daemon.ListenOption{
				// TODO
				Security: daemon.SecurityOption{
					Insecure: true,
				},
				UnixListen: &daemon.UnixListenOption{
					Socket: flagDaemonOpt.downloadSocket,
				},
			},
			PeerGRPC: daemon.ListenOption{
				// TODO
				Security: daemon.SecurityOption{
					Insecure: true,
				},
				TCPListen: &daemon.TCPListenOption{
					Listen: flagDaemonOpt.listenIP.String(),
					PortRange: daemon.TCPListenPortRange{
						Start: flagDaemonOpt.peerPort,
						End:   flagDaemonOpt.peerPortEnd,
					},
				},
			},
		},
		Proxy: &daemon.ProxyOption{
			ListenOption: &daemon.ListenOption{
				// TODO
				Security: daemon.SecurityOption{
					Insecure: true,
				},
				TCPListen: &daemon.TCPListenOption{
					Listen: flagDaemonOpt.listenIP.String(),
					PortRange: daemon.TCPListenPortRange{
						Start: flagDaemonOpt.uploadPort,
						End:   flagDaemonOpt.uploadPortEnd,
					},
				},
			},
			// TODO
			RegistryMirror: &config.RegistryMirror{
				Insecure: false,
			},
			Proxies: []*config.Proxy{
				{
					Regx: exp,
				},
			},
			HijackHTTPS: nil,
		},
		Upload: daemon.UploadOption{
			ListenOption: daemon.ListenOption{
				// TODO
				Security: daemon.SecurityOption{
					Insecure: true,
				},
				TCPListen: &daemon.TCPListenOption{
					Listen: flagDaemonOpt.listenIP.String(),
					PortRange: daemon.TCPListenPortRange{
						Start: flagDaemonOpt.uploadPort,
						End:   flagDaemonOpt.uploadPortEnd,
					},
				},
			},
			RateLimit: rate.Limit(ur),
		},
		Storage: daemon.StorageOption{
			Option: storage.Option{
				DataPath:       flagDaemonOpt.dataDir,
				TaskExpireTime: flagDaemonOpt.dataExpireTime,
			},
			StoreStrategy: storage.StoreStrategy(flagDaemonOpt.storeStrategy),
		},
	}
	return option, nil
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
