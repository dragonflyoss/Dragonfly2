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
	"time"

	"github.com/docker/go-units"
	"github.com/go-echarts/statsview"
	"github.com/go-echarts/statsview/viewer"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"

	"github.com/dragonflyoss/Dragonfly2/client/config"
	"github.com/dragonflyoss/Dragonfly2/client/daemon"
	"github.com/dragonflyoss/Dragonfly2/client/daemon/storage"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	_ "github.com/dragonflyoss/Dragonfly2/pkg/rpc/dfdaemon/server"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
)

var daemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "Launch a peer daemon for downloading and uploading files.",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runDaemon()
	},
}

func init() {
	initDaemonFlags()
	rootCmd.AddCommand(daemonCmd)
}

type daemonOption struct {
	dataDir  string
	workHome string

	schedulers []string

	pidFile string
	//grpcUnixListen string

	advertiseIP net.IP
	listenIP    net.IP
	grpcPort    int
	uploadPort  int
	proxyPort   int

	downloadRate string
	uploadRate   string

	storageDriver string

	dataExpireTime  time.Duration
	daemonAliveTime time.Duration
	gcInterval      time.Duration

	securityDomain string
	location       string
	idc            string
	sswitch        string

	verbose bool
}

var flagDaemonOpt = daemonOption{
	dataDir:  "",
	workHome: "",

	schedulers:      nil,
	pidFile:         "/var/run/dfdaemon.pid",
	advertiseIP:     net.IPv4zero,
	listenIP:        net.IPv4zero,
	grpcPort:        65000,
	uploadPort:      65002,
	proxyPort:       65001,
	downloadRate:    "100Mi",
	uploadRate:      "100Mi",
	storageDriver:   string(storage.SimpleLocalTaskStoreDriver),
	dataExpireTime:  config.DataExpireTime,
	daemonAliveTime: config.DaemonAliveTime,
	gcInterval:      time.Minute,
	verbose:         false,
}

func initDaemonFlags() {
	flagSet := daemonCmd.Flags()

	flagSet.StringVar(&flagDaemonOpt.dataDir, "data", flagDaemonOpt.dataDir, "local directory which stores temporary files for p2p uploading")
	flagSet.StringVar(&flagDaemonOpt.workHome, "home", flagDaemonOpt.workHome, "the work home directory of dfget daemon")

	flagSet.DurationVar(&flagDaemonOpt.dataExpireTime, "expire-time", flagDaemonOpt.dataExpireTime, "caching duration for which cached file keeps no accessed by any process, after this period cache file will be deleted")
	flagSet.DurationVar(&flagDaemonOpt.daemonAliveTime, "alive-time", flagDaemonOpt.daemonAliveTime, "alive duration for which uploader keeps no accessing by any uploading requests, after this period uploader will automatically exit")
	flagSet.DurationVar(&flagDaemonOpt.gcInterval, "gc-interval", flagDaemonOpt.gcInterval, "gc interval")

	flagSet.BoolVar(&flagDaemonOpt.verbose, "verbose", flagDaemonOpt.verbose, "print verbose log and enable golang debug info")

	flagSet.IPVar(&flagDaemonOpt.advertiseIP, "advertise-ip", flagDaemonOpt.advertiseIP, "the ip report to scheduler, normal same with listen ip")
	flagSet.IPVar(&flagDaemonOpt.listenIP, "listen-ip", flagDaemonOpt.listenIP, "local listen ip address")
	flagSet.StringArrayVar(&flagDaemonOpt.schedulers, "schedulers", []string{""}, "scheduler addresses")
	//flagSet.StringVar(&flagDaemonOpt.grpcUnixListen, "grpc-unix-listen", "/var/run/dfdaemon.sock", "the local unix domain socket listen address for grpc with dfget")
	flagSet.IntVar(&flagDaemonOpt.grpcPort, "grpc-port", flagDaemonOpt.grpcPort, "the listen address for grpc with other peers")
	flagSet.IntVar(&flagDaemonOpt.proxyPort, "proxy-port", flagDaemonOpt.proxyPort, "the address that daemon will listen on for proxy service")
	flagSet.IntVar(&flagDaemonOpt.uploadPort, "upload-port", flagDaemonOpt.uploadPort, "the address that daemon will listen on for peer upload")
	flagSet.StringVar(&flagDaemonOpt.downloadRate, "download-rate", flagDaemonOpt.downloadRate, "download rate limit for other peers and back source")
	flagSet.StringVar(&flagDaemonOpt.uploadRate, "upload-rate", flagDaemonOpt.uploadRate, "upload rate limit for other peers")
	flagSet.StringVar(&flagDaemonOpt.storageDriver, "storage-driver", flagDaemonOpt.storageDriver,
		"storage driver: io.d7y.storage.v2.simple, io.d7y.storage.v2.advance")
	flagSet.StringVar(&flagDaemonOpt.pidFile, "pid", flagDaemonOpt.pidFile, "dfdaemon pid file location")

	flagSet.StringVar(&flagDaemonOpt.securityDomain, "security-domain", "", "peer security domain for scheduler")
	flagSet.StringVar(&flagDaemonOpt.location, "location", "", "peer location for scheduler")
	flagSet.StringVar(&flagDaemonOpt.idc, "idc", "", "peer idc for scheduler")
	flagSet.StringVar(&flagDaemonOpt.sswitch, "switch", "", "peer switch for scheduler")
}

func runDaemon() error {
	if flagDaemonOpt.verbose {
		//logger.SetLevel(logrus.DebugLevel)
		go func() {
			// enable go pprof and statsview
			var debugListen = "localhost:18066"
			viewer.SetConfiguration(viewer.WithAddr(debugListen))
			logger.Infof("enable go pprof at %s", debugListen)
			if err := statsview.New().Start(); err != nil {
				logger.Warnf("serve go pprof error: %s", err)
			}
		}()
	}

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
		ip, err = getLocalIP()
		if err != nil {
			return nil
		}
	}
	if ip == "" || ip == "0.0.0.0" {
		return fmt.Errorf("unable to autodetect peer ip for scheduler, please set it via --advertise-ip")
	}
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	host := &scheduler.PeerHost{
		Uuid:           uuid.New().String(),
		Ip:             ip,
		Port:           int32(flagDaemonOpt.grpcPort),
		HostName:       hostname,
		SecurityDomain: flagDaemonOpt.securityDomain,
		Location:       flagDaemonOpt.location,
		Idc:            flagDaemonOpt.idc,
		Switch:         flagDaemonOpt.sswitch,
	}
	ph, err := daemon.NewPeerHost(host, *option)
	if err != nil {
		logger.Errorf("init peer host failed: %s", err)
		return err
	}
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
	option := &daemon.PeerHostOption{
		GCInterval: flagDaemonOpt.gcInterval,
		Scheduler: daemon.SchedulerOption{
			// TODO client cert
			Addresses: flagDaemonOpt.schedulers,
			DialOptions: []grpc.DialOption{
				grpc.WithInsecure(),
			},
		},
		AliveTime: flagDaemonOpt.daemonAliveTime,
		Download: daemon.DownloadOption{
			GRPC: daemon.ListenOption{
				Listen:   fmt.Sprintf("%s:%d", flagDaemonOpt.listenIP.String(), flagDaemonOpt.grpcPort),
				Network:  "tcp",
				Insecure: true,
			},
			// TODO
			Proxy:     nil,
			RateLimit: rate.Limit(dr),
		},
		Upload: daemon.UploadOption{
			ListenOption: daemon.ListenOption{
				Listen:  fmt.Sprintf("%s:%d", flagDaemonOpt.listenIP.String(), flagDaemonOpt.uploadPort),
				Network: "tcp",
				// TODO
				Insecure: true,
			},
			RateLimit: rate.Limit(ur),
		},
		Storage: daemon.StorageOption{
			Option: storage.Option{
				DataPath:       flagDaemonOpt.dataDir,
				TaskExpireTime: flagDaemonOpt.dataExpireTime,
			},
			Driver: storage.Driver(flagDaemonOpt.storageDriver),
		},
	}
	return option, nil
}

func getLocalIP() (ip string, err error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return
	}
	for _, addr := range addrs {
		ipAddr, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}
		if ipAddr.IP.IsLoopback() {
			continue
		}
		if !ipAddr.IP.IsGlobalUnicast() {
			continue
		}
		return ipAddr.IP.String(), nil
	}
	return
}
