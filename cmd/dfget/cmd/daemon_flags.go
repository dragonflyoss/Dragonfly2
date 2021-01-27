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
)

type daemonOption struct {
	dataDir  string
	workHome string

	schedulers []string

	pidFile  string
	lockFile string

	advertiseIP    net.IP
	listenIP       net.IP
	downloadSocket string
	peerPort       int
	peerPortEnd    int
	uploadPort     int
	uploadPortEnd  int
	proxyPort      int

	downloadRate string
	uploadRate   string

	storeStrategy string

	dataExpireTime  time.Duration
	daemonAliveTime time.Duration
	gcInterval      time.Duration
	keepStorage     bool

	securityDomain string
	location       string
	idc            string
	netTopology    string

	verbose bool
}

func init() {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = os.TempDir()
	}
	flagDaemonOpt.workHome = fmt.Sprintf("%s/.small-dragonfly/dfdaemon/", homeDir)
	flagDaemonOpt.dataDir = flagDaemonOpt.workHome
}

func initDaemonFlags() {
	flagSet := daemonCmd.Flags()

	flagSet.StringVar(&flagDaemonOpt.dataDir, "data", flagDaemonOpt.dataDir, "local directory which stores temporary files for p2p uploading")
	flagSet.StringVar(&flagDaemonOpt.workHome, "home", flagDaemonOpt.workHome, "the work home directory of dfget daemon")

	flagSet.DurationVar(&flagDaemonOpt.dataExpireTime, "expire-time", flagDaemonOpt.dataExpireTime, "caching duration for which cached file keeps no accessed by any process, after this period cache file will be deleted")
	flagSet.DurationVar(&flagDaemonOpt.daemonAliveTime, "alive-time", flagDaemonOpt.daemonAliveTime, "alive duration for which uploader keeps no accessing by any uploading requests, after this period uploader will automatically exit")
	flagSet.DurationVar(&flagDaemonOpt.gcInterval, "gc-interval", flagDaemonOpt.gcInterval, "gc interval")
	flagSet.BoolVar(&flagDaemonOpt.keepStorage, "keep-storage", flagDaemonOpt.keepStorage, "keep storage after daemon exit")

	flagSet.BoolVar(&flagDaemonOpt.verbose, "verbose", flagDaemonOpt.verbose, "print verbose log and enable golang debug info")

	flagSet.IPVar(&flagDaemonOpt.advertiseIP, "advertise-ip", flagDaemonOpt.advertiseIP, "the ip report to scheduler, normal same with listen ip")
	flagSet.IPVar(&flagDaemonOpt.listenIP, "listen-ip", flagDaemonOpt.listenIP, "local listen ip address")
	flagSet.StringArrayVar(&flagDaemonOpt.schedulers, "schedulers", []string{""}, "scheduler addresses")
	flagSet.StringVar(&flagDaemonOpt.downloadSocket, "grpc-unix-listen", flagDaemonOpt.downloadSocket, "the local unix domain socket listen address for grpc with dfget")
	flagSet.IntVar(&flagDaemonOpt.peerPort, "grpc-port", flagDaemonOpt.peerPort, "the listen address for grpc with other peers")
	flagSet.IntVar(&flagDaemonOpt.peerPortEnd, "grpc-port-end", flagDaemonOpt.peerPort, "the listen address for grpc with other peers")
	flagSet.IntVar(&flagDaemonOpt.proxyPort, "proxy-port", flagDaemonOpt.proxyPort, "the address that daemon will listen on for proxy service")
	flagSet.IntVar(&flagDaemonOpt.uploadPort, "upload-port", flagDaemonOpt.uploadPort, "the address that daemon will listen on for peer upload")
	flagSet.IntVar(&flagDaemonOpt.uploadPortEnd, "upload-port-end", flagDaemonOpt.uploadPort, "the address that daemon will listen on for peer upload")
	flagSet.StringVar(&flagDaemonOpt.downloadRate, "download-rate", flagDaemonOpt.downloadRate, "download rate limit for other peers and back source")
	flagSet.StringVar(&flagDaemonOpt.uploadRate, "upload-rate", flagDaemonOpt.uploadRate, "upload rate limit for other peers")
	flagSet.StringVar(&flagDaemonOpt.storeStrategy, "store-strategy", flagDaemonOpt.storeStrategy,
		"storage driver: io.d7y.storage.v2.simple, io.d7y.storage.v2.advance")
	flagSet.StringVar(&flagDaemonOpt.pidFile, "pid", flagDaemonOpt.pidFile, "dfdaemon pid file location")
	flagSet.StringVar(&flagDaemonOpt.lockFile, "lock", flagDaemonOpt.lockFile, "dfdaemon lock file location")

	flagSet.StringVar(&flagDaemonOpt.securityDomain, "security-domain", "", "peer security domain for scheduler")
	flagSet.StringVar(&flagDaemonOpt.location, "location", "", "peer location for scheduler")
	flagSet.StringVar(&flagDaemonOpt.idc, "idc", "", "peer idc for scheduler")
	flagSet.StringVar(&flagDaemonOpt.netTopology, "net-topology", "", "peer switch for scheduler")
}
