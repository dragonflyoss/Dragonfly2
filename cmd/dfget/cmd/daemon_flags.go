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
	"os"

	"d7y.io/dragonfly/v2/client/config"
)

func init() {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = os.TempDir()
	}
	flagDaemonOpt.WorkHome = fmt.Sprintf("%s/.small-dragonfly/dfdaemon/", homeDir)
	flagDaemonOpt.DataDir = flagDaemonOpt.WorkHome
}

func initDaemonFlags() {
	flagSet := daemonCmd.Flags()
	flagSet.StringVar(&flagDaemonOpt.DataDir, "data", flagDaemonOpt.DataDir, "local directory which stores temporary files for p2p uploading")
	flagSet.StringVar(&flagDaemonOpt.WorkHome, "home", flagDaemonOpt.WorkHome, "the work home directory of dfget daemon")
	flagSet.DurationVar(&flagDaemonOpt.Storage.Option.TaskExpireTime.Duration, "expire-time", flagDaemonOpt.Storage.Option.TaskExpireTime.Duration, "caching duration for which cached file keeps no accessed by any process, after this period cache file will be deleted")
	flagSet.DurationVar(&flagDaemonOpt.AliveTime.Duration, "alive-time", flagDaemonOpt.AliveTime.Duration, "alive duration for which uploader keeps no accessing by any uploading requests, after this period uploader will automatically exit")
	flagSet.DurationVar(&flagDaemonOpt.GCInterval.Duration, "gc-interval", flagDaemonOpt.GCInterval.Duration, "gc interval")
	flagSet.BoolVar(&flagDaemonOpt.KeepStorage, "keep-storage", flagDaemonOpt.KeepStorage, "keep storage after daemon exit")
	flagSet.BoolVar(&flagDaemonOpt.Verbose, "verbose", flagDaemonOpt.Verbose, "print verbose log and enable golang debug info")
	flagSet.StringVar(&flagDaemonOpt.Host.AdvertiseIP, "advertise-ip", flagDaemonOpt.Host.AdvertiseIP, "the ip report to scheduler, normal same with listen ip")
	flagSet.StringVar(&flagDaemonOpt.Host.ListenIP, "listen", flagDaemonOpt.Host.ListenIP, "the listen ip")
	flagSet.StringVar(&flagDaemonOpt.Download.DownloadGRPC.UnixListen.Socket, "grpc-unix-listen", flagDaemonOpt.Download.DownloadGRPC.UnixListen.Socket, "the local unix domain socket listen address for grpc with dfget")
	flagSet.IntVar(&flagDaemonOpt.Download.PeerGRPC.TCPListen.PortRange.Start, "grpc-port", flagDaemonOpt.Download.PeerGRPC.TCPListen.PortRange.Start, "the listen address for grpc with other peers")
	flagSet.IntVar(&flagDaemonOpt.Download.PeerGRPC.TCPListen.PortRange.End, "grpc-port-end", flagDaemonOpt.Download.PeerGRPC.TCPListen.PortRange.End, "the listen address for grpc with other peers")
	flagSet.IntVar(&flagDaemonOpt.Proxy.ListenOption.TCPListen.PortRange.Start, "proxy-port", flagDaemonOpt.Proxy.ListenOption.TCPListen.PortRange.Start, "the address that daemon will listen on for proxy service")
	flagSet.IntVar(&flagDaemonOpt.Proxy.ListenOption.TCPListen.PortRange.End, "proxy-port-end", flagDaemonOpt.Proxy.ListenOption.TCPListen.PortRange.End, "the address that daemon will listen on for proxy service")
	flagSet.IntVar(&flagDaemonOpt.Upload.ListenOption.TCPListen.PortRange.Start, "upload-port", flagDaemonOpt.Upload.ListenOption.TCPListen.PortRange.Start, "the address that daemon will listen on for peer upload")
	flagSet.IntVar(&flagDaemonOpt.Upload.ListenOption.TCPListen.PortRange.End, "upload-port-end", flagDaemonOpt.Upload.ListenOption.TCPListen.PortRange.End, "the address that daemon will listen on for peer upload")
	flagSet.StringVar(&flagDaemonOpt.PidFile, "pid", flagDaemonOpt.PidFile, "dfdaemon pid file location")
	flagSet.StringVar(&flagDaemonOpt.LockFile, "lock", flagDaemonOpt.LockFile, "dfdaemon lock file location")
	flagSet.StringVar(&flagDaemonOpt.Host.SecurityDomain, "security-domain", "", "peer security domain for scheduler")
	flagSet.StringVar(&flagDaemonOpt.Host.Location, "location", flagDaemonOpt.Host.Location, "peer location for scheduler")
	flagSet.StringVar(&flagDaemonOpt.Host.IDC, "idc", flagDaemonOpt.Host.IDC, "peer idc for scheduler")
	flagSet.StringVar(&flagDaemonOpt.Host.NetTopology, "net-topology", flagDaemonOpt.Host.NetTopology, "peer net topology for scheduler")

	flagSet.Var(config.NewLimitRateValue(&flagDaemonOpt.Download.RateLimit), "download-rate", "download rate limit for other peers and back source")
	flagSet.Var(config.NewLimitRateValue(&flagDaemonOpt.Upload.RateLimit), "upload-rate", "upload rate limit for other peers")
	flagSet.VarP(config.NewSchedulersValue(&flagDaemonOpt), "schedulers", "s", "schedulers")

	flagSet.StringVar(&peerHostConfigPath, "config", "c", "daemon config file location")
}
