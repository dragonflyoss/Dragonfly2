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

	"d7y.io/dragonfly/v2/client/config"
)

func addDaemonFlags() {
	flagSet := daemonCmd.Flags()
	flagSet.StringVar(&daemonConfig.DataDir, "data", daemonConfig.DataDir, "local directory which stores temporary files for p2p uploading")
	flagSet.StringVar(&daemonConfig.WorkHome, "home", daemonConfig.WorkHome, "the work home directory of dfget daemon")
	flagSet.DurationVar(&daemonConfig.Storage.Option.TaskExpireTime.Duration, "expire-time", daemonConfig.Storage.Option.TaskExpireTime.Duration, "caching duration for which cached file keeps no accessed by any process, after this period cache file will be deleted")
	flagSet.DurationVar(&daemonConfig.AliveTime.Duration, "alive-time", daemonConfig.AliveTime.Duration, "alive duration for which uploader keeps no accessing by any uploading requests, after this period uploader will automatically exit")
	flagSet.DurationVar(&daemonConfig.GCInterval.Duration, "gc-interval", daemonConfig.GCInterval.Duration, "gc interval")
	flagSet.BoolVar(&daemonConfig.KeepStorage, "keep-storage", daemonConfig.KeepStorage, "keep storage after daemon exit")
	flagSet.BoolVar(&daemonConfig.Verbose, "verbose", daemonConfig.Verbose, "print verbose log and enable golang debug info")
	flagSet.BoolVar(&daemonConfig.Console, "console", daemonConfig.Console, "console shows log on console")
	flagSet.StringVar(&daemonConfig.Host.AdvertiseIP, "advertise-ip", daemonConfig.Host.AdvertiseIP, "the ip report to scheduler, normal same with listen ip")
	flagSet.StringVar(&daemonConfig.Host.ListenIP, "listen", daemonConfig.Host.ListenIP, "the listen ip")
	flagSet.StringVar(&daemonConfig.Download.DownloadGRPC.UnixListen.Socket, "grpc-unix-listen", daemonConfig.Download.DownloadGRPC.UnixListen.Socket, "the local unix domain socket listen address for grpc with dfget")
	flagSet.IntVar(&daemonConfig.Download.PeerGRPC.TCPListen.PortRange.Start, "grpc-port", daemonConfig.Download.PeerGRPC.TCPListen.PortRange.Start, "the listen address for grpc with other peers")
	flagSet.IntVar(&daemonConfig.Download.PeerGRPC.TCPListen.PortRange.End, "grpc-port-end", daemonConfig.Download.PeerGRPC.TCPListen.PortRange.End, "the listen address for grpc with other peers")
	flagSet.IntVar(&daemonConfig.Upload.ListenOption.TCPListen.PortRange.Start, "upload-port", daemonConfig.Upload.ListenOption.TCPListen.PortRange.Start, "the address that daemon will listen on for peer upload")
	flagSet.IntVar(&daemonConfig.Upload.ListenOption.TCPListen.PortRange.End, "upload-port-end", daemonConfig.Upload.ListenOption.TCPListen.PortRange.End, "the address that daemon will listen on for peer upload")
	flagSet.StringVar(&daemonConfig.PidFile, "pid", daemonConfig.PidFile, "dfdaemon pid file location")
	flagSet.StringVar(&daemonConfig.LockFile, "lock", daemonConfig.LockFile, "dfdaemon lock file location")
	flagSet.StringVar(&daemonConfig.Host.SecurityDomain, "security-domain", "", "peer security domain for scheduler")
	flagSet.StringVar(&daemonConfig.Host.Location, "location", daemonConfig.Host.Location, "peer location for scheduler")
	flagSet.StringVar(&daemonConfig.Host.IDC, "idc", daemonConfig.Host.IDC, "peer idc for scheduler")
	flagSet.StringVar(&daemonConfig.Host.NetTopology, "net-topology", daemonConfig.Host.NetTopology, "peer net topology for scheduler")
	flagSet.Var(config.NewLimitRateValue(&daemonConfig.Download.RateLimit), "download-rate", "download rate limit for other peers and back source")
	flagSet.Var(config.NewLimitRateValue(&daemonConfig.Upload.RateLimit), "upload-rate", "upload rate limit for other peers")
	flagSet.VarP(config.NewNetAddrsValue(&daemonConfig.Scheduler.NetAddrs), "schedulers", "s", "schedulers")
	flagSet.DurationVar(&daemonConfig.Scheduler.ScheduleTimeout.Duration, "schedule-timeout", daemonConfig.Scheduler.ScheduleTimeout.Duration, "schedule timeout")
	flagSet.StringVar(&daemonConfigPath, "config", daemonConfigPath, "daemon config file location")

	fmt.Println("1111111111111")
	fmt.Println(daemonCmd.Flags().Lookup("verbose").Value)
	fmt.Println("1111111111111")
}
