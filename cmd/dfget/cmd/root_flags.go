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
	"github.com/dragonflyoss/Dragonfly2/client/config"
)

var deprecatedFlags struct {
	dfdaemon    bool
	clientQueue int
	nodes       config.SupernodesValue
}

func initRootFlags() {
	// pass to server
	flagSet := rootCmd.Flags()

	// url & output
	flagSet.StringVarP(&flagClientOpt.URL, "url", "u", "", "URL of user requested downloading file(only HTTP/HTTPs supported)")
	flagSet.StringVarP(&flagClientOpt.Output, "output", "o", "",
		"destination path which is used to store the requested downloading file. It must contain detailed directory and specific filename, for example, '/tmp/file.mp4'")

	// localLimit & minRate & totalLimit & timeout
	//flagSet.VarP(&flagDaemonOpt.LocalLimit, "locallimit", "s",
	//"network bandwidth rate limit for single download task, in format of G(B)/g/M(B)/m/K(B)/k/B, pure number will also be parsed as Byte")
	//flagSet.Var(&flagClientOpt.MinRate, "minrate",
	//"minimal network bandwidth rate for downloading a file, in format of G(B)/g/M(B)/m/K(B)/k/B, pure number will also be parsed as Byte")
	flagSet.Var(config.NewLimitRateValue(&flagDaemonOpt.Download.RateLimit), "totallimit",
		"network bandwidth rate limit for the whole host, in format of G(B)/g/M(B)/m/K(B)/k/B, pure number will also be parsed as Byte")
	flagSet.DurationVarP(&flagClientOpt.Timeout, "timeout", "e", 0,
		"timeout set for file downloading task. If dfget has not finished downloading all pieces of file before --timeout, the dfget will throw an error and exit")

	// md5 & identifier
	flagSet.StringVarP(&flagClientOpt.Md5, "md5", "m", "",
		"md5 value input from user for the requested downloading file to enhance security")
	flagSet.StringVarP(&flagClientOpt.Identifier, "identifier", "i", "",
		"the usage of identifier is making different downloading tasks generate different downloading task IDs even if they have the same URLs. conflict with --md5.")
	flagSet.StringVar(&flagClientOpt.CallSystem, "callsystem", "",
		"the name of dfget caller which is for debugging. Once set, it will be passed to all components around the request to make debugging easy")
	flagSet.StringSliceVar(&flagClientOpt.Cacerts, "cacerts", nil,
		"the cacert file which is used to verify remote server when supernode interact with the source.")
	flagSet.StringVarP(&flagClientOpt.Pattern, "pattern", "p", "p2p",
		"download pattern, must be p2p/cdn/source, cdn and source do not support flag --totallimit")
	flagSet.StringVarP(&filter, "filter", "f", "",
		"filter some query params of URL, use char '&' to separate different params"+
			"\neg: -f 'key&sign' will filter 'key' and 'sign' query param"+
			"\nin this way, different but actually the same URLs can reuse the same downloading task")
	flagSet.StringArrayVar(&flagClientOpt.Header, "header", nil,
		"http header, eg: --header='Accept: *' --header='Host: abc'")
	flagSet.VarP(&deprecatedFlags.nodes, "node", "n",
		"deprecated, please use schedulers instead. specify the addresses(host:port=weight) of supernodes where the host is necessary, the port(default: 8002) and the weight(default:1) are optional. And the type of weight must be integer")
	flagSet.BoolVar(&flagClientOpt.NotBackSource, "notbacksource", false,
		"disable back source downloading for requested file when p2p fails to download it")

	flagSet.BoolVar(&deprecatedFlags.dfdaemon, "dfdaemon", deprecatedFlags.dfdaemon,
		"identify whether the request is from dfdaemon")
	flagSet.BoolVar(&flagClientOpt.Insecure, "insecure", false,
		"identify whether supernode should skip secure verify when interact with the source.")
	flagSet.IntVar(&deprecatedFlags.clientQueue, "clientqueue", deprecatedFlags.clientQueue,
		"specify the size of client queue which controls the number of pieces that can be processed simultaneously")

	// others
	flagSet.BoolVarP(&flagClientOpt.ShowBar, "showbar", "b", false,
		"show progress bar, it is conflict with '--console'")
	flagSet.BoolVar(&flagClientOpt.Console, "console", false,
		"show log on console, it's conflict with '--showbar'")
	flagSet.BoolVar(&flagClientOpt.Verbose, "verbose", false,
		"enable verbose mode, all debug log will be display")

	flagSet.StringVar(&flagDaemonOpt.WorkHome, "home", flagDaemonOpt.WorkHome,
		"the work home directory of dfget")

	//pass to peer server which as a uploader server
	flagSet.StringVar(&flagDaemonOpt.Host.ListenIP, "ip", flagDaemonOpt.Host.ListenIP,
		"IP address that server will listen on")
	flagSet.IntVar(&flagDaemonOpt.Upload.ListenOption.TCPListen.PortRange.Start, "port", flagDaemonOpt.Upload.ListenOption.TCPListen.PortRange.Start,
		"port number that server will listen on")
	flagSet.DurationVar(&flagDaemonOpt.Storage.Option.TaskExpireTime.Duration, "expiretime", flagDaemonOpt.Storage.Option.TaskExpireTime.Duration,
		"caching duration for which cached file keeps no accessed by any process, after this period cache file will be deleted")
	flagSet.DurationVar(&flagDaemonOpt.AliveTime.Duration, "alivetime", flagDaemonOpt.AliveTime.Duration,
		"alive duration for which uploader keeps no accessing by any uploading requests, after this period uploader will automatically exit")

	flagSet.MarkDeprecated("exceed", "please use '--timeout' or '-e' instead")
	//flagSet.MarkDeprecated("notbs", "please use '--notbacksource' instead")

	flagSet.StringVar(&flagDaemonOpt.Download.DownloadGRPC.UnixListen.Socket, "daemon-sock",
		flagDaemonOpt.Download.DownloadGRPC.UnixListen.Socket, "the unix domain socket address for grpc with daemon")
	flagSet.StringVar(&flagDaemonOpt.PidFile, "daemon-pid", flagDaemonOpt.PidFile, "the daemon pid")
	flagSet.Var(config.NewSchedulersValue(&flagDaemonOpt), "schedulers", "the scheduler addresses")

	flagSet.StringVar(&flagClientOpt.MoreDaemonOptions, "more-daemon-options", "",
		"more options passed to daemon by command line, please confirm your options with \"dfget daemon --help\"")
}
