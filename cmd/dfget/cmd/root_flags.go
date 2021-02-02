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

import "github.com/dragonflyoss/Dragonfly2/client/config"

type dfgetOption struct {
	daemonSock string
	daemonPid  string
	schedulers []string
}

func initRootFlags() {
	// pass to server
	flagSet := rootCmd.Flags()

	// url & output
	flagSet.StringVarP(&cfg.URL, "url", "u", "", "URL of user requested downloading file(only HTTP/HTTPs supported)")
	flagSet.StringVarP(&cfg.Output, "output", "o", "",
		"destination path which is used to store the requested downloading file. It must contain detailed directory and specific filename, for example, '/tmp/file.mp4'")

	// localLimit & minRate & totalLimit & timeout
	flagSet.VarP(&cfg.LocalLimit, "locallimit", "s",
		"network bandwidth rate limit for single download task, in format of G(B)/g/M(B)/m/K(B)/k/B, pure number will also be parsed as Byte")
	flagSet.Var(&cfg.MinRate, "minrate",
		"minimal network bandwidth rate for downloading a file, in format of G(B)/g/M(B)/m/K(B)/k/B, pure number will also be parsed as Byte")
	flagSet.Var(&cfg.TotalLimit, "totallimit",
		"network bandwidth rate limit for the whole host, in format of G(B)/g/M(B)/m/K(B)/k/B, pure number will also be parsed as Byte")
	flagSet.DurationVarP(&cfg.Timeout, "timeout", "e", 0,
		"timeout set for file downloading task. If dfget has not finished downloading all pieces of file before --timeout, the dfget will throw an error and exit")

	// md5 & identifier
	flagSet.StringVarP(&cfg.Md5, "md5", "m", "",
		"md5 value input from user for the requested downloading file to enhance security")
	flagSet.StringVarP(&cfg.Identifier, "identifier", "i", "",
		"the usage of identifier is making different downloading tasks generate different downloading task IDs even if they have the same URLs. conflict with --md5.")
	flagSet.StringVar(&cfg.CallSystem, "callsystem", "",
		"the name of dfget caller which is for debugging. Once set, it will be passed to all components around the request to make debugging easy")
	flagSet.StringSliceVar(&cfg.Cacerts, "cacerts", nil,
		"the cacert file which is used to verify remote server when supernode interact with the source.")
	flagSet.StringVarP(&cfg.Pattern, "pattern", "p", "p2p",
		"download pattern, must be p2p/cdn/source, cdn and source do not support flag --totallimit")
	flagSet.StringVarP(&filter, "filter", "f", "",
		"filter some query params of URL, use char '&' to separate different params"+
			"\neg: -f 'key&sign' will filter 'key' and 'sign' query param"+
			"\nin this way, different but actually the same URLs can reuse the same downloading task")
	flagSet.StringArrayVar(&cfg.Header, "header", nil,
		"http header, eg: --header='Accept: *' --header='Host: abc'")
	//flagSet.VarP(config.NewSupernodesValue(&cfg.Supernodes, nil), "node", "n",
	//	"specify the addresses(host:port=weight) of supernodes where the host is necessary, the port(default: 8002) and the weight(default:1) are optional. And the type of weight must be integer")
	flagSet.BoolVar(&cfg.NotBackSource, "notbacksource", false,
		"disable back source downloading for requested file when p2p fails to download it")

	//flagSet.BoolVar(&cfg.DFDaemon, "dfdaemon", false,
	//	"identify whether the request is from dfdaemon")
	flagSet.BoolVar(&cfg.Insecure, "insecure", false,
		"identify whether supernode should skip secure verify when interact with the source.")
	//flagSet.IntVar(&cfg.ClientQueueSize, "clientqueue", config.DefaultClientQueueSize,
	//	"specify the size of client queue which controls the number of pieces that can be processed simultaneously")

	// others
	flagSet.BoolVarP(&cfg.ShowBar, "showbar", "b", false,
		"show progress bar, it is conflict with '--console'")
	flagSet.BoolVar(&cfg.Console, "console", false,
		"show log on console, it's conflict with '--showbar'")
	flagSet.BoolVar(&cfg.Verbose, "verbose", false,
		"enable verbose mode, all debug log will be display")
	flagSet.StringVar(&cfg.WorkHome, "home", cfg.WorkHome,
		"the work home directory of dfget")

	// pass to peer server which as a uploader server
	//flagSet.StringVar(&cfg.RV.LocalIP, "ip", "",
	//	"IP address that server will listen on")
	//flagSet.IntVar(&cfg.RV.PeerPort, "port", 0,
	//	"port number that server will listen on")
	flagSet.DurationVar(&cfg.DataExpireTime, "expiretime", config.DataExpireTime,
		"caching duration for which cached file keeps no accessed by any process, after this period cache file will be deleted")
	flagSet.DurationVar(&cfg.DaemonAliveTime, "alivetime", config.DaemonAliveTime,
		"alive duration for which uploader keeps no accessing by any uploading requests, after this period uploader will automatically exit")

	flagSet.MarkDeprecated("exceed", "please use '--timeout' or '-e' instead")
	flagSet.MarkDeprecated("notbs", "please use '--notbacksource' instead")

	flagSet.StringVar(&flagDfGetOpt.daemonSock, "daemon-sock", flagDfGetOpt.daemonSock, "the unix domain socket address for grpc with daemon")
	flagSet.StringVar(&flagDfGetOpt.daemonPid, "daemon-pid", flagDfGetOpt.daemonPid, "the daemon pid")
	flagSet.StringArrayVar(&flagDfGetOpt.schedulers, "schedulers", flagDfGetOpt.schedulers, "the scheduler addresses")
}
