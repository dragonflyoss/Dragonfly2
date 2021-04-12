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
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/avast/retry-go"
	"github.com/go-echarts/statsview"
	"github.com/go-echarts/statsview/viewer"
	"github.com/go-http-utils/headers"
	"github.com/gofrs/flock"
	"github.com/phayes/freeport"
	"github.com/spf13/cobra"
	"go.uber.org/zap/zapcore"

	"d7y.io/dragonfly/v2/cdnsystem/source"
	"d7y.io/dragonfly/v2/client/clientutil/progressbar"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/pidfile"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/dflog/logcore"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	dfdaemongrpc "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	_ "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	dfclient "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	"d7y.io/dragonfly/v2/version"
)

var filter string

var dfgetConfig *config.ClientOption

// dfgetDescription is used to describe dfget command in details.
var dfgetDescription = `dfget is the client of Dragonfly which takes a role of peer in a P2P network.
When user triggers a file downloading task, dfget will download the pieces of
file from other peers. Meanwhile, it will act as an uploader to support other
peers to download pieces from it if it owns them. In addition, dfget has the
abilities to provide more advanced functionality, such as network bandwidth
limit, transmission encryption and so on.`

// dfgetExample shows examples in dfget command, and is used in auto-generated cli docs.
var dfgetExample = `
$ dfget -u https://www.taobao.com -o /tmp/test/b.test --expiretime 20s
--2019-02-02 18:56:34--  https://www.taobao.com
dfget version:0.3.0
workspace:/root/.dragonfly
sign:96414-1549104994.143
client:127.0.0.1 connected to node:127.0.0.1
start download by dragonfly...
download SUCCESS cost:0.026s length:141898 reason:0
`

var deprecatedFlags struct {
	nodes   config.SupernodesValue
	version bool

	commonString string
	commonBool   bool
	commonInt    int
}

var rootCmd = &cobra.Command{
	Use:               "dfget",
	Short:             "client of Dragonfly used to download and upload files",
	SilenceUsage:      true,
	Long:              dfgetDescription,
	DisableAutoGenTag: true, // disable displaying auto generation tag in cli docs
	Example:           dfgetExample,
	RunE: func(cmd *cobra.Command, args []string) error {
		if deprecatedFlags.version {
			version.VersionCmd.Run(nil, nil)
			return nil
		}
		// Convent deprecated flags
		convertDeprecatedFlags()

		// Dfget config validate
		if err := dfgetConfig.Validate(); err != nil {
			return err
		}

		// Init logger
		logcore.InitDfget(dfgetConfig.Console)

		// Start dfget
		return runDfget()
	},
}

// Execute will process dfget.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		logger.Errorf("Execute error: %s", err)
		os.Exit(1)
	}
}

func init() {
	// Initialize default dfget config
	dfgetConfig = &config.DfgetConfig

	// Add flags
	flagSet := rootCmd.Flags()
	persistentflagSet := rootCmd.PersistentFlags()

	flagSet.StringVarP(&dfgetConfig.URL, "url", "u", "", "URL of user requested downloading file(only HTTP/HTTPs supported)")
	flagSet.StringVarP(&dfgetConfig.Output, "output", "o", "",
		"destination path which is used to store the requested downloading file. It must contain detailed directory and specific filename, for example, '/tmp/file.mp4'")
	flagSet.StringVarP(&dfgetConfig.Output, "", "O", "", "Deprecated, keep for backward compatibility, use --output or -o instead")
	flagSet.Var(config.NewLimitRateValue(&daemonConfig.Download.TotalRateLimit), "totallimit",
		"network bandwidth rate limit for the whole host, in format of G(B)/g/M(B)/m/K(B)/k/B, pure number will also be parsed as Byte")
	flagSet.VarP(config.NewDurationValue(&dfgetConfig.Timeout), "timeout", "e",
		"timeout for file downloading task. If dfget has not finished downloading all pieces of file before --timeout, the dfget will throw an error and exit")
	flagSet.Var(config.NewDurationValue(&dfgetConfig.Timeout), "exceed",
		"timeout for file downloading task. If dfget has not finished downloading all pieces of file before --timeout, the dfget will throw an error and exit")
	flagSet.StringVarP(&dfgetConfig.Md5, "md5", "m", "",
		"md5 value input from user for the requested downloading file to enhance security")
	flagSet.StringVarP(&dfgetConfig.Identifier, "identifier", "i", "",
		"the usage of identifier is making different downloading tasks generate different downloading task IDs even if they have the same URLs. conflict with --md5.")
	flagSet.StringVar(&dfgetConfig.CallSystem, "callsystem", "",
		"the name of dfget caller which is for debugging. Once set, it will be passed to all components around the request to make debugging easy")
	flagSet.StringSliceVar(&dfgetConfig.Cacerts, "cacerts", nil,
		"the cacert file which is used to verify remote server when supernode interact with the source.")
	flagSet.StringVarP(&dfgetConfig.Pattern, "pattern", "p", "p2p",
		"download pattern, must be p2p/cdn/source, cdn and source do not support flag --totallimit")
	flagSet.StringVarP(&filter, "filter", "f", "",
		"filter some query params of URL, use char '&' to separate different params"+
			"\neg: -f 'key&sign' will filter 'key' and 'sign' query param"+
			"\nin this way, different but actually the same URLs can reuse the same downloading task")
	flagSet.StringArrayVar(&dfgetConfig.Header, "header", nil,
		"http header, eg: --header='Accept: *' --header='Host: abc'")
	flagSet.VarP(&deprecatedFlags.nodes, "node", "n",
		"deprecated, please use schedulers instead. specify the addresses(host:port=weight) of supernodes where the host is necessary, the port(default: 8002) and the weight(default:1) are optional. And the type of weight must be integer")
	flagSet.BoolVar(&dfgetConfig.NotBackSource, "notbacksource", false,
		"disable back source downloading for requested file when p2p fails to download it")
	flagSet.BoolVar(&dfgetConfig.NotBackSource, "notbs", false,
		"disable back source downloading for requested file when p2p fails to download it")
	flagSet.BoolVar(&deprecatedFlags.commonBool, "dfdaemon", false,
		"identify whether the request is from dfdaemon")
	flagSet.BoolVar(&dfgetConfig.Insecure, "insecure", false,
		"identify whether supernode should skip secure verify when interact with the source.")
	flagSet.IntVar(&deprecatedFlags.commonInt, "clientqueue", 0,
		"specify the size of client queue which controls the number of pieces that can be processed simultaneously")
	flagSet.BoolVarP(&dfgetConfig.ShowBar, "showbar", "b", false,
		"show progress bar, it is conflict with '--console'")
	flagSet.BoolVar(&dfgetConfig.Console, "console", false,
		"show log on console, it's conflict with '--showbar'")
	flagSet.BoolVar(&dfgetConfig.Verbose, "verbose", false,
		"enable verbose mode, all debug log will be display")
	persistentflagSet.StringVar(&daemonConfig.WorkHome, "home", daemonConfig.WorkHome,
		"the work home directory")
	persistentflagSet.StringVar(&daemonConfig.Host.ListenIP, "ip", daemonConfig.Host.ListenIP,
		"IP address that server will listen on")
	flagSet.IntVar(&daemonConfig.Upload.ListenOption.TCPListen.PortRange.Start, "port", daemonConfig.Upload.ListenOption.TCPListen.PortRange.Start,
		"port number that server will listen on")
	persistentflagSet.DurationVar(&daemonConfig.Storage.TaskExpireTime.Duration, "expiretime", daemonConfig.Storage.TaskExpireTime.Duration,
		"caching duration for which cached file keeps no accessed by any process, after this period cache file will be deleted")
	persistentflagSet.DurationVar(&daemonConfig.AliveTime.Duration, "alivetime", daemonConfig.AliveTime.Duration,
		"alive duration for which uploader keeps no accessing by any uploading requests, after this period uploader will automatically exit")
	flagSet.StringVar(&daemonConfig.Download.DownloadGRPC.UnixListen.Socket, "daemon-sock",
		daemonConfig.Download.DownloadGRPC.UnixListen.Socket, "the unix domain socket address for grpc with daemon")
	flagSet.StringVar(&daemonConfig.PidFile, "daemon-pid", daemonConfig.PidFile, "the daemon pid")
	persistentflagSet.VarP(config.NewNetAddrsValue(&daemonConfig.Scheduler.NetAddrs), "scheduler", "s", "the scheduler addresses")
	flagSet.StringVar(&dfgetConfig.MoreDaemonOptions, "more-daemon-options", "",
		"more options passed to daemon by command line, please confirm your options with \"dfget daemon --help\"")

	// backward compatibility
	flagSet.BoolVar(&deprecatedFlags.commonBool, "cachefirst", false, "deprecated")
	flagSet.BoolVar(&deprecatedFlags.commonBool, "check", false, "deprecated")
	flagSet.BoolVar(&deprecatedFlags.commonBool, "createmeta", false, "deprecated")
	flagSet.BoolVar(&deprecatedFlags.commonBool, "notmd5", false, "deprecated")
	flagSet.BoolVar(&deprecatedFlags.commonBool, "showcenter", false, "deprecated")
	flagSet.BoolVar(&deprecatedFlags.commonBool, "usewrap", false, "deprecated")

	flagSet.StringVar(&deprecatedFlags.commonString, "locallimit", "", "deprecated")
	flagSet.StringVar(&deprecatedFlags.commonString, "minrate", "", "deprecated")
	flagSet.StringVarP(&deprecatedFlags.commonString, "tasktype", "t", "", "deprecated")
	flagSet.StringVarP(&deprecatedFlags.commonString, "center", "c", "", "deprecated")

	flagSet.BoolVarP(&deprecatedFlags.version, "version", "v", false, "deprecated")

	flagSet.MarkDeprecated("exceed", "please use '--timeout' or '-e' instead")
	flagSet.MarkDeprecated("clientqueue", "controlled by Manager and Scheduler")
	flagSet.MarkDeprecated("dfdaemon", "not used anymore")
	flagSet.MarkDeprecated("version", "Please use 'dfget version' instead")
	flagSet.MarkShorthandDeprecated("v", "Please use 'dfget version' instead")
	// Add command
	rootCmd.AddCommand(version.VersionCmd)
}

// Convert flags
func convertDeprecatedFlags() {
	for _, node := range deprecatedFlags.nodes.Nodes {
		daemonConfig.Scheduler.NetAddrs = append(daemonConfig.Scheduler.NetAddrs, dfnet.NetAddr{
			Type: dfnet.TCP,
			Addr: node,
		})
	}
}

// runDfget does some init operations and starts to download.
func runDfget() error {
	var addr = dfnet.NetAddr{
		Type: dfnet.UNIX,
		Addr: daemonConfig.Download.DownloadGRPC.UnixListen.Socket,
	}
	var (
		ctx    = context.Background()
		cancel context.CancelFunc
		hdr    = parseHeader(dfgetConfig.Header)
	)

	// Initialize verbose mode
	initVerboseMode(dfgetConfig.Verbose)

	// Check df daemon state, start a new daemon if necessary
	daemonClient, err := checkAndSpawnDaemon(addr)
	if err != nil {
		return downloadFromSource(hdr)
	}

	output, err := filepath.Abs(dfgetConfig.Output)
	if err != nil {
		return err
	}

	if dfgetConfig.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, dfgetConfig.Timeout)
		defer cancel()
	} else {
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
	}

	request := &dfdaemongrpc.DownRequest{
		Url: dfgetConfig.URL,
		UrlMeta: &base.UrlMeta{
			Md5:    dfgetConfig.Md5,
			Range:  hdr[headers.Range],
			Header: hdr,
		},
		Output: output,
		BizId:  dfgetConfig.CallSystem,
		Filter: filter,
	}
	var (
		start = time.Now()
		end   time.Time
	)
	down, err := daemonClient.Download(ctx, request)
	if err != nil {
		return err
	}
	var (
		result *dfdaemongrpc.DownResult
	)
	pb := progressbar.DefaultBytes(-1, "downloading")
	for {
		result, err = down.Recv()
		if err != nil {
			if de, ok := err.(*dferrors.DfError); ok {
				logger.Errorf("dragonfly daemon returns error code %d/%s", de.Code, de.Message)
			} else {
				logger.Errorf("dragonfly daemon returns error %s", err)
			}
			break
		}
		if result.CompletedLength > 0 {
			pb.Set64(int64(result.CompletedLength))
		}
		if result.Done {
			pb.Finish()
			end = time.Now()
			fmt.Printf("time cost: %dms\n", end.Sub(start).Milliseconds())
			break
		}
	}
	if err != nil {
		start = time.Now()
		logger.Errorf("download by dragonfly error: %s", err)
		if !dfgetConfig.NotBackSource {
			return downloadFromSource(hdr)
		} else {
			logger.Warnf("back source disabled")
		}
	}
	return err
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

func downloadFromSource(hdr map[string]string) (err error) {
	logger.Infof("try to download from source")
	var (
		resourceClient source.ResourceClient
		target         *os.File
		response       io.ReadCloser
		_              map[string]string
		written        int64
	)

	resourceClient, err = source.NewSourceClient()
	if err != nil {
		logger.Errorf("init source client error: %s", err)
		return err
	}

	response, _, err = resourceClient.Download(dfgetConfig.URL, hdr)
	if err != nil {
		logger.Errorf("download from source error: %s", err)
		return err
	}
	defer response.Close()

	target, err = os.OpenFile(dfgetConfig.Output, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		logger.Errorf("open %s error: %s", dfgetConfig.Output)
		return err
	}

	written, err = io.Copy(target, response)
	if err != nil {
		logger.Errorf("copied %d bytes to %s, with error: %s",
			written, dfgetConfig.Output, err)
	} else {
		logger.Infof("copied %d bytes to %s", written, dfgetConfig.Output)
	}
	return err
}

func checkAndSpawnDaemon(addr dfnet.NetAddr) (dfclient.DaemonClient, error) {
	// Check pid
	if ok, err := pidfile.IsProcessExistsByPIDFile(daemonConfig.PidFile); err != nil || !ok {
		if err = spawnDaemon(); err != nil {
			return nil, fmt.Errorf("start daemon error: %s", err)
		}
	}

	// Check socket
	_, err := os.Stat(addr.Addr)
	if os.IsNotExist(err) {
		if err = spawnDaemon(); err != nil {
			return nil, fmt.Errorf("start daemon error: %s", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("unknown error when stat daemon socket: %s", err)
	}

	// Check daemon health
	var dc dfclient.DaemonClient
	err = retry.Do(func() error {
		dc, err = probeDaemon(addr)
		return err
	})
	return dc, err
}

func probeDaemon(addr dfnet.NetAddr) (dfclient.DaemonClient, error) {
	dc, err := dfclient.GetClientByAddr([]dfnet.NetAddr{addr})
	if err != nil {
		return nil, err
	}

	err = dc.CheckHealth(context.Background(), addr)
	if err != nil {
		return nil, err
	}

	return dc, nil
}

func spawnDaemon() error {
	// Initialize lock file
	lock := flock.New(dfgetConfig.LockFile)
	lock.Lock()
	defer lock.Unlock()

	// Initialize daemon args
	var args = []string{
		"daemon",
		"--download-rate", fmt.Sprintf("%f", daemonConfig.Download.TotalRateLimit.Limit),
		"--upload-port", fmt.Sprintf("%d", daemonConfig.Upload.TCPListen.PortRange.Start),
		"--home", daemonConfig.WorkHome,
		"--ip", daemonConfig.Host.ListenIP,
		"--expiretime", daemonConfig.Storage.TaskExpireTime.String(),
		"--alivetime", daemonConfig.AliveTime.String(),
		"--grpc-unix-listen", daemonConfig.Download.DownloadGRPC.UnixListen.Socket,
		"--pid", daemonConfig.PidFile,
	}

	// Set more daemon options
	if dfgetConfig.MoreDaemonOptions != "" {
		args = append(args, strings.Split(dfgetConfig.MoreDaemonOptions, " ")...)
	}
	cmd := exec.Command(os.Args[0], args...)

	// Set verbose
	if dfgetConfig.Verbose {
		cmd.Args = append(cmd.Args, "--verbose")
	}

	// Set scheduler
	for _, s := range daemonConfig.Scheduler.NetAddrs {
		cmd.Args = append(cmd.Args, "--scheduler", s.Addr)
	}

	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	logger.Infof("start daemon with cmd: %s", strings.Join(cmd.Args, " "))
	return cmd.Start()
}

func parseHeader(s []string) map[string]string {
	hdr := map[string]string{}
	for _, h := range s {
		idx := strings.Index(h, ":")
		if idx > 0 {
			hdr[h[:idx]] = strings.TrimLeft(h[idx:], " ")
		}
	}
	return hdr
}
