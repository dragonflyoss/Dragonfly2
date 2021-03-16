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
	"strings"
	"syscall"

	"github.com/avast/retry-go"
	"github.com/go-http-utils/headers"
	"github.com/gofrs/flock"
	"github.com/spf13/cobra"

	"d7y.io/dragonfly/v2/cdnsystem/source"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/client/clientutil/progressbar"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/pidfile"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
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

var flagClientOpt = config.NewClientConfig()

// dfgetDescription is used to describe dfget command in details.
var dfgetDescription = `dfget is the client of Dragonfly which takes a role of peer in a P2P network.
When user triggers a file downloading task, dfget will download the pieces of
file from other peers. Meanwhile, it will act as an uploader to support other
peers to download pieces from it if it owns them. In addition, dfget has the
abilities to provide more advanced functionality, such as network bandwidth
limit, transmission encryption and so on.`

var rootCmd = &cobra.Command{
	Use:               "dfget",
	Short:             "client of Dragonfly used to download and upload files",
	SilenceUsage:      true,
	Long:              dfgetDescription,
	DisableAutoGenTag: true, // disable displaying auto generation tag in cli docs
	Example:           dfgetExample(),
	RunE: func(cmd *cobra.Command, args []string) error {
		// init logger
		logcore.InitDfget(flagClientOpt.Console)
		if err := checkClientOptions(); err != nil {
			return err
		}
		return runDfget()
	},
}

func init() {
	rootCmd.AddCommand(version.VersionCmd)
	initRootFlags()
}

// runDfget does some init operations and starts to download.
func runDfget() error {
	var addr = dfnet.NetAddr{
		Type: dfnet.UNIX,
		Addr: flagDaemonOpt.Download.DownloadGRPC.UnixListen.Socket,
	}
	var (
		ctx    = context.Background()
		cancel context.CancelFunc
		hdr    = parseHeader(flagClientOpt.Header)
	)

	// check df daemon state, start a new daemon if necessary
	daemonClient, err := checkAndSpawnDaemon(addr)
	if err != nil {
		return downloadFromSource(hdr)
	}
	output, err := filepath.Abs(flagClientOpt.Output)
	if err != nil {
		return err
	}
	if flagClientOpt.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, flagClientOpt.Timeout)
		defer cancel()
	} else {
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
	}
	request := &dfdaemongrpc.DownRequest{
		Url: flagClientOpt.URL,
		UrlMeta: &base.UrlMeta{
			Md5:    flagClientOpt.Md5,
			Range:  hdr[headers.Range],
			Header: hdr,
		},
		Output: output,
		BizId:  flagClientOpt.CallSystem,
		Filter: filter,
	}
	down, err := daemonClient.Download(ctx, request)
	if err != nil {
		return err
	}
	var (
		result *dfdaemongrpc.DownResult
		ok     bool
	)
	pb := progressbar.DefaultBytes(-1, "downloading")
download:
	for {
		select {
		case result, ok = <-down:
			if !ok && request == nil {
				err = fmt.Errorf("progress channel closed unexpected")
				break download
			}
			if result.CompletedLength > 0 {
				pb.Set64(int64(result.CompletedLength))
			}
			if result.State.Code != dfcodes.Success {
				err = fmt.Errorf("dragonfly daemon returns error code %d/%s",
					result.State.Code, result.State.Msg)
				break download
			}
			if result.Done {
				pb.Finish()
				break download
			}
		case <-ctx.Done():
			logger.Errorf("content done due to: %s", ctx.Err())
			return ctx.Err()
		}
	}
	if err != nil {
		logger.Errorf("download by dragonfly error: %s", err)
		if !flagClientOpt.NotBackSource {
			return downloadFromSource(hdr)
		} else {
			logger.Warnf("back source disabled")
		}
	}
	return err
}

func downloadFromSource(hdr map[string]string) (err error) {
	logger.Infof("try to download from source")
	var (
		resourceClient source.ResourceClient
		target         *os.File
		response       *types.DownloadResponse
		written        int64
	)

	resourceClient, err = source.NewSourceClient()
	if err != nil {
		logger.Errorf("init source client error: %s", err)
		return err
	}
	response, err = resourceClient.Download(flagClientOpt.URL, hdr)
	if err != nil {
		logger.Errorf("download from source error: %s", err)
		return err
	}
	defer response.Body.Close()
	target, err = os.OpenFile(flagClientOpt.Output, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		logger.Errorf("open %s error: %s", flagClientOpt.Output)
		return err
	}
	written, err = io.Copy(target, response.Body)
	if err != nil {
		logger.Errorf("copied %d bytes to %s, with error: %s",
			written, flagClientOpt.Output, err)
	} else {
		logger.Infof("copied %d bytes to %s", written, flagClientOpt.Output)
	}
	return err
}

func convertDeprecatedFlags() {
	for _, node := range deprecatedFlags.nodes.Nodes {
		flagDaemonOpt.Scheduler.NetAddrs = append(flagDaemonOpt.Scheduler.NetAddrs, dfnet.NetAddr{
			Type: dfnet.TCP,
			Addr: node,
		})
	}
}

func checkClientOptions() error {
	convertDeprecatedFlags()
	if len(os.Args) < 2 {
		return dferrors.New(-1, "Please use the command 'help' to show the help information.")
	}
	if err := config.CheckConfig(flagClientOpt); err != nil {
		return err
	}
	if len(flagDaemonOpt.Scheduler.NetAddrs) < 1 {
		return dferrors.New(-1, "Empty schedulers. Please use the command 'help' to show the help information.")
	}
	return nil
}

// Execute will process dfget.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		logger.Errorf("Execute error: %s", err)
		os.Exit(1)
	}
}

// dfgetExample shows examples in dfget command, and is used in auto-generated cli docs.
func dfgetExample() string {
	return `
$ dfget -u https://www.taobao.com -o /tmp/test/b.test --notbs --expiretime 20s
--2019-02-02 18:56:34--  https://www.taobao.com
dfget version:0.3.0
workspace:/root/.dragonfly
sign:96414-1549104994.143
client:127.0.0.1 connected to node:127.0.0.1
start download by dragonfly...
download SUCCESS cost:0.026s length:141898 reason:0
`
}

func checkAndSpawnDaemon(addr dfnet.NetAddr) (dfclient.DaemonClient, error) {
	// check pid
	if ok, err := pidfile.IsProcessExistsByPIDFile(flagDaemonOpt.PidFile); err != nil || !ok {
		if err = spawnDaemon(); err != nil {
			return nil, fmt.Errorf("start daemon error: %s", err)
		}
	}
	// check socket
	_, err := os.Stat(addr.Addr)
	if os.IsNotExist(err) {
		if err = spawnDaemon(); err != nil {
			return nil, fmt.Errorf("start daemon error: %s", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("unknown error when stat daemon socket: %s", err)
	}

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
	state, err := dc.CheckHealth(context.Background(), addr)
	if err != nil {
		//dc.Close()
		return nil, err
	}
	if !state.Success {
		//dc.Close()
		return nil, fmt.Errorf("check health error: %s/%s", state.Code, state.Msg)
	}
	return dc, nil
}

func spawnDaemon() error {
	lock := flock.New(dfgetLockFile)
	lock.Lock()
	defer lock.Unlock()

	var schedulers []string
	for _, s := range flagDaemonOpt.Scheduler.NetAddrs {
		schedulers = append(schedulers, s.Addr)
	}

	var args = []string{
		"daemon",
		"--download-rate", fmt.Sprintf("%f", flagDaemonOpt.Download.RateLimit.Limit),
		"--upload-port", fmt.Sprintf("%d", flagDaemonOpt.Upload.TCPListen.PortRange.Start),
		"--home", flagDaemonOpt.WorkHome,
		"--listen", flagDaemonOpt.Host.ListenIP,
		"--expire-time", flagDaemonOpt.Storage.TaskExpireTime.String(),
		"--alive-time", flagDaemonOpt.AliveTime.String(),
		"--grpc-unix-listen", flagDaemonOpt.Download.DownloadGRPC.UnixListen.Socket,
		"--schedulers", strings.Join(schedulers, ","),
		"--pid", flagDaemonOpt.PidFile,
	}
	if flagClientOpt.MoreDaemonOptions != "" {
		args = append(args, strings.Split(flagClientOpt.MoreDaemonOptions, " ")...)
	}
	logger.Infof("start daemon with cmd: %s %s", os.Args[0], strings.Join(args, " "))
	cmd := exec.Command(os.Args[0], args...)
	if flagClientOpt.Verbose {
		cmd.Args = append(cmd.Args, "--verbose")
	}

	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
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
