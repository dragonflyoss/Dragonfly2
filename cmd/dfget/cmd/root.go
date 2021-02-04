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
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/avast/retry-go"
	"github.com/gofrs/flock"
	"github.com/spf13/cobra"

	"github.com/dragonflyoss/Dragonfly/v2/client/config"
	"github.com/dragonflyoss/Dragonfly/v2/pkg/basic/dfnet"
	"github.com/dragonflyoss/Dragonfly/v2/pkg/dferrors"
	logger "github.com/dragonflyoss/Dragonfly/v2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly/v2/pkg/rpc/base"
	dfdaemongrpc "github.com/dragonflyoss/Dragonfly/v2/pkg/rpc/dfdaemon"
	_ "github.com/dragonflyoss/Dragonfly/v2/pkg/rpc/dfdaemon/client"
	dfclient "github.com/dragonflyoss/Dragonfly/v2/pkg/rpc/dfdaemon/client"
	"github.com/dragonflyoss/Dragonfly/v2/pkg/util/pidfile"
	"github.com/dragonflyoss/Dragonfly/v2/pkg/util/progressbar"
	"github.com/dragonflyoss/Dragonfly/v2/pkg/util/stringutils"
)

type loadGlobalConfigResult struct {
	prop     *config.GlobalConfig
	fileName string
	err      error
}

var filter string

var cfg = config.NewConfig()

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
		logger.InitDfget()
		if err := checkParameters(); err != nil {
			return err
		}
		return runDfget()
	},
}

func init() {
	initRootFlags()
}

// runDfget does some init operations and starts to download.
func runDfget() error {
	var addr = dfnet.NetAddr{
		Type: dfnet.UNIX,
		Addr: flagDfGetOpt.daemonSock,
	}

	// check df daemon state, start a new daemon if necessary
	client, err := checkAndSpawnDaemon(addr)
	if err != nil {
		// FIXME(jim): back source
		return err
	}
	output, err := filepath.Abs(cfg.Output)
	if err != nil {
		return err
	}
	request := &dfdaemongrpc.DownRequest{
		Url:    cfg.URL,
		Output: output,
		BizId:  "d7s/dfget",
		Filter: filter,
	}
	down, err := client.Download(context.Background(), request)
	if err != nil {
		return err
	}
	defer client.Close()
	var result *dfdaemongrpc.DownResult
	pb := progressbar.DefaultBytes(-1, "downloading")
	for result = range down {
		if result.CompletedLength > 0 {
			pb.Set64(int64(result.CompletedLength))
		}
		if !result.Done {
			continue
		}
		switch result.State.Code {
		case base.Code_SUCCESS:
			pb.Finish()
			return nil
		default:
			return fmt.Errorf("%s", result.State.GetMsg())
		}
		break
	}
	return nil
}

func checkParameters() error {
	if len(os.Args) < 2 {
		return dferrors.New(-1, "Please use the command 'help' to show the help information.")
	}
	if len(flagDfGetOpt.schedulers) < 1 {
		return dferrors.New(-1, "Empty schedulers. Please use the command 'help' to show the help information.")
	}
	return nil
}

// initGlobalConfig loads config from files.
func initGlobalConfig() ([]*loadGlobalConfigResult, error) {
	var results []*loadGlobalConfigResult
	properties := config.NewGlobalConfig()
	for _, v := range cfg.ConfigFiles {
		err := properties.Load(v)
		if err == nil {
			break
		}
		results = append(results, &loadGlobalConfigResult{
			prop:     properties,
			fileName: v,
			err:      err,
		})
	}

	supernodes := cfg.Supernodes
	if supernodes == nil {
		supernodes = properties.Supernodes
	}
	if supernodes != nil {
		cfg.Nodes = config.NodeWeightSlice2StringSlice(supernodes)
	}

	if cfg.LocalLimit == 0 {
		cfg.LocalLimit = properties.LocalLimit
	}

	if cfg.MinRate == 0 {
		cfg.MinRate = properties.MinRate
	}

	if cfg.TotalLimit == 0 {
		cfg.TotalLimit = properties.TotalLimit
	}

	if cfg.ClientQueueSize == 0 {
		cfg.ClientQueueSize = properties.ClientQueueSize
	}

	currentUser, err := user.Current()
	if err != nil {
		os.Exit(config.CodeGetUserError)
	}
	cfg.User = currentUser.Username
	if cfg.WorkHome == "" {
		cfg.WorkHome = properties.WorkHome
		if cfg.WorkHome == "" {
			cfg.WorkHome = filepath.Join(currentUser.HomeDir, ".small-dragonfly")
		}
	}
	cfg.RV.MetaPath = filepath.Join(cfg.WorkHome, "meta", "host.meta")
	cfg.RV.SystemDataDir = filepath.Join(cfg.WorkHome, "data")
	cfg.RV.FileLength = -1

	return results, nil
}

func transFilter(filter string) []string {
	if stringutils.IsEmptyStr(filter) {
		return nil
	}
	return strings.Split(filter, "&")
}

func resultMsg(cfg *config.Config, end time.Time, e *dferrors.DfError) string {
	if e != nil {
		return fmt.Sprintf("download FAIL(%d) cost:%.3fs length:%d reason:%d error:%v",
			e.Code, end.Sub(cfg.StartTime).Seconds(), cfg.RV.FileLength,
			cfg.BackSourceReason, e)
	}
	return fmt.Sprintf("download SUCCESS cost:%.3fs length:%d reason:%d",
		end.Sub(cfg.StartTime).Seconds(), cfg.RV.FileLength, cfg.BackSourceReason)
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
workspace:/root/.small-dragonfly
sign:96414-1549104994.143
client:127.0.0.1 connected to node:127.0.0.1
start download by dragonfly...
download SUCCESS cost:0.026s length:141898 reason:0
`
}

func checkAndSpawnDaemon(addr dfnet.NetAddr) (dfclient.DaemonClient, error) {
	// check pid
	if ok, err := pidfile.IsProcessExistsByPIDFile(flagDfGetOpt.daemonPid); err != nil || !ok {
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
	dc, err := dfclient.CreateClient([]dfnet.NetAddr{addr})
	if err != nil {
		return nil, err
	}
	state, err := dc.CheckHealth(context.Background())
	if err != nil {
		dc.Close()
		return nil, err
	}
	if !state.Success {
		dc.Close()
		return nil, fmt.Errorf("check health error: %s/%s", state.Code, state.Msg)
	}
	return dc, nil
}

func spawnDaemon() error {
	lock := flock.New(dfgetLockFile)
	lock.Lock()
	defer lock.Unlock()

	// FIXME(jim): customize by config file or flag
	var args = []string{
		"daemon",
		"--grpc-port", strconv.Itoa(cfg.RV.PeerPort),
		"--expire-time", cfg.RV.DataExpireTime.String(),
		"--alive-time", cfg.RV.DaemonAliveTime.String(),
		"--schedulers", strings.Join(flagDfGetOpt.schedulers, ",")}
	logger.Infof("start daemon with cmd: %s %s", os.Args[0], strings.Join(args, " "))
	cmd := exec.Command(os.Args[0], args...)
	if cfg.Verbose {
		cmd.Args = append(cmd.Args, "--verbose")
	}

	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	//var (
	//	stdout io.ReadCloser
	//	err    error
	//)
	//if stdout, err = cmd.StdoutPipe(); err != nil {
	//	return err
	//}
	return cmd.Start()
}
