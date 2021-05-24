/*
 *     Copyright 2020 The Dragonfly Authors
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
	"strconv"
	"strings"
	"syscall"
	"time"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/dfget"
	"d7y.io/dragonfly/v2/cmd/dependency"
	"d7y.io/dragonfly/v2/internal/dfpath"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/dflog/logcore"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	"d7y.io/dragonfly/v2/pkg/unit"
	"github.com/gofrs/flock"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

var (
	dfgetConfig *config.DfgetConfig
)

var dfgetDescription = `dfget is the client of dragonfly which takes a role of peer in a P2P network.
When user triggers a file downloading task, dfget will download the pieces of
file from other peers. Meanwhile, it will act as an uploader to support other
peers to download pieces from it if it owns them. In addition, dfget has the
abilities to provide more advanced functionality, such as network bandwidth
limit, transmission encryption and so on.`

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:                "dfget url -O path",
	Short:              "the P2P client of dragonfly",
	Long:               dfgetDescription,
	Args:               cobra.MaximumNArgs(1),
	DisableAutoGenTag:  true,
	FParseErrWhitelist: cobra.FParseErrWhitelist{UnknownFlags: true},
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := logcore.InitDfget(dfgetConfig.Console); err != nil {
			return errors.Wrap(err, "init client dfget logger")
		}

		// Convert config
		if err := dfgetConfig.Convert(args); err != nil {
			return err
		}

		// Validate config
		if err := dfgetConfig.Validate(); err != nil {
			return err
		}

		//  do get file
		return runDfget()
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		logger.Error(err)
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	// Initialize default dfget config
	dfgetConfig = config.NewDfgetConfig()
	// Initialize cobra
	dependency.InitCobra(rootCmd, false, dfgetConfig)

	// Add flags
	flagSet := rootCmd.Flags()

	flagSet.StringP("url", "u", dfgetConfig.URL,
		"download a file from the url, equivalent to the command's first position argument")

	flagSet.StringP("output", "O", dfgetConfig.Output,
		"destination path which is used to store the downloaded file. It must be a full path, for example, '/tmp/file.mp4'")

	flagSet.DurationP("timeout", "e", dfgetConfig.Timeout,
		"timeout for file downloading task. If dfget has not finished downloading all pieces of file "+
			"before --timeout, the dfget will throw an error and exit, 0 is infinite")

	flagSet.String("limit", unit.Bytes(dfgetConfig.RateLimit).String(),
		"network bandwidth rate limit in format of G(B)/g/M(B)/m/K(B)/k/B, pure number will be parsed as Byte, 0 is infinite")

	flagSet.String("digest", dfgetConfig.Digest,
		"digest is used to check the integrity of the downloaded file, in format of md5:xxx or sha256:yyy")

	flagSet.StringP("identifier", "i", dfgetConfig.Identifier,
		"different identifiers for the same url will be divided into different P2P tasks, it conflicts with --digest")

	flagSet.StringP("filter", "f", strings.Join(dfgetConfig.Filter, "&"),
		"filter some query params of url, use char '&' to separate different params, eg: -f 'key&sign' "+
			"will filter 'key' and 'sign' query param. in this way, different urls can correspond to the same P2P task")

	flagSet.Bool("disable-back-source", dfgetConfig.DisableBackSource,
		"disable dfget downloading file directly from url source when peer fails to download file, "+
			"--limit is invalid when peer downloads the file from source")

	flagSet.StringP("pattern", "p", dfgetConfig.Pattern, "downloading pattern, must be p2p/cdn/source")

	flagSet.StringArrayP("header", "H", dfgetConfig.Header, "url header, eg: --header='Accept: *' --header='Host: abc'")

	flagSet.StringArray("cacerts", dfgetConfig.Cacerts,
		"cacert files is used to verify CA for remote server when dragonfly interacts with the url source")

	flagSet.Bool("insecure", dfgetConfig.Insecure,
		"identify whether dragonfly should skip CA verification for remote server when it interacts with the url source")

	flagSet.BoolP("show-progress", "b", dfgetConfig.ShowBar, "show progress bar, it conflicts with --console")

	flagSet.String("callsystem", dfgetConfig.CallSystem, "the system name of dfget caller which is mainly used for statistics and access control")

	// Bind cmd flags
	if err := viper.BindPFlags(flagSet); err != nil {
		panic(errors.Wrap(err, "bind dfget flags to viper"))
	}
}

// runDfget does some init operations and starts to download.
func runDfget() error {
	// Dfget config values
	s, _ := yaml.Marshal(dfgetConfig)
	logger.Infof("client dfget configuration:\n%s", string(s))

	ff := dependency.InitMonitor(dfgetConfig.Verbose, dfgetConfig.PProfPort, dfgetConfig.Jaeger)
	defer ff()

	logger.Info("start to check and spawn daemon")
	daemonClient, err := checkAndSpawnDaemon()
	if err != nil {
		logger.Errorf("check and spawn daemon error:%v", err)
	}

	logger.Info("check and spawn daemon success")
	return dfget.Download(dfgetConfig, daemonClient)
}

// checkAndSpawnDaemon do checking at four checkpoints
func checkAndSpawnDaemon() (client.DaemonClient, error) {
	target := dfnet.NetAddr{Type: dfnet.UNIX, Addr: dfpath.DaemonSockPath}
	daemonClient, err := client.GetClientByAddr([]dfnet.NetAddr{target})
	if err != nil {
		return nil, err
	}

	// 1.Check without lock
	if daemonClient.CheckHealth(context.Background(), target) == nil {
		return daemonClient, nil
	}

	lock := flock.New(dfpath.DfgetLockPath)
	lock.Lock()
	defer lock.Unlock()

	// 2.Check with lock
	if daemonClient.CheckHealth(context.Background(), target) == nil {
		return daemonClient, nil
	}

	cmd := exec.Command(os.Args[0], "daemon", "--launcher", strconv.Itoa(os.Getpid()))
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	logger.Info("do start daemon")

	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	// 3.First check since starting
	if daemonClient.CheckHealth(context.Background(), target) == nil {
		return daemonClient, nil
	}

	times := 0
	limit := 100
	interval := 50 * time.Millisecond
	for {
		// 4.Cycle check with 5s timeout
		if daemonClient.CheckHealth(context.Background(), target) == nil {
			return daemonClient, nil
		}

		times++
		if times > limit {
			return nil, errors.New("the daemon is unhealthy")
		}
		time.Sleep(interval)
	}
}
