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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"syscall"
	"time"

	"github.com/gofrs/flock"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/dfget"
	"d7y.io/dragonfly/v2/cmd/dependency"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/dfpath"
	"d7y.io/dragonfly/v2/pkg/net/ip"
	"d7y.io/dragonfly/v2/pkg/os/user"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	"d7y.io/dragonfly/v2/pkg/source"
	_ "d7y.io/dragonfly/v2/pkg/source/loader" // register all source clients
	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/version"
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

// rootCmd represents the commonv1 command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:                "dfget url -O path",
	Short:              "the P2P client of dragonfly",
	Long:               dfgetDescription,
	Args:               cobra.MaximumNArgs(1),
	DisableAutoGenTag:  true,
	SilenceUsage:       true,
	FParseErrWhitelist: cobra.FParseErrWhitelist{UnknownFlags: true},
	RunE: func(cmd *cobra.Command, args []string) error {
		start := time.Now()

		// Convert config
		if err := dfgetConfig.Convert(args); err != nil {
			return err
		}

		// Validate config
		if err := dfgetConfig.Validate(); err != nil {
			return err
		}

		// Initialize daemon dfpath
		d, err := initDfgetDfpath(dfgetConfig)
		if err != nil {
			return err
		}

		rotateConfig := logger.LogRotateConfig{
			MaxSize:    dfgetConfig.LogMaxSize,
			MaxAge:     dfgetConfig.LogMaxAge,
			MaxBackups: dfgetConfig.LogMaxBackups}

		// Initialize logger
		if err := logger.InitDfget(dfgetConfig.Verbose, dfgetConfig.Console, d.LogDir(), rotateConfig); err != nil {
			return fmt.Errorf("init client dfget logger: %w", err)
		}

		// update plugin directory
		source.UpdatePluginDir(d.PluginDir())

		fmt.Printf("--%s--  %s\n", start.Format("2006-01-02 15:04:05"), dfgetConfig.URL)
		fmt.Printf("dfget version: %s\n", version.GitVersion)
		fmt.Printf("current user: %s, default peer ip: %s\n", user.Username(), ip.IPv4.String())
		fmt.Printf("output path: %s\n", dfgetConfig.Output)

		// do get file
		err = runDfget(cmd, d.DfgetLockPath(), d.DaemonSockPath())
		if err != nil {
			msg := fmt.Sprintf("download success: %t, cost: %d ms error: %s", false, time.Since(start).Milliseconds(), err.Error())
			logger.With("url", dfgetConfig.URL).Info(msg)
			fmt.Println(msg)
			return fmt.Errorf("download url %s: %w", dfgetConfig.URL, err)
		}

		msg := fmt.Sprintf("download success: %t, cost: %d ms", true, time.Since(start).Milliseconds())
		logger.With("url", dfgetConfig.URL).Info(msg)
		fmt.Println(msg)
		return nil
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		logger.Error(err)
		os.Exit(1)
	}
}

func init() {
	// Initialize default dfget config
	dfgetConfig = config.NewDfgetConfig()
	// Initialize command and config
	dependency.InitCommandAndConfig(rootCmd, false, dfgetConfig)

	// Add flags
	flagSet := rootCmd.Flags()

	flagSet.StringP("url", "u", dfgetConfig.URL,
		"Download one file from the url, equivalent to the command's first position argument")

	flagSet.StringP("output", "O", dfgetConfig.Output,
		"Destination path which is used to store the downloaded file, it must be a full path")

	flagSet.Duration("timeout", dfgetConfig.Timeout, "Timeout for the downloading task, 0 is infinite")

	flagSet.String("ratelimit", unit.Bytes(dfgetConfig.RateLimit.Limit).String(),
		"The downloading network bandwidth limit per second in format of G(B)/g/M(B)/m/K(B)/k/B, pure number will be parsed as Byte, 0 is infinite")

	flagSet.String("digest", dfgetConfig.Digest,
		"Check the integrity of the downloaded file with digest, in format of md5:xxx or sha256:yyy")

	flagSet.String("tag", dfgetConfig.Tag,
		"Different tags for the same url will be divided into different P2P overlay, it conflicts with --digest")

	flagSet.String("filter", dfgetConfig.Filter,
		"Filter the query parameters of the url, P2P overlay is the same one if the filtered url is same, "+
			"in format of key&sign, which will filter 'key' and 'sign' query parameters")

	flagSet.StringSliceP("header", "H", dfgetConfig.Header, "url header, eg: --header='Accept: *' --header='Host: abc'")

	flagSet.Bool("disable-back-source", dfgetConfig.DisableBackSource,
		"Disable downloading directly from source when the daemon fails to download file")

	flagSet.Int32P("priority", "P", dfgetConfig.Priority, "Scheduler will schedule task according to priority")

	flagSet.BoolP("show-progress", "b", dfgetConfig.ShowProgress, "Show progress bar, it conflicts with --console")

	flagSet.String("application", dfgetConfig.Application, "The caller name which is mainly used for statistics and access control")

	flagSet.String("daemon-sock", dfgetConfig.DaemonSock, "Download socket path of daemon. In linux, default value is /var/run/dfdaemon.sock, in macos(just for testing), default value is /tmp/dfdaemon.sock")

	flagSet.String("workhome", dfgetConfig.WorkHome, "Dfget working directory")

	flagSet.String("logdir", dfgetConfig.LogDir, "Dfget log directory")

	flagSet.String("datadir", dfgetConfig.DataDir, "Dfget data directory")

	flagSet.String("cachedir", dfgetConfig.CacheDir, "Dfget cache directory")

	flagSet.BoolP("recursive", "r", dfgetConfig.Recursive,
		"Recursively download all resources in target url, the target source client must support list action")

	flagSet.Uint("level", dfgetConfig.RecursiveLevel,
		"Recursively download only. Set the maximum number of subdirectories that dfget will recurse into. Set to 0 for no limit")

	flagSet.BoolP("list", "l", dfgetConfig.RecursiveList,
		"Recursively download only. List all urls instead of downloading them.")

	flagSet.String("accept-regex", dfgetConfig.RecursiveAcceptRegex,
		"Recursively download only. Specify a regular expression to accept the complete URL. In this case, you have to enclose the pattern into quotes to prevent your shell from expanding it")

	flagSet.String("reject-regex", dfgetConfig.RecursiveRejectRegex,
		`Recursively download only. Specify a regular expression to reject the complete URL. In this case, you have to enclose the pattern into quotes to prevent your shell from expanding it`)

	flagSet.Bool("original-offset", dfgetConfig.KeepOriginalOffset,
		`Range request only. Download ranged data into target file with original offset. Daemon will make a hardlink to target file. Client can download many ranged data into one file for same url. When enabled, back source in client will be disabled`)

	flagSet.String("range", dfgetConfig.Range,
		`Download range. Like: 0-9, stands download 10 bytes from 0 -9, [0:9] in real url`)

	// Bind cmd flags
	if err := viper.BindPFlags(flagSet); err != nil {
		panic(fmt.Errorf("bind dfget flags to viper: %w", err))
	}
}

func initDfgetDfpath(cfg *config.ClientOption) (dfpath.Dfpath, error) {
	options := []dfpath.Option{}
	if cfg.WorkHome != "" {
		options = append(options, dfpath.WithWorkHome(cfg.WorkHome))
	}

	if cfg.LogDir != "" {
		options = append(options, dfpath.WithLogDir(cfg.LogDir))
	}

	if cfg.DataDir != "" {
		options = append(options, dfpath.WithDataDir(cfg.DataDir))
	}

	if cfg.CacheDir != "" {
		options = append(options, dfpath.WithCacheDir(cfg.CacheDir))
	}

	if cfg.DaemonSock != "" {
		options = append(options, dfpath.WithDownloadUnixSocketPath(cfg.DaemonSock))
	}

	return dfpath.New(options...)
}

// runDfget does some init operations and starts to download.
func runDfget(cmd *cobra.Command, dfgetLockPath, daemonSockPath string) error {
	logger.Infof("version:\n%s", version.Version())

	ff := dependency.InitMonitor(dfgetConfig.PProfPort, dfgetConfig.Telemetry)
	defer ff()

	var (
		dfdaemonClient client.V1
		err            error
	)

	if err := loadSourceClients(cmd); err != nil {
		return err
	}

	logger.Info("start to check and spawn daemon")
	if dfdaemonClient, err = checkAndSpawnDaemon(dfgetLockPath, daemonSockPath); err != nil {
		logger.Errorf("check and spawn daemon error: %v", err)
	} else {
		logger.Info("check and spawn daemon success")
	}

	return dfget.Download(dfgetConfig, dfdaemonClient)
}

// loadSourceClients loads daemon config, extracts the source clients config, then initialize it.
func loadSourceClients(cmd *cobra.Command) error {
	configPath := path.Join(dfpath.DefaultConfigDir, cmd.Name()+".yaml")
	config := config.NewDaemonConfig()
	if err := config.Load(configPath); err != nil {
		// skip not exist error
		if !os.IsNotExist(err) {
			logger.Warnf("load daemon config err: %s, use default config", err)
		}
		if err = source.InitSourceClients(map[string]any{}); err != nil {
			logger.Errorf("init source clients with default config err: %s", err)
			return err
		}
		return nil
	}

	if err := source.InitSourceClients(config.Download.ResourceClients); err != nil {
		logger.Errorf("init source clients with daemon config err: %s", err)
		return err
	}
	return nil
}

// checkAndSpawnDaemon do checking at three checkpoints
func checkAndSpawnDaemon(dfgetLockPath, daemonSockPath string) (client.V1, error) {
	netAddr := &dfnet.NetAddr{Type: dfnet.UNIX, Addr: daemonSockPath}
	dfdaemonClient, err := client.GetInsecureV1(context.Background(), netAddr.String())
	if err != nil {
		return nil, err
	}

	// 1.Check without lock
	if dfdaemonClient.CheckHealth(context.Background()) == nil {
		return dfdaemonClient, nil
	}

	lock := flock.New(dfgetLockPath)
	if err := lock.Lock(); err != nil {
		logger.Errorf("flock lock failed %s", err)
		return nil, err
	}

	defer func() {
		if err := lock.Unlock(); err != nil {
			logger.Errorf("flock unlock failed %s", err)
		}
	}()

	// 2.Check with lock
	if dfdaemonClient.CheckHealth(context.Background()) == nil {
		return dfdaemonClient, nil
	}

	cmd := exec.Command(os.Args[0], "daemon", "--launcher", strconv.Itoa(os.Getpid()), "--config", viper.GetString("config"))
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	logger.Info("do start daemon")

	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	// 3. check health with at least 5s timeout
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	timeout := time.After(5 * time.Second)

	for {
		select {
		case <-timeout:
			return nil, errors.Join(errors.New("the daemon is unhealthy"), err)
		case <-tick.C:
			if err = dfdaemonClient.CheckHealth(context.Background()); err != nil {
				logger.Debugf("check health failed: %s", err)
				continue
			}
			return dfdaemonClient, nil
		}
	}
}
