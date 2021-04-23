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
	"fmt"
	"os"
	"reflect"
	"time"

	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"go.uber.org/zap/zapcore"

	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon"
	"d7y.io/dragonfly/v2/cmd/common"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/dflog/logcore"
	"d7y.io/dragonfly/v2/pkg/ratelimiter"
	"d7y.io/dragonfly/v2/pkg/util/fileutils/fsize"
	"github.com/go-echarts/statsview"
	"github.com/go-echarts/statsview/viewer"
	"github.com/mitchellh/mapstructure"
	"github.com/phayes/freeport"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

const (
	// cdnNodeEnvPrefix is the default environment prefix for Viper.
	// Both BindEnv and AutomaticEnv will use this prefix.
	cdnNodeEnvPrefix = "cdn"
)

var (
	cdnNodeViper = viper.GetViper()
)

// cdnNodeDescription is used to describe cdn command in details.
var cdnNodeDescription = `cdn server caches downloaded data from source to avoid downloading the same files from source repeatedly.`

var rootCmd = &cobra.Command{
	Use:               "cdn",
	Short:             "the data cache server of Dragonfly used for avoiding downloading the same files from source repeatedly",
	Long:              cdnNodeDescription,
	Args:              cobra.NoArgs,
	DisableAutoGenTag: true, // disable displaying auto generation tag in cli docs
	SilenceUsage:      true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runCdnSystem(cmd, args)
	},
}

func runCdnSystem(cmd *cobra.Command, args []string) error {
	// load config file into the given viper instance.
	if err := readConfigFile(cdnNodeViper, cmd); err != nil {
		return errors.Wrap(err, "failed to read config file")
	}

	// get config from viper.
	cfg, err := getConfigFromViper(cdnNodeViper)
	if err != nil {
		return errors.Wrap(err, "get config from viper")
	}

	err = initCdnSystem(cfg)

	d, err := daemon.New(cfg)
	if err != nil {
		logger.Errorf("failed to initialize daemon in cdn: %v", err)
		return err
	}
	return d.Run()
}

func initCdnSystem(cfg *config.Config) error {
	// set cdn node advertise ip
	if stringutils.IsBlank(cfg.AdvertiseIP) {
		cfg.AdvertiseIP = iputils.HostIp
	}

	// init logger
	if err := logcore.InitCdnSystem(cfg.Console); err != nil {
		return errors.Wrapf(err, "init log fail")
	}

	logger.Debugf("cdn config: %+v", cfg)

	logger.Infof("start to run cdn system, use ip: %s", cfg.AdvertiseIP)

	initProfiler(cfg.EnableProfiler)
	return nil
}

func initProfiler(enableProfiler bool) {
	if !enableProfiler {
		return
	}
	logcore.SetCoreLevel(zapcore.DebugLevel)
	logcore.SetGrpcLevel(zapcore.DebugLevel)
	go func() {
		// enable go pprof and statsview
		port, _ := freeport.GetFreePort()
		debugListen := fmt.Sprintf("%s:%d", iputils.HostIp, port)
		viewer.SetConfiguration(viewer.WithAddr(debugListen))
		logger.With("pprof", fmt.Sprintf("http://%s/debug/pprof", debugListen),
			"statsview", fmt.Sprintf("http://%s/debug/statsview", debugListen)).
			Debugf("enable debug at http://%s", debugListen)
		if err := statsview.New().Start(); err != nil {
			logger.Warnf("serve go pprof error: %s", err)
		}
	}()
}

func init() {
	setupFlags(rootCmd)

	// add sub commands
	rootCmd.AddCommand(common.VersionCmd)
}

// setupFlags setups flags for command line.
func setupFlags(cmd *cobra.Command) {

	flagSet := cmd.Flags()

	defaultBaseProperties := config.NewDefaultBaseProperties()

	flagSet.String("config", config.DefaultCdnConfigFilePath,
		"the path of cdn configuration file")

	flagSet.Int("port", defaultBaseProperties.ListenPort,
		"listenPort is the port that cdn server listens on")

	flagSet.Int("download-port", defaultBaseProperties.DownloadPort,
		"downloadPort is the port for download files from cdn")

	flagSet.Var(&defaultBaseProperties.SystemReservedBandwidth, "system-bandwidth",
		"network rate reserved for system")

	flagSet.Var(&defaultBaseProperties.MaxBandwidth, "max-bandwidth",
		"network rate that cdnNode can use")

	flagSet.Bool("profiler", defaultBaseProperties.EnableProfiler,
		"profiler sets if cdn HTTP server setups profiler")

	flagSet.String("advertise-ip", defaultBaseProperties.AdvertiseIP,
		"the cdn node ip is the ip we advertise to other peers in the p2p-network")

	flagSet.Duration("fail-access-interval", defaultBaseProperties.FailAccessInterval,
		"fail access interval is the interval time after failed to access the URL")

	flagSet.Duration("gc-initial-delay", defaultBaseProperties.GCInitialDelay,
		"gc initial delay is the delay time from the start to the first GC execution")

	flagSet.Duration("gc-meta-interval", defaultBaseProperties.GCMetaInterval,
		"gc meta interval is the interval time to execute the GC meta")

	flagSet.Duration("gc-storage-interval", defaultBaseProperties.GCStorageInterval,
		"gc storage interval is the interval time to execute GC storage.")

	flagSet.Duration("task-expire-time", defaultBaseProperties.TaskExpireTime,
		"task expire time is the time that a task is treated expired if the task is not accessed within the time")

	flagSet.String("storagePattern", defaultBaseProperties.StoragePattern,
		"storagePattern is the pattern of storage: hybrid/disk/memory")

	flagSet.Bool("console", defaultBaseProperties.Console, "console sets if shows log on console")

	exitOnError(bindRootFlags(cdnNodeViper), "bind root command flags")
}

// bindRootFlags binds flags on rootCmd and env variable to the given viper instance.
func bindRootFlags(v *viper.Viper) error {
	flags := []struct {
		key  string
		flag string
	}{
		{
			key:  "config",
			flag: "config",
		}, {
			key:  "base.listenPort",
			flag: "port",
		}, {
			key:  "base.downloadPort",
			flag: "download-port",
		}, {
			key:  "base.systemReservedBandwidth",
			flag: "system-bandwidth",
		}, {
			key:  "base.maxBandwidth",
			flag: "max-bandwidth",
		}, {
			key:  "base.enableProfiler",
			flag: "profiler",
		}, {
			key:  "base.advertiseIP",
			flag: "advertise-ip",
		}, {
			key:  "base.failAccessInterval",
			flag: "fail-access-interval",
		}, {
			key:  "base.gcInitialDelay",
			flag: "gc-initial-delay",
		}, {
			key:  "base.gcMetaInterval",
			flag: "gc-meta-interval",
		}, {
			key:  "base.gcStorageInterval",
			flag: "gc-storage-interval",
		}, {
			key:  "base.taskExpireTime",
			flag: "task-expire-time",
		}, {
			key:  "base.storagePattern",
			flag: "storagePattern",
		}, {
			key:  "base.Console",
			flag: "console",
		},
	}

	for _, f := range flags {
		if err := v.BindPFlag(f.key, rootCmd.Flag(f.flag)); err != nil {
			return err
		}
	}
	// bind env
	v.SetEnvPrefix(cdnNodeEnvPrefix)
	v.AutomaticEnv()

	return nil
}

// readConfigFile reads config file into the given viper instance. If we're
// reading the default configuration file and the file does not exist, nil will
// be returned.
func readConfigFile(v *viper.Viper, cmd *cobra.Command) error {
	v.SetConfigFile(v.GetString("config"))
	v.SetConfigType("yaml")

	if err := v.ReadInConfig(); err != nil {
		// when the default config file is not found, ignore the error
		if os.IsNotExist(err) && !cmd.Flag("config").Changed {
			return nil
		}
		return err
	}

	return nil
}

// getDefaultConfig returns the default configuration of cdn
func getDefaultConfig() (interface{}, error) {
	return getConfigFromViper(viper.GetViper())
}

// getConfigFromViper returns cdn config from the given viper instance
func getConfigFromViper(v *viper.Viper) (*config.Config, error) {
	cfg := config.NewDefaultConfig()

	if err := v.Unmarshal(cfg, func(dc *mapstructure.DecoderConfig) {
		dc.TagName = "yaml"
		dc.DecodeHook = decodeWithYAML(
			reflect.TypeOf(time.Second),
			reflect.TypeOf(ratelimiter.B),
			reflect.TypeOf(fsize.B),
		)
	}); err != nil {
		return nil, errors.Wrap(err, "unmarshal yaml")
	}

	return cfg, nil
}

// decodeWithYAML returns a mapstructure.DecodeHookFunc to decode the given
// types by unmarshalling from yaml text.
func decodeWithYAML(types ...reflect.Type) mapstructure.DecodeHookFunc {
	return func(f, t reflect.Type, data interface{}) (interface{}, error) {
		for _, typ := range types {
			if t == typ {
				b, _ := yaml.Marshal(data)
				v := reflect.New(t)
				return v.Interface(), yaml.Unmarshal(b, v.Interface())
			}
		}
		return data, nil
	}
}

func setAdvertiseIP(cfg *config.Config) error {
	// use the first non-loop address if the AdvertiseIP is empty

	return nil
}

// Execute will process cdn.
func Execute() {
	exitOnError(rootCmd.Execute(), "cdn cmd execute")
}

func exitOnError(err error, msg string) {
	if err != nil {
		logger.Fatalf("%s: %v", msg, err)
	}
}
