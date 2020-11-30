
package app

import (
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon"
	"github.com/dragonflyoss/Dragonfly2/pkg/stringutils"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/dragonflyoss/Dragonfly2/pkg/cmd"
	"github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly2/pkg/fileutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/netutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/rate"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
)

const (
	// cdnNodeEnvPrefix is the default environment prefix for Viper.
	// Both BindEnv and AutomaticEnv will use this prefix.
	cdnNodeEnvPrefix = "cdnNode"
)

var (
	cdnNodeViper = viper.GetViper()
)

// cdnNodeDescription is used to describe cdn command in details.
var cdnNodeDescription = `cdnNode is a CDN server that caches downloaded data from source to avoid downloading the same files from source repeatedly.`

var rootCmd = &cobra.Command{
	Use:               "cdn",
	Short:             "the data cache server of Dragonfly used for avoiding downloading the same files from source repeatedly",
	Long:              cdnNodeDescription,
	Args:              cobra.NoArgs,
	DisableAutoGenTag: true, // disable displaying auto generation tag in cli docs
	SilenceUsage:      true,
	RunE: func(cmd *cobra.Command, args []string) error {
		// load config file into the given viper instance.
		if err := readConfigFile(cdnNodeViper, cmd); err != nil {
			return errors.Wrap(err, "read config file")
		}

		// get config from viper.
		cfg, err := getConfigFromViper(cdnNodeViper)
		if err != nil {
			return errors.Wrap(err, "get config from viper")
		}

		// create home dir
		if err := fileutils.CreateDirectory(cdnNodeViper.GetString("base.homeDir")); err != nil {
			return fmt.Errorf("failed to create home dir %s: %v", cdnNodeViper.GetString("base.homeDir"), err)
		}

		// initialize cdnnode logger.
		if err := initLog(logrus.StandardLogger(), "app.log", cfg.LogConfig); err != nil {
			return err
		}

		// initialize dfget logger.
		dfgetLogger := logrus.New()
		if err := initLog(dfgetLogger, "dfget.log", cfg.LogConfig); err != nil {
			return err
		}

		// set cdnNode advertise ip
		if stringutils.IsEmptyStr(cfg.AdvertiseIP) {
			if err := setAdvertiseIP(cfg); err != nil {
				return err
			}
		}
		logrus.Infof("success to init local ip of cdn, use ip: %s", cfg.AdvertiseIP)

		// set up the CIDPrefix
		cfg.SetCIDPrefix(cfg.AdvertiseIP)

		logrus.Debugf("get cdnNode config: %+v", cfg)
		logrus.Info("start to run cdnNode")

		d, err := daemon.New(cfg, dfgetLogger)
		if err != nil {
			logrus.Errorf("failed to initialize daemon in cdn: %v", err)
			return err
		}
		return d.Run()
	},
}

func init() {
	setupFlags(rootCmd)

	// add sub commands
	rootCmd.AddCommand(cmd.NewGenDocCommand("cdn"))
	rootCmd.AddCommand(cmd.NewVersionCommand("cdn"))
	rootCmd.AddCommand(cmd.NewConfigCommand("cdn", getDefaultConfig))
}

// setupFlags setups flags for command line.
func setupFlags(cmd *cobra.Command) {
	// Cobra supports Persistent Flags, which, if defined here,
	// will be global for your application.
	// flagSet := cmd.PersistentFlags()

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	flagSet := cmd.Flags()

	defaultBaseProperties := config.NewBaseProperties()

	flagSet.String("config", config.DefaultCdnConfigFilePath,
		"the path of cdn's configuration file")

	flagSet.String("cdn-pattern", config.CDNPatternLocal,
		"cdn pattern, must be in [\"local\", \"source\"]. Default: local")

	flagSet.Int("rpc-port", defaultBaseProperties.ListenRpcPort,
		"listenPort is the port that cdn rpc server listens on")

	flagSet.Int("http-port", defaultBaseProperties.ListenHttpPort,
		"listenPort is the port that cdn http server listens on")

	flagSet.Int("download-port", defaultBaseProperties.DownloadPort,
		"downloadPort is the port for download files from cdnNode")

	flagSet.String("home-dir", defaultBaseProperties.HomeDir,
		"homeDir is the working directory of cdnNode")

	flagSet.Var(&defaultBaseProperties.SystemReservedBandwidth, "system-bandwidth",
		"network rate reserved for system")

	flagSet.Var(&defaultBaseProperties.MaxBandwidth, "max-bandwidth",
		"network rate that cdnNode can use")

	flagSet.Int("pool-size", defaultBaseProperties.SchedulerCorePoolSize,
		"pool size is the core pool size of ScheduledExecutorService")

	flagSet.Bool("profiler", defaultBaseProperties.EnableProfiler,
		"profiler sets whether cdnNode HTTP server setups profiler")

	flagSet.BoolP("debug", "D", defaultBaseProperties.Debug,
		"switch daemon log level to DEBUG mode")

	flagSet.String("advertise-ip", "",
		"the cdnNode ip is the ip we advertise to other peers in the p2p-network")

	flagSet.Duration("fail-access-interval", defaultBaseProperties.FailAccessInterval,
		"fail access interval is the interval time after failed to access the URL")

	flagSet.Duration("gc-initial-delay", defaultBaseProperties.GCInitialDelay,
		"gc initial delay is the delay time from the start to the first GC execution")

	flagSet.Duration("gc-meta-interval", defaultBaseProperties.GCMetaInterval,
		"gc meta interval is the interval time to execute the GC meta")

	flagSet.Duration("task-expire-time", defaultBaseProperties.TaskExpireTime,
		"task expire time is the time that a task is treated expired if the task is not accessed within the time")

	flagSet.Duration("peer-gc-delay", defaultBaseProperties.PeerGCDelay,
		"peer gc delay is the delay time to execute the GC after the peer has reported the offline")

	exitOnError(bindRootFlags(cdnNodeViper), "bind root command flags")
}

// bindRootFlags binds flags on rootCmd to the given viper instance.
func bindRootFlags(v *viper.Viper) error {
	flags := []struct {
		key  string
		flag string
	}{
		{
			key:  "config",
			flag: "config",
		},
		{
			key:  "base.CDNPattern",
			flag: "cdn-pattern",
		},
		{
			key:  "base.listenPort",
			flag: "port",
		},
		{
			key:  "base.downloadPort",
			flag: "download-port",
		},
		{
			key:  "base.homeDir",
			flag: "home-dir",
		},
		{
			key:  "base.systemReservedBandwidth",
			flag: "system-bandwidth",
		},
		{
			key:  "base.maxBandwidth",
			flag: "max-bandwidth",
		},
		{
			key:  "base.schedulerCorePoolSize",
			flag: "pool-size",
		},
		{
			key:  "base.enableProfiler",
			flag: "profiler",
		},
		{
			key:  "base.debug",
			flag: "debug",
		},
		{
			key:  "base.peerUpLimit",
			flag: "up-limit",
		},
		{
			key:  "base.peerDownLimit",
			flag: "down-limit",
		},
		{
			key:  "base.advertiseIP",
			flag: "advertise-ip",
		},
		{
			key:  "base.failAccessInterval",
			flag: "fail-access-interval",
		},
		{
			key:  "base.gcInitialDelay",
			flag: "gc-initial-delay",
		},
		{
			key:  "base.gcMetaInterval",
			flag: "gc-meta-interval",
		},
		{
			key:  "base.taskExpireTime",
			flag: "task-expire-time",
		},
		{
			key:  "base.peerGCDelay",
			flag: "peer-gc-delay",
		},
	}

	for _, f := range flags {
		if err := v.BindPFlag(f.key, rootCmd.Flag(f.flag)); err != nil {
			return err
		}
	}

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

// getDefaultConfig returns the default configuration of cdnNode
func getDefaultConfig() (interface{}, error) {
	return getConfigFromViper(viper.GetViper())
}

// getConfigFromViper returns cdnNode config from the given viper instance
func getConfigFromViper(v *viper.Viper) (*config.Config, error) {
	cfg := config.NewConfig()

	if err := v.Unmarshal(cfg, func(dc *mapstructure.DecoderConfig) {
		dc.TagName = "yaml"
		dc.DecodeHook = decodeWithYAML(
			reflect.TypeOf(time.Second),
			reflect.TypeOf(rate.B),
			reflect.TypeOf(fileutils.B),
		)
	}); err != nil {
		return nil, errors.Wrap(err, "unmarshal yaml")
	}

	// set dynamic configuration
	cfg.DownloadPath = filepath.Join(cfg.HomeDir, "repo", "download")

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

// initLog initializes log Level and log format.
func initLog(logger *logrus.Logger, logPath string, logConfig dflog.LogConfig) error {
	// get log file path
	logFilePath := filepath.Join(cdnNodeViper.GetString("base.homeDir"), "logs", logPath)

	opts := []dflog.Option{
		dflog.WithLogFile(logFilePath, logConfig.MaxSize, logConfig.MaxBackups),
		dflog.WithSign(fmt.Sprintf("%d", os.Getpid())),
		dflog.WithDebug(cdnNodeViper.GetBool("base.debug")),
	}

	logrus.Debugf("use log file %s", logFilePath)
	if err := dflog.Init(logger, opts...); err != nil {
		return errors.Wrap(err, "init log")
	}

	return nil
}

func setAdvertiseIP(cfg *config.Config) error {
	// use the first non-loop address if the AdvertiseIP is empty
	ipList, err := netutils.GetAllIPs()
	if err != nil {
		return errors.Wrapf(errortypes.ErrSystemError, "failed to get ip list: %v", err)
	}
	if len(ipList) == 0 {
		logrus.Errorf("get empty system's unicast interface addresses")
		return errors.Wrapf(errortypes.ErrSystemError, "Unable to autodetect advertiser ip, please set it via --advertise-ip")
	}

	cfg.AdvertiseIP = ipList[0]

	return nil
}

// Execute will process cdnNode.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		logrus.Error(err)
		os.Exit(1)
	}
}

func exitOnError(err error, msg string) {
	if err != nil {
		logrus.Fatalf("%s: %v", msg, err)
	}
}
