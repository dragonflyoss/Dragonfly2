package cmd

import (
	"encoding/json"
	"os"
	"strconv"

	"go.uber.org/zap/zapcore"

	"fmt"

	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/dflog/logcore"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/server"
	"d7y.io/dragonfly/v2/version"
	"github.com/go-echarts/statsview"
	"github.com/go-echarts/statsview/viewer"
	"github.com/phayes/freeport"
	"github.com/sirupsen/logrus"

	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	// SupernodeEnvPrefix is the default environment prefix for Viper.
	// Both BindEnv and AutomaticEnv will use this prefix.
	SchedulerEnvPrefix = "scheduler"
)

var cfg *config.Config
var cfgFile string

// schedulerDescription is used to describe supernode command in details.
var schedulerDescription = `scheduler is a long-running process with two primary responsibilities:
It's the tracker and scheduler in the P2P network that choose appropriate downloading net-path for each peer.`

var schedulerCmd = &cobra.Command{
	Use:               "scheduler",
	Short:             "the central control server of Dragonfly used for scheduling",
	Long:              schedulerDescription,
	Args:              cobra.NoArgs,
	DisableAutoGenTag: true, // disable displaying auto generation tag in cli docs
	SilenceUsage:      true,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Init logger
		if err := logcore.InitScheduler(cfg.Console); err != nil {
			return errors.Wrap(err, "init scheduler logger")
		}

		return runScheduler()
	},
}

func init() {
	// Initialize default daemon config
	cfg = &config.SchedulerConfig

	// Initialize cobra
	cobra.OnInitialize(initConfig)

	// Add flags
	flagSet := schedulerCmd.Flags()
	flagSet.BoolVar(&cfg.Console, "console", cfg.Console, "console")
	flagSet.BoolVar(&cfg.Verbose, "verbose", cfg.Verbose, "print verbose log and enable golang debug info")
	flagSet.IntVar(&cfg.Server.Port, "port", cfg.Server.Port, "port is the port that scheduler server listens on")
	flagSet.IntVar(&cfg.Worker.WorkerNum, "worker-num", cfg.Worker.WorkerNum, "worker-num is used for scheduler and do not change it")
	flagSet.IntVar(&cfg.Worker.WorkerJobPoolSize, "worker-job-pool-size", cfg.Worker.WorkerJobPoolSize, "worker-job-pool-size is used for scheduler and do not change it")
	flagSet.IntVar(&cfg.Worker.SenderNum, "sender-num", cfg.Worker.SenderNum, "sender-num is used for scheduler and do not change it")
	flagSet.IntVar(&cfg.Worker.WorkerJobPoolSize, "sender-job-pool-size", cfg.Worker.WorkerJobPoolSize, "sender-job-pool-size is used for scheduler and do not change it")
	flagSet.StringVar(&cfgFile, "config", "", "the path of scheduler's configuration file")
	flagSet.Var(config.NewCDNValue(&cfg.CDN), "cdn-servers", "cdn server list with format of [CdnName1]:[ip1]:[rpcPort1]:[downloadPort1],[CdnName2]:[ip2]:[rpcPort2]:[downloadPort2]")

	schedulerCmd.AddCommand(version.VersionCmd)
}

func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		viper.AddConfigPath(filepath.Dir(config.DefaultConfigFilePath))
		viper.SetConfigFile(filepath.Base(config.DefaultConfigFilePath))
	}

	fmt.Printf("file: %s", cfgFile)

	viper.SetEnvPrefix(SchedulerEnvPrefix)
	viper.AutomaticEnv() // read in envionment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		logrus.Debugf("Using config file: %s", viper.ConfigFileUsed())
	}

	// Unmarshal config
	if err := viper.Unmarshal(&cfg); err != nil {
		logrus.Fatalf(errors.Wrap(err, "cannot unmarshal config").Error())
	}
}

func runScheduler() error {
	// Scheduler config values
	s, _ := json.MarshalIndent(cfg, "", "  ")
	logger.Debugf("dfget option(debug only, can not use as config):\n%s", string(s))

	// Initialize verbose mode
	initVerboseMode(cfg.Verbose)

	svr := server.NewServer()
	return svr.Start()
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

// Execute will process supernode.
func Execute() {
	if err := schedulerCmd.Execute(); err != nil {
		logger.Errorf(err.Error())
		os.Exit(1)
	}
}
