package cmd

import (
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/server"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"github.com/mitchellh/mapstructure"
	"os"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

const (
	// ManagerEnvPrefix is the default environment prefix for Viper.
	// Both BindEnv and AutomaticEnv will use this prefix.
	ManagerEnvPrefix = "manager"
)

var (
	managerViper = viper.GetViper()
)

// supernodeDescription is used to describe supernode command in details.
var supernodeDescription = `manager is a long-running process which has these primary responsibilities:
1. auto dispatch configs to schedulers and cdns.`

var rootCmd = &cobra.Command{
	Use:               "manager",
	Short:             "the central control server of Dragonfly used for manager",
	Long:              supernodeDescription,
	Args:              cobra.NoArgs,
	DisableAutoGenTag: true, // disable displaying auto generation tag in cli docs
	SilenceUsage:      true,
	RunE: func(cmd *cobra.Command, args []string) error {
		err := logger.InitManager()
		if err != nil {
			return errors.Wrap(err, "init manager logger")
		}

		// load config file.
		if err = readConfigFile(managerViper, cmd); err != nil {
			return errors.Wrap(err, "read config file")
		}

		// get config from viper.
		cfg, err := getConfigFromViper(managerViper)
		if err != nil {
			return errors.Wrap(err, "get config from viper")
		}

		logger.Debugf("get manager config: %+v", cfg)
		logger.Infof("start to run manager")

		if server, err := server.NewServer(cfg); err != nil {
			return errors.Wrap(err, "failed to initialize daemon in manager")
		} else {
			return server.Start()
		}
	},
}

func init() {
	setupFlags(rootCmd)
}

// setupFlags setups flags for command line.
func setupFlags(cmd *cobra.Command) {
	flagSet := cmd.Flags()

	defaultBaseProperties := config.GetConfig()

	flagSet.Bool("console", false, "print log in console or not")

	flagSet.String("config", config.DefaultConfigFilePath,
		"the path of manager's configuration file")

	flagSet.Int("port", defaultBaseProperties.Server.Port,
		"port is the port that manager server listens on")

	exitOnError(bindRootFlags(managerViper), "bind root command flags")
}

// bindRootFlags binds flags on rootCmd to the given viper instance.
func bindRootFlags(v *viper.Viper) error {
	flags := []struct {
		key  string
		flag string
	}{
		{
			key: "console",
			flag: "console",
		},
		{
			key:  "config",
			flag: "config",
		},
		{
			key:  "server.port",
			flag: "port",
		},
	}

	for _, f := range flags {
		if err := v.BindPFlag(f.key, rootCmd.Flag(f.flag)); err != nil {
			return err
		}
	}

	v.SetEnvPrefix(ManagerEnvPrefix)
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

// getDefaultConfig returns the default configuration of scheduler
func getDefaultConfig() (interface{}, error) {
	return getConfigFromViper(viper.GetViper())
}

// getConfigFromViper returns supernode config from the given viper instance
func getConfigFromViper(v *viper.Viper) (*config.Config, error) {
	cfg := config.GetConfig()

	if err := v.Unmarshal(cfg, func(dc *mapstructure.DecoderConfig) {
		dc.TagName = "yaml"
		dc.DecodeHook = decodeWithYAML(
			reflect.TypeOf(time.Second),
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

// Execute will process supernode.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		logger.Errorf(err.Error())
		os.Exit(1)
	}
}

func exitOnError(err error, msg string) {
	if err != nil {
		logger.Errorf("%s: %v", msg, err)
	}
}
