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

package common

import (
	"fmt"
	"reflect"

	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/dflog/logcore"
	"d7y.io/dragonfly/v2/pkg/unit"
	"github.com/go-echarts/statsview"
	"github.com/go-echarts/statsview/viewer"
	"github.com/mitchellh/mapstructure"
	"github.com/phayes/freeport"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
)

// InitCobra initializes flags binding and common sub cmds.
// cfgFile is a pointer to configuration path, config is a pointer to configuration struct.
func InitCobra(cmd *cobra.Command, cfgFile *string, envPrefix string, config interface{}) {
	cobra.OnInitialize(func() { initConfig(cfgFile, envPrefix, config) })

	// Add flags
	flagSet := cmd.Flags()
	flagSet.Bool("console", false, "whether print log on the terminal")
	flagSet.Bool("verbose", false, "whether use debug level logger and enable pprof")
	flagSet.Int("pprofPort", 0, "listen port for pprof, only valid when the verbose option is true, default is random port")
	flagSet.StringVarP(cfgFile, "config", "f", "", "the path of configuration file")

	if err := viper.BindPFlags(flagSet); err != nil {
		panic(errors.Wrap(err, "bind flags to viper"))
	}

	// Add common cmds
	cmd.AddCommand(VersionCmd)
	cmd.AddCommand(newDocCommand(cmd.Name()))
}

func InitVerboseMode(verbose bool, pprofPort int) {
	if !verbose {
		return
	}

	logcore.SetCoreLevel(zapcore.DebugLevel)
	logcore.SetGrpcLevel(zapcore.DebugLevel)

	// Enable go pprof and statsview
	go func() {
		if pprofPort == 0 {
			pprofPort, _ = freeport.GetFreePort()
		}

		debugAddr := fmt.Sprintf("localhost:%d", pprofPort)
		viewer.SetConfiguration(viewer.WithAddr(debugAddr))

		logger.With("pprof", fmt.Sprintf("http://%s/debug/pprof", debugAddr),
			"statsview", fmt.Sprintf("http://%s/debug/statsview", debugAddr)).
			Infof("enable pprof at %s", debugAddr)

		if err := statsview.New().Start(); err != nil {
			logger.Warnf("serve pprof error:%v", err)
		}
	}()
}

// initConfig reads in config file and ENV variables if set.
func initConfig(cfgFile *string, envPrefix string, config interface{}) {
	if *cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(*cfgFile)
	} else {
		viper.AddConfigPath(defaultConfigDir)
		viper.SetConfigName(envPrefix)
		viper.SetConfigType("yaml")
	}

	viper.SetEnvPrefix(envPrefix)
	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("using config file:", viper.ConfigFileUsed())
	}

	if err := viper.Unmarshal(config, initDecoderConfig); err != nil {
		panic(errors.Wrap(err, "unmarshal config to struct"))
	}
}

func initDecoderConfig(dc *mapstructure.DecoderConfig) {
	dc.TagName = "yaml"
	dc.DecodeHook = mapstructure.ComposeDecodeHookFunc(dc.DecodeHook, func(from, to reflect.Type, v interface{}) (interface{}, error) {
		switch to {
		case reflect.TypeOf(unit.B):
			b, _ := yaml.Marshal(v)
			p := reflect.New(to)
			if err := yaml.Unmarshal(b, p.Interface()); err != nil {
				return nil, err
			} else {
				return p.Interface(), nil
			}
		default:
			return v, nil
		}
	})
}
