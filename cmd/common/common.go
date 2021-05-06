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
	"context"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"syscall"

	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/dflog/logcore"
	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/version"
	"github.com/go-echarts/statsview"
	"github.com/go-echarts/statsview/viewer"
	"github.com/mitchellh/mapstructure"
	"github.com/phayes/freeport"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/trace/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
)

var (
	configFileRead bool
	existEnvPrefix string
)

type FinalHandler func()
type SignalHandler func()

// InitCobra initializes flags binding and common sub cmds.
// config is a pointer to configuration struct.
func InitCobra(cmd *cobra.Command, useFile bool, envPrefix string, config interface{}) {
	var cfgFile string
	cobra.OnInitialize(func() { initConfig(useFile, &cfgFile, envPrefix, config) })

	// Add flags
	rootFlags := cmd.Root().PersistentFlags()
	if rootFlags.Lookup("console") == nil {
		rootFlags.Bool("console", false, "whether output log info on the terminal")
		rootFlags.Bool("verbose", false, "whether use debug level logger and enable pprof")
		rootFlags.Int("pprofPort", 0, "listen port for pprof, only valid when the verbose option is true, default is random port")
		rootFlags.String("jaeger", "", "jaeger back-end address, like: http://localhost:14250")
		rootFlags.StringVarP(&cfgFile, "config", "f", "", "the path of configuration file")
	}

	// Bind flags
	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		panic(errors.Wrap(err, "bind cmd flags to viper"))
	} else if err := viper.BindPFlags(rootFlags); err != nil {
		panic(errors.Wrap(err, "bind root flags to viper"))
	}

	// Add common cmds
	if !cmd.HasParent() {
		cmd.AddCommand(VersionCmd)
		cmd.AddCommand(newDocCommand(cmd.Name()))
	}
}

// InitMonitor initialize monitor and return final func
func InitMonitor(verbose bool, pprofPort int, jaeger string) FinalHandler {
	var ffs []func()

	if verbose {
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

	if jaeger != "" {
		if ff, err := initTracer(verbose, jaeger); err != nil {
			logger.Warnf("init tracer error:%v", err)
		} else {
			ffs = append(ffs, ff)
		}
	}

	return func() {
		logger.Infof("do %d final handlers", len(ffs))
		for _, ff := range ffs {
			ff()
		}
	}
}

func SetupQuitSignalHandler(handler SignalHandler) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		var done bool
		for {
			select {
			case sig := <-signals:
				logger.Warnf("receive signal:%v", sig)
				if !done {
					done = true
					handler()
				}
			}
		}
	}()
}

// initConfig reads in config file and ENV variables if set.
func initConfig(useFile bool, cfgFile *string, envPrefix string, config interface{}) {
	// Env prefix must be consistent.
	if envPrefix != "" {
		if existEnvPrefix != "" {
			if existEnvPrefix != envPrefix {
				panic("viper can not support multi env prefix")
			}
		} else {
			viper.SetEnvPrefix(envPrefix)
			viper.AutomaticEnv() // read in environment variables that match
			existEnvPrefix = envPrefix
		}
	}

	// Use config file and read once.
	if useFile && !configFileRead {
		if *cfgFile != "" {
			// Use config file from the flag.
			viper.SetConfigFile(*cfgFile)
		} else {
			viper.AddConfigPath(defaultConfigDir)
			viper.SetConfigName(envPrefix)
			viper.SetConfigType("yaml")
		}

		// If a config file is found, read it in.
		if err := viper.ReadInConfig(); err != nil {
			ignoreErr := false
			if _, ok := err.(viper.ConfigFileNotFoundError); ok {
				if *cfgFile == "" {
					ignoreErr = true
				}
			}
			if !ignoreErr {
				panic(errors.Wrap(err, "viper read config"))
			}
		}

		configFileRead = true
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

// initTracer creates a new trace provider instance and registers it as global trace provider.
func initTracer(verbose bool, addr string) (func(), error) {
	exp, err := jaeger.NewRawExporter(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(addr)))
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		// Always be sure to batch in production.
		sdktrace.WithBatcher(exp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		// Record information about this application in an Resource.
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.ServiceNameKey.String("dragonfly"),
			semconv.ServiceVersionKey.String(version.GitVersion))),
	)

	// Register our TracerProvider as the global so any imported
	// instrumentation in the future will default to using it.
	otel.SetTracerProvider(tp)

	return func() { _ = tp.Shutdown(context.Background()) }, nil
}
