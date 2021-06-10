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

package dependency

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"time"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/internal/dfpath"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/dflog/logcore"
	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
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

// InitCobra initializes flags binding and common sub cmds.
// config is a pointer to configuration struct.
func InitCobra(cmd *cobra.Command, useConfigFile bool, config interface{}) {
	rootName := cmd.Root().Name()
	cobra.OnInitialize(func() { initConfig(useConfigFile, rootName, config) })

	if !cmd.HasParent() {
		// Add common flags
		flags := cmd.PersistentFlags()
		flags.Bool("console", false, "whether logger output records to the stdout")
		flags.Bool("verbose", false, "whether logger use debug level")
		flags.Int("pprof-port", -1, "listen port for pprof, 0 represents random port")
		flags.String("jaeger", "", "jaeger endpoint url, like: http://localhost:14250/api/traces")
		flags.String("config", "", fmt.Sprintf("the path of configuration file with yaml extension name, default is %s, it can also be set by env var:%s", filepath.Join(dfpath.DefaultConfigDir, rootName+".yaml"), strings.ToUpper(rootName+"_config")))

		// Bind common flags
		if err := viper.BindPFlags(flags); err != nil {
			panic(errors.Wrap(err, "bind common flags to viper"))
		}

		// Config for binding env
		viper.SetEnvPrefix(rootName)
		viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
		_ = viper.BindEnv("config")

		// Add common cmds only on root cmd
		cmd.AddCommand(VersionCmd)
		cmd.AddCommand(newDocCommand(cmd.Name()))
	}
}

// InitMonitor initialize monitor and return final handler
func InitMonitor(verbose bool, pprofPort int, jaeger string) func() {
	var fc = make(chan func(), 5)

	if verbose {
		logcore.SetCoreLevel(zapcore.DebugLevel)
		logcore.SetGrpcLevel(zapcore.DebugLevel)
	}

	if pprofPort >= 0 {
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

			vm := statsview.New()
			if err := vm.Start(); err != nil {
				logger.Warnf("serve pprof error:%v", err)
			}
			fc <- func() { vm.Stop() }
		}()
	}

	if jaeger != "" {
		ff, err := initJaegerTracer(jaeger)
		if err != nil {
			logger.Warnf("init jaeger tracer error:%v", err)
		}

		fc <- ff
	}

	return func() {
		logger.Infof("do %d monitor finalizer", len(fc))
		for {
			select {
			case f := <-fc:
				f()
			default:
				return
			}
		}
	}
}

func SetupQuitSignalHandler(handler func()) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	go func() {
		var done bool
		for {
			select {
			case sig := <-signals:
				logger.Warnf("receive signal:%v", sig)
				if !done {
					done = true
					handler()
					logger.Warnf("handle signal:%v finish", sig)
				}
			}
		}
	}()
}

// initConfig reads in config file and ENV variables if set.
func initConfig(useConfigFile bool, name string, config interface{}) {
	// Use config file and read once.
	if useConfigFile {
		cfgFile := viper.GetString("config")
		if cfgFile != "" {
			// Use config file from the flag.
			viper.SetConfigFile(cfgFile)
		} else {
			viper.AddConfigPath(dfpath.DefaultConfigDir)
			viper.SetConfigName(name)
			viper.SetConfigType("yaml")
		}

		// If a config file is found, read it in.
		if err := viper.ReadInConfig(); err != nil {
			ignoreErr := false
			if _, ok := err.(viper.ConfigFileNotFoundError); ok {
				if cfgFile == "" {
					ignoreErr = true
				}
			}
			if !ignoreErr {
				panic(errors.Wrap(err, "viper read config"))
			}
		}
	}
	if err := viper.Unmarshal(config, initDecoderConfig); err != nil {
		panic(errors.Wrap(err, "unmarshal config to struct"))
	}
}

func initDecoderConfig(dc *mapstructure.DecoderConfig) {
	dc.DecodeHook = mapstructure.ComposeDecodeHookFunc(func(from, to reflect.Type, v interface{}) (interface{}, error) {
		switch to {
		case reflect.TypeOf(unit.B),
			reflect.TypeOf(dfnet.NetAddr{}),
			reflect.TypeOf(clientutil.RateLimit{}),
			reflect.TypeOf(clientutil.Duration{}),
			reflect.TypeOf(config.ProxyOption{}),
			reflect.TypeOf(config.TCPListenPortRange{}),
			reflect.TypeOf(config.FileString("")),
			reflect.TypeOf(config.URL{}),
			reflect.TypeOf(config.CertPool{}),
			reflect.TypeOf(config.Regexp{}):

			b, _ := yaml.Marshal(v)
			p := reflect.New(to)
			if err := yaml.Unmarshal(b, p.Interface()); err != nil {
				return nil, err
			}

			return p.Interface(), nil
		default:
			return v, nil
		}
	}, mapstructure.StringToSliceHookFunc("&"), dc.DecodeHook)
}

// initTracer creates a new trace provider instance and registers it as global trace provider.
func initJaegerTracer(url string) (func(), error) {
	exp, err := jaeger.NewRawExporter(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(url)))
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
			semconv.ServiceInstanceIDKey.String(fmt.Sprintf("%s|%s", iputils.HostName, iputils.HostIP)),
			semconv.ServiceVersionKey.String(version.GitVersion))),
	)

	// Register our TracerProvider as the global so any imported
	// instrumentation in the future will default to using it.
	otel.SetTracerProvider(tp)

	return func() {
		// Do not make the application hang when it is shutdown.
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = tp.Shutdown(ctx)
	}, nil
}
