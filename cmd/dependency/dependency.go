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
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"time"

	"github.com/go-echarts/statsview"
	"github.com/go-echarts/statsview/viewer"
	"github.com/mitchellh/mapstructure"
	"github.com/phayes/freeport"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"gopkg.in/yaml.v3"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/util"
	"d7y.io/dragonfly/v2/cmd/dependency/base"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/dfpath"
	"d7y.io/dragonfly/v2/pkg/net/fqdn"
	"d7y.io/dragonfly/v2/pkg/net/ip"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/version"
)

// InitCommandAndConfig initializes flags binding and common sub cmds.
// config is a pointer to configuration struct.
func InitCommandAndConfig(cmd *cobra.Command, useConfigFile bool, config any) {
	rootName := cmd.Root().Name()
	cobra.OnInitialize(func() {
		initConfig(useConfigFile, rootName, config)
	})

	if !cmd.HasParent() {
		// Add common flags
		flags := cmd.PersistentFlags()
		flags.Bool("console", false, "whether logger output records to the stdout")
		flags.Bool("verbose", false, "whether logger use debug level")
		flags.Int("pprof-port", -1, "listen port for pprof, 0 represents random port")
		flags.String("jaeger", "", "jaeger endpoint url, like: http://localhost:14250/api/traces")
		flags.String("service-name", fmt.Sprintf("%s-%s", "dragonfly", cmd.Name()), "name of the service for tracer")
		flags.String("config", "", fmt.Sprintf("the path of configuration file with yaml extension name, default is %s, it can also be set by env var: %s", filepath.Join(dfpath.DefaultConfigDir, rootName+".yaml"), strings.ToUpper(rootName+"_config")))

		// Bind common flags
		if err := viper.BindPFlags(flags); err != nil {
			panic(fmt.Errorf("bind common flags to viper: %w", err))
		}

		// Config for binding env
		viper.SetEnvPrefix(rootName)
		viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
		_ = viper.BindEnv("config")

		// Add common cmds only on root cmd
		cmd.AddCommand(VersionCmd)
		cmd.AddCommand(newDocCommand(cmd.Name()))
		cmd.AddCommand(PluginCmd)
	}
}

// InitMonitor initialize monitor and return final handler
func InitMonitor(pprofPort int, otelOption base.TelemetryOption) func() {
	var fc = make(chan func(), 5)

	if pprofPort >= 0 {
		// Enable go pprof and statsview
		go func() {
			if pprofPort == 0 {
				pprofPort, _ = freeport.GetFreePort()
			}

			debugAddr := fmt.Sprintf(":%d", pprofPort)
			viewer.SetConfiguration(viewer.WithAddr(debugAddr))

			logger.With("pprof", fmt.Sprintf("http://%s/debug/pprof", debugAddr),
				"statsview", fmt.Sprintf("http://%s/debug/statsview", debugAddr)).
				Infof("enable pprof at %s", debugAddr)

			vm := statsview.New()
			if err := vm.Start(); err != nil {
				logger.Warnf("serve pprof error: %v", err)
			}
			fc <- func() { vm.Stop() }
		}()
	}

	ff, err := initJaegerTracer(otelOption)
	if err != nil {
		logger.Warnf("init jaeger tracer error: %v", err)
		return func() {}
	}
	fc <- ff

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
				logger.Warnf("receive signal: %v", sig)
				if !done {
					done = true
					handler()
					logger.Warnf("handle signal: %v finish", sig)
				}
			}
		}
	}()
}

// initConfig reads in config file and ENV variables if set.
func initConfig(useConfigFile bool, name string, config any) {
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
			if errors.As(err, &viper.ConfigFileNotFoundError{}) {
				if cfgFile == "" {
					ignoreErr = true
				}
			}
			if !ignoreErr {
				panic(fmt.Errorf("viper read config: %w", err))
			}
		}
	}
	if err := viper.Unmarshal(config, initDecoderConfig); err != nil {
		panic(fmt.Errorf("unmarshal config to struct: %w", err))
	}
}

func LoadConfig(config any) error {
	if err := viper.ReadInConfig(); err != nil {
		return err
	}
	return viper.Unmarshal(config, initDecoderConfig)
}

func WatchConfig(interval time.Duration, newConfig func() (cfg any), watcher func(cfg any)) {
	var oldData string
	file := viper.ConfigFileUsed()

	data, err := os.ReadFile(file)
	if err != nil {
		logger.Errorf("read file %s error: %v", file, err)
	}
	oldData = string(data)
loop:
	for {
		select {
		case <-time.After(interval):
			// for k8s configmap case, the config file is symbol link
			// reload file instead use fsnotify
			data, err = os.ReadFile(file)
			if err != nil {
				logger.Errorf("read file %s error: %v", file, err)
				continue loop
			}
			if oldData != string(data) {
				cfg := newConfig()
				err = LoadConfig(cfg)
				if err != nil {
					logger.Errorf("load config file %s error: %v", file, err)
					continue loop
				}
				logger.Infof("config file %s changed", file)
				watcher(cfg)
				oldData = string(data)
			}
		}
	}
}

func initDecoderConfig(dc *mapstructure.DecoderConfig) {
	dc.DecodeHook = mapstructure.ComposeDecodeHookFunc(func(from, to reflect.Type, v any) (any, error) {
		switch to {
		case reflect.TypeOf(unit.B),
			reflect.TypeOf(dfnet.NetAddr{}),
			reflect.TypeOf(util.RateLimit{}),
			reflect.TypeOf(util.Duration{}),
			reflect.TypeOf(&config.ProxyOption{}),
			reflect.TypeOf(config.TCPListenPortRange{}),
			reflect.TypeOf(types.PEMContent("")),
			reflect.TypeOf(config.URL{}),
			reflect.TypeOf(net.IP{}),
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
	}, dc.DecodeHook)
}

// initTracer creates a new trace provider instance and registers it as global trace provider.
func initJaegerTracer(otelOption base.TelemetryOption) (func(), error) {
	// currently, only support jaeger, otherwise just keeps the context of the caller but does not record spans.
	if otelOption.Jaeger == "" {
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.NeverSample()),
		)
		otel.SetTracerProvider(tp)
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
		return func() {}, nil
	}

	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(otelOption.Jaeger)))
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		// Always be sure to batch in production.
		sdktrace.WithBatcher(exp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		// Record information about this application in an Resource.
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(otelOption.ServiceName),
			semconv.ServiceInstanceIDKey.String(fmt.Sprintf("%s|%s", fqdn.FQDNHostname, ip.IPv4.String())),
			semconv.ServiceNamespaceKey.String("dragonfly"),
			semconv.ServiceVersionKey.String(version.GitVersion))),
	)

	// Register our TracerProvider as the global so any imported
	// instrumentation in the future will default to using it.
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return func() {
		// Do not make the application hang when it is shutdown.
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = tp.Shutdown(ctx)
	}, nil
}
