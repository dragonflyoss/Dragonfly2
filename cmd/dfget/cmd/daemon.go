/*
 * Copyright The Dragonfly Authors.
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
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/gofrs/flock"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/trace/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon"
	"d7y.io/dragonfly/v2/client/pidfile"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/dflog/logcore"
	_ "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/server"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

var daemonConfig *config.PeerHostOption

var daemonCmd = &cobra.Command{
	Use:          "daemon",
	Short:        "Launch a peer daemon for downloading and uploading files.",
	SilenceUsage: true,

	RunE: func(cmd *cobra.Command, args []string) error {
		// Convert config
		if err := daemonConfig.Convert(); err != nil {
			return err
		}

		// Validate config
		if err := daemonConfig.Validate(); err != nil {
			return err
		}

		// Initialize logger
		logcore.InitDaemon(daemonConfig.Console)

		// Initialize telemetry
		if daemonConfig.Telemetry.Jaeger != "" {
			flush, err := initTracer(daemonConfig.Telemetry.Jaeger)
			if err != nil {
				logger.Errorf("initialize trace for jaeger error: %s", err)
			} else {
				logger.Infof("initialize trace for jaeger at %s", daemonConfig.Telemetry.Jaeger)
				defer flush()
			}
		}

		// Start daemon
		return runDaemon()
	},
}

func init() {
	// Initialize default daemon config
	daemonConfig = &config.PeerHostConfig

	// Initialize cobra
	initDaemonConfig(config.PeerHostConfigPath)

	// Add flags
	flagSet := daemonCmd.Flags()
	flagSet.StringVar(&daemonConfig.DataDir, "data", daemonConfig.DataDir, "local directory which stores temporary files for p2p uploading")
	flagSet.DurationVar(&daemonConfig.GCInterval.Duration, "gc-interval", daemonConfig.GCInterval.Duration, "gc interval")
	flagSet.BoolVar(&daemonConfig.KeepStorage, "keep-storage", daemonConfig.KeepStorage, "keep storage after daemon exit")
	flagSet.BoolVar(&daemonConfig.Verbose, "verbose", daemonConfig.Verbose, "print verbose log and enable golang debug info")
	flagSet.BoolVar(&daemonConfig.Console, "console", daemonConfig.Console, "console shows log on console")
	flagSet.StringVar(&daemonConfig.Host.AdvertiseIP, "advertise-ip", daemonConfig.Host.AdvertiseIP, "the ip report to scheduler, normal same with listen ip")
	flagSet.StringVar(&daemonConfig.Download.DownloadGRPC.UnixListen.Socket, "grpc-unix-listen", daemonConfig.Download.DownloadGRPC.UnixListen.Socket, "the local unix domain socket listen address for grpc with dfget")
	flagSet.IntVar(&daemonConfig.Download.PeerGRPC.TCPListen.PortRange.Start, "grpc-port", daemonConfig.Download.PeerGRPC.TCPListen.PortRange.Start, "the listen address for grpc with other peers")
	flagSet.IntVar(&daemonConfig.Download.PeerGRPC.TCPListen.PortRange.End, "grpc-port-end", daemonConfig.Download.PeerGRPC.TCPListen.PortRange.End, "the listen address for grpc with other peers")
	flagSet.IntVar(&daemonConfig.Upload.ListenOption.TCPListen.PortRange.Start, "upload-port", daemonConfig.Upload.ListenOption.TCPListen.PortRange.Start, "the address that daemon will listen on for peer upload")
	flagSet.IntVar(&daemonConfig.Upload.ListenOption.TCPListen.PortRange.End, "upload-port-end", daemonConfig.Upload.ListenOption.TCPListen.PortRange.End, "the address that daemon will listen on for peer upload")
	flagSet.StringVar(&daemonConfig.PidFile, "pid", daemonConfig.PidFile, "dfdaemon pid file location")
	flagSet.StringVar(&daemonConfig.LockFile, "lock", daemonConfig.LockFile, "dfdaemon lock file location")
	flagSet.StringVar(&daemonConfig.Host.SecurityDomain, "security-domain", daemonConfig.Host.SecurityDomain, "peer security domain for scheduler")
	flagSet.StringVar(&daemonConfig.Host.Location, "location", daemonConfig.Host.Location, "peer location for scheduler")
	flagSet.StringVar(&daemonConfig.Host.IDC, "idc", daemonConfig.Host.IDC, "peer idc for scheduler")
	flagSet.StringVar(&daemonConfig.Host.NetTopology, "net-topology", daemonConfig.Host.NetTopology, "peer net topology for scheduler")
	flagSet.Var(config.NewLimitRateValue(&daemonConfig.Download.TotalRateLimit), "download-rate", "download rate limit for other peers and back source")
	flagSet.Var(config.NewLimitRateValue(&daemonConfig.Upload.RateLimit), "upload-rate", "upload rate limit for other peers")
	flagSet.DurationVar(&daemonConfig.Scheduler.ScheduleTimeout.Duration, "schedule-timeout", daemonConfig.Scheduler.ScheduleTimeout.Duration, "schedule timeout")
	flagSet.StringVar(&daemonConfig.Telemetry.Jaeger, "jaeger", daemonConfig.Telemetry.Jaeger, "jaeger addr, like: http://localhost:14268")
	flagSet.String("config", config.PeerHostConfigPath, "daemon config file location")

	// Add command
	rootCmd.AddCommand(daemonCmd)
}

// initDaemonConfig reads in config file if set
func initDaemonConfig(cfgPath string) {
	var flagPath string
	for i, v := range os.Args {
		if v == "--config" && i+1 < len(os.Args) {
			flagPath = os.Args[i+1]
		}
	}

	if flagPath != "" {
		cfgPath = flagPath
	}

	_, err := os.Stat(cfgPath)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}

		fmt.Println(err)
		os.Exit(1)
	}

	// Load from config file
	if err := daemonConfig.Load(cfgPath); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// initTracer creates a new trace provider instance and registers it as global trace provider.
func initTracer(addr string) (func(), error) {
	// Create and install Jaeger export pipeline.
	flush, err := jaeger.InstallNewPipeline(
		jaeger.WithCollectorEndpoint(fmt.Sprintf("%s/api/traces", addr)),
		jaeger.WithSDKOptions(
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
			sdktrace.WithResource(resource.NewWithAttributes(
				semconv.ServiceNameKey.String("dragonfly"),
				attribute.String("exporter", "jaeger"),
			)),
		),
	)
	if err != nil {
		return nil, err
	}
	return flush, nil
}

func runDaemon() error {
	s, _ := json.MarshalIndent(daemonConfig, "", "  ")
	logger.Debugf("daemon option(debug only, can not use as config):\n%s", string(s))

	// Initialize lock file
	lock := flock.New(daemonConfig.LockFile)
	if ok, err := lock.TryLock(); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("lock file %s failed, other daemon is already running", daemonConfig.LockFile)
	}
	defer lock.Unlock()

	// Initialize pid file
	pid, err := pidfile.New(daemonConfig.PidFile)
	if err != nil {
		return fmt.Errorf("check pid failed: %s, please check %s", err, daemonConfig.PidFile)
	}
	defer pid.Remove()

	// Initialize verbose mode
	initVerboseMode(daemonConfig.Verbose)

	ph, err := daemon.NewPeerHost(&scheduler.PeerHost{
		Uuid:           uuid.New().String(),
		Ip:             daemonConfig.Host.AdvertiseIP,
		RpcPort:        int32(daemonConfig.Download.PeerGRPC.TCPListen.PortRange.Start),
		DownPort:       0,
		HostName:       iputils.HostName,
		SecurityDomain: daemonConfig.Host.SecurityDomain,
		Location:       daemonConfig.Host.Location,
		Idc:            daemonConfig.Host.IDC,
		NetTopology:    daemonConfig.Host.NetTopology,
	}, *daemonConfig)
	if err != nil {
		logger.Errorf("init peer host failed: %s", err)
		return err
	}

	setupSignalHandler(ph)
	return ph.Serve()
}

func setupSignalHandler(ph daemon.PeerHost) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		var done bool
		for {
			select {
			case sig := <-sigs:
				logger.Infof("receive %s signal", sig)
				if !done {
					ph.Stop()
					done = true
				}
			}
		}
	}()
}
