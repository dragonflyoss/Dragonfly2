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

package config

import (
	"errors"
	"fmt"
	"net"
	"time"

	"d7y.io/dragonfly/v2/cmd/dependency/base"
	"d7y.io/dragonfly/v2/pkg/net/fqdn"
	"d7y.io/dragonfly/v2/pkg/net/ip"
)

type Config struct {
	// Base options.
	base.Options `yaml:",inline" mapstructure:",squash"`

	// Server configuration.
	Server ServerConfig `yaml:"server" mapstructure:"server"`

	// Scheduler configuration.
	Scheduler SchedulerConfig `yaml:"scheduler" mapstructure:"scheduler"`

	// Database configuration.
	Database DatabaseConfig `yaml:"database" mapstructure:"database"`

	// Dynconfig configuration.
	DynConfig DynConfig `yaml:"dynConfig" mapstructure:"dynConfig"`

	// Manager configuration.
	Manager ManagerConfig `yaml:"manager" mapstructure:"manager"`

	// SeedPeer configuration.
	SeedPeer SeedPeerConfig `yaml:"seedPeer" mapstructure:"seedPeer"`

	// Peer configuration.
	Peer PeerConfig `yaml:"peer" mapstructure:"peer"`

	// Host configuration.
	Host HostConfig `yaml:"host" mapstructure:"host"`

	// Job configuration.
	Job JobConfig `yaml:"job" mapstructure:"job"`

	// Storage configuration.
	Storage StorageConfig `yaml:"storage" mapstructure:"storage"`

	// Metrics configuration.
	Metrics MetricsConfig `yaml:"metrics" mapstructure:"metrics"`

	// Network configuration.
	Network NetworkConfig `yaml:"network" mapstructure:"network"`
}

type ServerConfig struct {
	// AdvertiseIP is advertise ip.
	AdvertiseIP net.IP `yaml:"advertiseIP" mapstructure:"advertiseIP"`

	// AdvertisePort is advertise port.
	AdvertisePort int `yaml:"advertisePort" mapstructure:"advertisePort"`

	// ListenIP is listen ip, like: 0.0.0.0, 192.168.0.1.
	ListenIP net.IP `yaml:"listenIP" mapstructure:"listenIP"`

	// Server port.
	Port int `yaml:"port" mapstructure:"port"`

	// Server hostname.
	Host string `yaml:"host" mapstructure:"host"`

	// TLS server configuration.
	TLS *GRPCTLSServerConfig `yaml:"tls" mapstructure:"tls"`

	// Server dynamic config cache directory.
	CacheDir string `yaml:"cacheDir" mapstructure:"cacheDir"`

	// Server log directory.
	LogDir string `yaml:"logDir" mapstructure:"logDir"`

	// Maximum size in megabytes of log files before rotation (default: 1024)
	LogMaxSize int `yaml:"logMaxSize" mapstructure:"logMaxSize"`

	// Maximum number of days to retain old log files (default: 7)
	LogMaxAge int `yaml:"logMaxAge" mapstructure:"logMaxAge"`

	// Maximum number of old log files to keep (default: 20)
	LogMaxBackups int `yaml:"logMaxBackups" mapstructure:"logMaxBackups"`

	// Server plugin directory.
	PluginDir string `yaml:"pluginDir" mapstructure:"pluginDir"`

	// Server storage data directory.
	DataDir string `yaml:"dataDir" mapstructure:"dataDir"`
}

type GRPCTLSServerConfig struct {
	// CACert is the file path of CA certificate for mTLS.
	CACert string `yaml:"caCert" mapstructure:"caCert"`

	// Cert is the file path of server certificate for mTLS.
	Cert string `yaml:"cert" mapstructure:"cert"`

	// Key is the file path of server key for mTLS.
	Key string `yaml:"key" mapstructure:"key"`
}

type SchedulerConfig struct {
	// Algorithm is scheduling algorithm used by the scheduler.
	Algorithm string `yaml:"algorithm" mapstructure:"algorithm"`

	// BackToSourceCount is single task allows the peer to back-to-source count.
	BackToSourceCount int `yaml:"backToSourceCount" mapstructure:"backToSourceCount"`

	// RetryBackToSourceLimit reaches the limit, then the peer back-to-source.
	RetryBackToSourceLimit int `yaml:"retryBackToSourceLimit" mapstructure:"retryBackToSourceLimit"`

	// RetryLimit reaches the limit, then scheduler returns scheduling failed.
	RetryLimit int `yaml:"retryLimit" mapstructure:"retryLimit"`

	// RetryInterval is scheduling interval.
	RetryInterval time.Duration `yaml:"retryInterval" mapstructure:"retryInterval"`

	// GC configuration.
	GC GCConfig `yaml:"gc" mapstructure:"gc"`
}

type DatabaseConfig struct {
	// Redis configuration.
	Redis RedisConfig `yaml:"redis" mapstructure:"redis"`
}

type DownloadTinyConfig struct {
	// Scheme is download tiny task scheme.
	Scheme string `yaml:"scheme" mapstructure:"scheme"`

	// Timeout is http request timeout.
	Timeout time.Duration `yaml:"timeout" mapstructure:"timeout"`

	// TLS is download tiny task TLS configuration.
	TLS DownloadTinyTLSClientConfig `yaml:"tls" mapstructure:"tls"`
}

type DownloadTinyTLSClientConfig struct {
	// InsecureSkipVerify controls whether a client verifies the
	// server's certificate chain and host name.
	InsecureSkipVerify bool `yaml:"insecureSkipVerify" mapstructure:"insecureSkipVerify"`
}

type GCConfig struct {
	// PieceDownloadTimeout is timeout of downloading piece.
	PieceDownloadTimeout time.Duration `yaml:"pieceDownloadTimeout" mapstructure:"pieceDownloadTimeout"`

	// PeerGCInterval is interval of peer gc.
	PeerGCInterval time.Duration `yaml:"peerGCInterval" mapstructure:"peerGCInterval"`

	// PeerTTL is time to live of peer. If the peer has been downloaded by other peers,
	// then PeerTTL will be reset.
	PeerTTL time.Duration `yaml:"peerTTL" mapstructure:"peerTTL"`

	// TaskGCInterval is interval of task gc. If all the peers have been reclaimed in the task,
	// then the task will also be reclaimed.
	TaskGCInterval time.Duration `yaml:"taskGCInterval" mapstructure:"taskGCInterval"`

	// HostGCInterval is interval of host gc.
	HostGCInterval time.Duration `yaml:"hostGCInterval" mapstructure:"hostGCInterval"`

	// HostTTL is time to live of host. If host announces message to scheduler,
	// then HostTTl will be reset.
	HostTTL time.Duration `yaml:"hostTTL" mapstructure:"hostTTL"`
}

type DynConfig struct {
	// RefreshInterval is refresh interval for manager cache.
	RefreshInterval time.Duration `yaml:"refreshInterval" mapstructure:"refreshInterval"`
}

type HostConfig struct {
	// IDC for scheduler.
	IDC string `mapstructure:"idc" yaml:"idc"`

	// Location for scheduler.
	Location string `mapstructure:"location" yaml:"location"`
}

type ManagerConfig struct {
	// Addr is manager address.
	Addr string `yaml:"addr" mapstructure:"addr"`

	// TLS client configuration.
	TLS *GRPCTLSClientConfig `yaml:"tls" mapstructure:"tls"`

	// SchedulerClusterID is scheduler cluster id.
	SchedulerClusterID uint `yaml:"schedulerClusterID" mapstructure:"schedulerClusterID"`

	// KeepAlive configuration.
	KeepAlive KeepAliveConfig `yaml:"keepAlive" mapstructure:"keepAlive"`
}

type GRPCTLSClientConfig struct {
	// CACert is the file path of CA certificate for mTLS.
	CACert string `yaml:"caCert" mapstructure:"caCert"`

	// Cert is the file path of client certificate for mTLS.
	Cert string `yaml:"cert" mapstructure:"cert"`

	// Key is the file path of client key for mTLS.
	Key string `yaml:"key" mapstructure:"key"`
}

type SeedPeerConfig struct {
	// Enable is to enable seed peer as P2P peer.
	Enable bool `yaml:"enable" mapstructure:"enable"`

	// TLS client configuration.
	TLS *GRPCTLSClientConfig `yaml:"tls" mapstructure:"tls"`

	// TaskDownloadTimeout is timeout of downloading task by seed peer.
	TaskDownloadTimeout time.Duration `yaml:"taskDownloadTimeout" mapstructure:"taskDownloadTimeout"`
}

type PeerConfig struct {
	// TLS client configuration.
	TLS *GRPCTLSClientConfig `yaml:"tls" mapstructure:"tls"`
}

type KeepAliveConfig struct {
	// Keep alive interval.
	Interval time.Duration `yaml:"interval" mapstructure:"interval"`
}

type JobConfig struct {
	// Enable job service.
	Enable bool `yaml:"enable" mapstructure:"enable"`

	// Number of workers in global queue.
	GlobalWorkerNum uint `yaml:"globalWorkerNum" mapstructure:"globalWorkerNum"`

	// Number of workers in scheduler queue.
	SchedulerWorkerNum uint `yaml:"schedulerWorkerNum" mapstructure:"schedulerWorkerNum"`

	// Number of workers in local queue.
	LocalWorkerNum uint `yaml:"localWorkerNum" mapstructure:"localWorkerNum"`

	// DEPRECATED: Please use the `database.redis` field instead.
	Redis RedisConfig `yaml:"redis" mapstructure:"redis"`
}

type StorageConfig struct {
	// MaxSize sets the maximum size in megabytes of storage file.
	MaxSize int `yaml:"maxSize" mapstructure:"maxSize"`

	// MaxBackups sets the maximum number of storage files to retain.
	MaxBackups int `yaml:"maxBackups" mapstructure:"maxBackups"`

	// BufferSize sets the size of buffer container,
	// if the buffer is full, write all the records in the buffer to the file.
	BufferSize int `yaml:"bufferSize" mapstructure:"bufferSize"`
}

type RedisConfig struct {
	// DEPRECATED: Please use the `addrs` field instead.
	Host string `yaml:"host" mapstructure:"host"`

	// DEPRECATED: Please use the `addrs` field instead.
	Port int `yaml:"port" mapstructure:"port"`

	// Addrs is server addresses.
	Addrs []string `yaml:"addrs" mapstructure:"addrs"`

	// MasterName is the sentinel master name.
	MasterName string `yaml:"masterName" mapstructure:"masterName"`

	// Username is server username.
	Username string `yaml:"username" mapstructure:"username"`

	// Password is server password.
	Password string `yaml:"password" mapstructure:"password"`

	// SentinelUsername is sentinel server username.
	SentinelUsername string `yaml:"sentinelUsername" mapstructure:"sentinelUsername"`

	// SentinelPassword is sentinel server password.
	SentinelPassword string `yaml:"sentinelPassword" mapstructure:"sentinelPassword"`

	// BrokerDB is broker database name.
	BrokerDB int `yaml:"brokerDB" mapstructure:"brokerDB"`

	// BackendDB is backend database name.
	BackendDB int `yaml:"backendDB" mapstructure:"backendDB"`
}

type MetricsConfig struct {
	// Enable metrics service.
	Enable bool `yaml:"enable" mapstructure:"enable"`

	// Metrics service address.
	Addr string `yaml:"addr" mapstructure:"addr"`

	// Enable host metrics.
	EnableHost bool `yaml:"enableHost" mapstructure:"enableHost"`
}

type NetworkConfig struct {
	// EnableIPv6 enables ipv6 for server.
	EnableIPv6 bool `mapstructure:"enableIPv6" yaml:"enableIPv6"`
}

// New default configuration.
func New() *Config {
	return &Config{
		Server: ServerConfig{
			Port:          DefaultServerPort,
			AdvertisePort: DefaultServerAdvertisePort,
			Host:          fqdn.FQDNHostname,
			LogMaxSize:    DefaultLogRotateMaxSize,
			LogMaxAge:     DefaultLogRotateMaxAge,
			LogMaxBackups: DefaultLogRotateMaxBackups,
		},
		Scheduler: SchedulerConfig{
			Algorithm:              DefaultSchedulerAlgorithm,
			BackToSourceCount:      DefaultSchedulerBackToSourceCount,
			RetryBackToSourceLimit: DefaultSchedulerRetryBackToSourceLimit,
			RetryLimit:             DefaultSchedulerRetryLimit,
			RetryInterval:          DefaultSchedulerRetryInterval,
			GC: GCConfig{
				PieceDownloadTimeout: DefaultSchedulerPieceDownloadTimeout,
				PeerGCInterval:       DefaultSchedulerPeerGCInterval,
				PeerTTL:              DefaultSchedulerPeerTTL,
				TaskGCInterval:       DefaultSchedulerTaskGCInterval,
				HostGCInterval:       DefaultSchedulerHostGCInterval,
				HostTTL:              DefaultSchedulerHostTTL,
			},
		},
		Database: DatabaseConfig{
			Redis: RedisConfig{
				BrokerDB:  DefaultRedisBrokerDB,
				BackendDB: DefaultRedisBackendDB,
			},
		},
		DynConfig: DynConfig{
			RefreshInterval: DefaultDynConfigRefreshInterval,
		},
		Host: HostConfig{},
		Manager: ManagerConfig{
			SchedulerClusterID: DefaultManagerSchedulerClusterID,
			KeepAlive: KeepAliveConfig{
				Interval: DefaultManagerKeepAliveInterval,
			},
		},
		SeedPeer: SeedPeerConfig{
			Enable:              true,
			TaskDownloadTimeout: DefaultSeedPeerTaskDownloadTimeout,
		},
		Job: JobConfig{
			Enable:             true,
			GlobalWorkerNum:    DefaultJobGlobalWorkerNum,
			SchedulerWorkerNum: DefaultJobSchedulerWorkerNum,
			LocalWorkerNum:     DefaultJobLocalWorkerNum,
		},
		Storage: StorageConfig{
			MaxSize:    DefaultStorageMaxSize,
			MaxBackups: DefaultStorageMaxBackups,
			BufferSize: DefaultStorageBufferSize,
		},
		Metrics: MetricsConfig{
			Enable:     false,
			Addr:       DefaultMetricsAddr,
			EnableHost: false,
		},
		Network: NetworkConfig{
			EnableIPv6: DefaultNetworkEnableIPv6,
		},
	}
}

// Validate config parameters.
func (cfg *Config) Validate() error {
	if cfg.Server.AdvertiseIP == nil {
		return errors.New("server requires parameter advertiseIP")
	}

	if cfg.Server.AdvertisePort <= 0 {
		return errors.New("server requires parameter advertisePort")
	}

	if cfg.Server.ListenIP == nil {
		return errors.New("server requires parameter listenIP")
	}

	if cfg.Server.Port <= 0 {
		return errors.New("server requires parameter port")
	}

	if cfg.Server.Host == "" {
		return errors.New("server requires parameter host")
	}

	if cfg.Server.TLS != nil {
		if cfg.Server.TLS.CACert == "" {
			return errors.New("server tls requires parameter caCert")
		}

		if cfg.Server.TLS.Cert == "" {
			return errors.New("server tls requires parameter cert")
		}

		if cfg.Server.TLS.Key == "" {
			return errors.New("server tls requires parameter key")
		}
	}

	if cfg.Scheduler.Algorithm == "" {
		return errors.New("scheduler requires parameter algorithm")
	}

	if cfg.Scheduler.BackToSourceCount == 0 {
		return errors.New("scheduler requires parameter backToSourceCount")
	}

	if cfg.Scheduler.RetryBackToSourceLimit == 0 {
		return errors.New("scheduler requires parameter retryBackToSourceLimit")
	}

	if cfg.Scheduler.RetryLimit <= 0 {
		return errors.New("scheduler requires parameter retryLimit")
	}

	if cfg.Scheduler.RetryInterval <= 0 {
		return errors.New("scheduler requires parameter retryInterval")
	}

	if cfg.Scheduler.GC.PieceDownloadTimeout <= 0 {
		return errors.New("scheduler requires parameter pieceDownloadTimeout")
	}

	if cfg.Scheduler.GC.PeerTTL <= 0 {
		return errors.New("scheduler requires parameter peerTTL")
	}

	if cfg.Scheduler.GC.PeerGCInterval <= 0 {
		return errors.New("scheduler requires parameter peerGCInterval")
	}

	if cfg.Scheduler.GC.TaskGCInterval <= 0 {
		return errors.New("scheduler requires parameter taskGCInterval")
	}

	if cfg.Scheduler.GC.HostGCInterval <= 0 {
		return errors.New("scheduler requires parameter hostGCInterval")
	}

	if cfg.Scheduler.GC.HostTTL <= 0 {
		return errors.New("scheduler requires parameter hostTTL")
	}

	if cfg.Database.Redis.BrokerDB < 0 {
		return errors.New("redis requires parameter brokerDB")
	}

	if cfg.Database.Redis.BackendDB < 0 {
		return errors.New("redis requires parameter backendDB")
	}

	if cfg.DynConfig.RefreshInterval <= 0 {
		return errors.New("dynconfig requires parameter refreshInterval")
	}

	if cfg.Manager.Addr == "" {
		return errors.New("manager requires parameter addr")
	}

	if cfg.Manager.TLS != nil {
		if cfg.Manager.TLS.CACert == "" {
			return errors.New("manager tls requires parameter caCert")
		}

		if cfg.Manager.TLS.Cert == "" {
			return errors.New("manager tls requires parameter cert")
		}

		if cfg.Manager.TLS.Key == "" {
			return errors.New("manager tls requires parameter key")
		}
	}

	if cfg.Manager.SchedulerClusterID == 0 {
		return errors.New("manager requires parameter schedulerClusterID")
	}

	if cfg.Manager.KeepAlive.Interval <= 0 {
		return errors.New("manager requires parameter keepAlive interval")
	}

	if cfg.SeedPeer.TaskDownloadTimeout <= 0 {
		return errors.New("seedPeer requires parameter taskDownloadTimeout")
	}

	if cfg.SeedPeer.TLS != nil {
		if cfg.SeedPeer.TLS.CACert == "" {
			return errors.New("seedPeer tls requires parameter caCert")
		}

		if cfg.SeedPeer.TLS.Cert == "" {
			return errors.New("seedPeer tls requires parameter cert")
		}

		if cfg.SeedPeer.TLS.Key == "" {
			return errors.New("seedPeer tls requires parameter key")
		}
	}

	if cfg.Job.Enable {
		if cfg.Job.GlobalWorkerNum == 0 {
			return errors.New("job requires parameter globalWorkerNum")
		}

		if cfg.Job.SchedulerWorkerNum == 0 {
			return errors.New("job requires parameter schedulerWorkerNum")
		}

		if cfg.Job.LocalWorkerNum == 0 {
			return errors.New("job requires parameter localWorkerNum")
		}
	}

	if cfg.Storage.MaxSize <= 0 {
		return errors.New("storage requires parameter maxSize")
	}

	if cfg.Storage.MaxBackups <= 0 {
		return errors.New("storage requires parameter maxBackups")
	}

	if cfg.Storage.BufferSize < 0 {
		return errors.New("storage requires parameter bufferSize")
	}

	if cfg.Metrics.Enable {
		if cfg.Metrics.Addr == "" {
			return errors.New("metrics requires parameter addr")
		}
	}

	return nil
}

func (cfg *Config) Convert() error {
	// TODO Compatible with deprecated fields address of redis of job.
	if len(cfg.Database.Redis.Addrs) == 0 && len(cfg.Job.Redis.Addrs) != 0 {
		cfg.Database.Redis.Addrs = cfg.Job.Redis.Addrs
	}

	// TODO Compatible with deprecated fields host and port of redis of job.
	if len(cfg.Database.Redis.Addrs) == 0 && len(cfg.Job.Redis.Addrs) == 0 && cfg.Job.Redis.Host != "" && cfg.Job.Redis.Port > 0 {
		cfg.Database.Redis.Addrs = []string{fmt.Sprintf("%s:%d", cfg.Job.Redis.Host, cfg.Job.Redis.Port)}
	}

	// TODO Compatible with deprecated fields master name of redis of job.
	if cfg.Database.Redis.MasterName == "" && cfg.Job.Redis.MasterName != "" {
		cfg.Database.Redis.MasterName = cfg.Job.Redis.MasterName
	}

	// TODO Compatible with deprecated fields user name of redis of job.
	if cfg.Database.Redis.Username == "" && cfg.Job.Redis.Username != "" {
		cfg.Database.Redis.Username = cfg.Job.Redis.Username
	}

	// TODO Compatible with deprecated fields password of redis of job.
	if cfg.Database.Redis.Password == "" && cfg.Job.Redis.Password != "" {
		cfg.Database.Redis.Password = cfg.Job.Redis.Password
	}

	// TODO Compatible with deprecated fields broker database of redis of job.
	if cfg.Database.Redis.BrokerDB == 0 && cfg.Job.Redis.BrokerDB != 0 {
		cfg.Database.Redis.BrokerDB = cfg.Job.Redis.BrokerDB
	}

	// TODO Compatible with deprecated fields backend database of redis of job.
	if cfg.Database.Redis.BackendDB == 0 && cfg.Job.Redis.BackendDB != 0 {
		cfg.Database.Redis.BackendDB = cfg.Job.Redis.BackendDB
	}

	if cfg.Server.AdvertiseIP == nil {
		if cfg.Network.EnableIPv6 {
			cfg.Server.AdvertiseIP = ip.IPv6
		} else {
			cfg.Server.AdvertiseIP = ip.IPv4
		}
	}

	if cfg.Server.ListenIP == nil {
		if cfg.Network.EnableIPv6 {
			cfg.Server.ListenIP = net.IPv6zero
		} else {
			cfg.Server.ListenIP = net.IPv4zero
		}
	}

	if cfg.Server.AdvertisePort == 0 {
		cfg.Server.AdvertisePort = cfg.Server.Port
	}

	return nil
}
