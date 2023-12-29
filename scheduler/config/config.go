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
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/slices"
	"d7y.io/dragonfly/v2/pkg/types"
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

	// Resource configuration.
	Resource ResourceConfig `yaml:"resource" mapstructure:"resource"`

	// Dynconfig configuration.
	DynConfig DynConfig `yaml:"dynConfig" mapstructure:"dynConfig"`

	// Manager configuration.
	Manager ManagerConfig `yaml:"manager" mapstructure:"manager"`

	// SeedPeer configuration.
	SeedPeer SeedPeerConfig `yaml:"seedPeer" mapstructure:"seedPeer"`

	// Host configuration.
	Host HostConfig `yaml:"host" mapstructure:"host"`

	// Job configuration.
	Job JobConfig `yaml:"job" mapstructure:"job"`

	// Storage configuration.
	Storage StorageConfig `yaml:"storage" mapstructure:"storage"`

	// Metrics configuration.
	Metrics MetricsConfig `yaml:"metrics" mapstructure:"metrics"`

	// Security configuration.
	Security SecurityConfig `yaml:"security" mapstructure:"security"`

	// Network configuration.
	Network NetworkConfig `yaml:"network" mapstructure:"network"`

	// NetworkTopology configuration.
	NetworkTopology NetworkTopologyConfig `yaml:"networkTopology" mapstructure:"networkTopology"`

	// Trainer configuration.
	Trainer TrainerConfig `yaml:"trainer" mapstructure:"trainer"`
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

	// Server work directory.
	WorkHome string `yaml:"workHome" mapstructure:"workHome"`

	// Server dynamic config cache directory.
	CacheDir string `yaml:"cacheDir" mapstructure:"cacheDir"`

	// Server log directory.
	LogDir string `yaml:"logDir" mapstructure:"logDir"`

	// Server plugin directory.
	PluginDir string `yaml:"pluginDir" mapstructure:"pluginDir"`

	// Server storage data directory.
	DataDir string `yaml:"dataDir" mapstructure:"dataDir"`
}

type SchedulerConfig struct {
	// Algorithm is scheduling algorithm used by the scheduler.
	Algorithm string `yaml:"algorithm" mapstructure:"algorithm"`

	// MaxScheduleCount is max schedule count.
	MaxScheduleCount int `yaml:"maxScheduleCount" mapstructure:"maxScheduleCount"`

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

type ResourceConfig struct {
	// Task resource configuration.
	Task TaskConfig `yaml:"task" mapstructure:"task"`
}

type TaskConfig struct {
	// Download tiny task configuration.
	DownloadTiny DownloadTinyConfig `yaml:"downloadTiny" mapstructure:"downloadTiny"`
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

	// SchedulerClusterID is scheduler cluster id.
	SchedulerClusterID uint `yaml:"schedulerClusterID" mapstructure:"schedulerClusterID"`

	// KeepAlive configuration.
	KeepAlive KeepAliveConfig `yaml:"keepAlive" mapstructure:"keepAlive"`
}

type SeedPeerConfig struct {
	// Enable is to enable seed peer as P2P peer.
	Enable bool `yaml:"enable" mapstructure:"enable"`

	// TaskDownloadTimeout is timeout of downloading task by seed peer.
	TaskDownloadTimeout time.Duration `yaml:"taskDownloadTimeout" mapstructure:"taskDownloadTimeout"`
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

	// BrokerDB is broker database name.
	BrokerDB int `yaml:"brokerDB" mapstructure:"brokerDB"`

	// BackendDB is backend database name.
	BackendDB int `yaml:"backendDB" mapstructure:"backendDB"`

	// NetworkTopologyDB is network topology database name.
	NetworkTopologyDB int `yaml:"networkTopologyDB" mapstructure:"networkTopologyDB"`
}

type MetricsConfig struct {
	// Enable metrics service.
	Enable bool `yaml:"enable" mapstructure:"enable"`

	// Metrics service address.
	Addr string `yaml:"addr" mapstructure:"addr"`

	// Enable host metrics.
	EnableHost bool `yaml:"enableHost" mapstructure:"enableHost"`
}

type SecurityConfig struct {
	// AutoIssueCert indicates to issue client certificates for all grpc call
	// if AutoIssueCert is false, any other option in Security will be ignored.
	AutoIssueCert bool `mapstructure:"autoIssueCert" yaml:"autoIssueCert"`

	// CACert is the root CA certificate for all grpc tls handshake, it can be path or PEM format string.
	CACert types.PEMContent `mapstructure:"caCert" yaml:"caCert"`

	// TLSVerify indicates to verify client certificates.
	TLSVerify bool `mapstructure:"tlsVerify" yaml:"tlsVerify"`

	// TLSPolicy controls the grpc shandshake behaviors:
	// force: both ClientHandshake and ServerHandshake are only support tls.
	// prefer: ServerHandshake supports tls and insecure (non-tls), ClientHandshake will only support tls.
	// default: ServerHandshake supports tls and insecure (non-tls), ClientHandshake will only support insecure (non-tls).
	TLSPolicy string `mapstructure:"tlsPolicy" yaml:"tlsPolicy"`

	// CertSpec is the desired state of certificate.
	CertSpec CertSpec `mapstructure:"certSpec" yaml:"certSpec"`
}

type CertSpec struct {
	// DNSNames is a list of dns names be set on the certificate.
	DNSNames []string `mapstructure:"dnsNames" yaml:"dnsNames"`

	// IPAddresses is a list of ip addresses be set on the certificate.
	IPAddresses []net.IP `mapstructure:"ipAddresses" yaml:"ipAddresses"`

	// ValidityPeriod is the validity period of certificate.
	ValidityPeriod time.Duration `mapstructure:"validityPeriod" yaml:"validityPeriod"`
}

type NetworkConfig struct {
	// EnableIPv6 enables ipv6 for server.
	EnableIPv6 bool `mapstructure:"enableIPv6" yaml:"enableIPv6"`
}

type NetworkTopologyConfig struct {
	// Enable network topology service, including probe, network topology collection.
	Enable bool `yaml:"enable" mapstructure:"enable"`

	// CollectInterval is the interval of collecting network topology.
	CollectInterval time.Duration `mapstructure:"collectInterval" yaml:"collectInterval"`

	// Probe is the configuration of probe.
	Probe ProbeConfig `yaml:"probe" mapstructure:"probe"`

	// Cache is the configuration of cache.
	Cache CacheConfig `yaml:"cache" mapstructure:"cache"`
}

type ProbeConfig struct {
	// QueueLength is the length of probe queue.
	QueueLength int `mapstructure:"queueLength" yaml:"queueLength"`

	// Count is the number of probing hosts.
	Count int `mapstructure:"count" yaml:"count"`
}

type CacheConfig struct {
	// Interval is cache cleanup interval.
	Interval time.Duration `yaml:"interval" mapstructure:"interval"`

	// TTL is networkTopology cache items TLL.
	TTL time.Duration `yaml:"ttl" mapstructure:"tll"`
}

type TrainerConfig struct {
	// Enable trainer service.
	Enable bool `yaml:"enable" mapstructure:"enable"`

	// Addr is trainer service address.
	Addr string `yaml:"addr" mapstructure:"addr"`

	// Interval is the interval of training.
	Interval time.Duration `yaml:"interval" mapstructure:"interval"`

	// UploadTimeout is the timeout of uploading dataset to trainer.
	UploadTimeout time.Duration `yaml:"uploadTimeout" mapstructure:"uploadTimeout"`
}

// New default configuration.
func New() *Config {
	return &Config{
		Server: ServerConfig{
			Port:          DefaultServerPort,
			AdvertisePort: DefaultServerAdvertisePort,
			Host:          fqdn.FQDNHostname,
		},
		Scheduler: SchedulerConfig{
			Algorithm:              DefaultSchedulerAlgorithm,
			MaxScheduleCount:       DefaultSchedulerMaxScheduleCount,
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
				BrokerDB:          DefaultRedisBrokerDB,
				BackendDB:         DefaultRedisBackendDB,
				NetworkTopologyDB: DefaultNetworkTopologyDB,
			},
		},
		Resource: ResourceConfig{
			Task: TaskConfig{
				DownloadTiny: DownloadTinyConfig{
					Scheme:  DefaultResourceTaskDownloadTinyScheme,
					Timeout: DefaultResourceTaskDownloadTinyTimeout,
					TLS: DownloadTinyTLSClientConfig{
						InsecureSkipVerify: true,
					},
				},
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
		Security: SecurityConfig{
			AutoIssueCert: false,
			TLSVerify:     true,
			TLSPolicy:     rpc.PreferTLSPolicy,
			CertSpec: CertSpec{
				DNSNames:       DefaultCertDNSNames,
				IPAddresses:    DefaultCertIPAddresses,
				ValidityPeriod: DefaultCertValidityPeriod,
			},
		},
		Network: NetworkConfig{
			EnableIPv6: DefaultNetworkEnableIPv6,
		},
		NetworkTopology: NetworkTopologyConfig{
			Enable:          true,
			CollectInterval: DefaultNetworkTopologyCollectInterval,
			Probe: ProbeConfig{
				QueueLength: DefaultProbeQueueLength,
				Count:       DefaultProbeCount,
			},
			Cache: CacheConfig{
				Interval: DefaultNetworkTopologyCacheInterval,
				TTL:      DefaultNetworkTopologyCacheTLL,
			},
		},
		Trainer: TrainerConfig{
			Enable:        false,
			Addr:          DefaultTrainerAddr,
			Interval:      DefaultTrainerInterval,
			UploadTimeout: DefaultTrainerUploadTimeout,
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

	if cfg.Scheduler.Algorithm == "" {
		return errors.New("scheduler requires parameter algorithm")
	}

	if cfg.Scheduler.MaxScheduleCount <= 0 {
		return errors.New("scheduler requires parameter maxScheduleCount")
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

	if cfg.Database.Redis.NetworkTopologyDB < 0 {
		return errors.New("redis requires parameter networkTopologyDB")
	}

	if !slices.Contains([]string{"http", "https"}, cfg.Resource.Task.DownloadTiny.Scheme) {
		return errors.New("downloadTiny requires parameter scheme")
	}

	if cfg.Resource.Task.DownloadTiny.Timeout == 0 {
		return errors.New("downloadTiny requires parameter timeout")
	}

	if cfg.DynConfig.RefreshInterval <= 0 {
		return errors.New("dynconfig requires parameter refreshInterval")
	}

	if cfg.Manager.Addr == "" {
		return errors.New("manager requires parameter addr")
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

	if cfg.Storage.BufferSize <= 0 {
		return errors.New("storage requires parameter bufferSize")
	}

	if cfg.Metrics.Enable {
		if cfg.Metrics.Addr == "" {
			return errors.New("metrics requires parameter addr")
		}
	}

	if cfg.Security.AutoIssueCert {
		if cfg.Security.CACert == "" {
			return errors.New("security requires parameter caCert")
		}

		if !slices.Contains([]string{rpc.DefaultTLSPolicy, rpc.ForceTLSPolicy, rpc.PreferTLSPolicy}, cfg.Security.TLSPolicy) {
			return errors.New("security requires parameter tlsPolicy")
		}

		if len(cfg.Security.CertSpec.IPAddresses) == 0 {
			return errors.New("certSpec requires parameter ipAddresses")
		}

		if len(cfg.Security.CertSpec.DNSNames) == 0 {
			return errors.New("certSpec requires parameter dnsNames")
		}

		if cfg.Security.CertSpec.ValidityPeriod <= 0 {
			return errors.New("certSpec requires parameter validityPeriod")
		}
	}

	if cfg.NetworkTopology.CollectInterval <= 0 {
		return errors.New("networkTopology requires parameter collectInterval")
	}

	if cfg.NetworkTopology.Probe.QueueLength <= 0 {
		return errors.New("probe requires parameter queueLength")
	}

	if cfg.NetworkTopology.Probe.Count <= 0 {
		return errors.New("probe requires parameter count")
	}

	if cfg.NetworkTopology.Cache.Interval <= 0 {
		return errors.New("networkTopology requires parameter interval")
	}

	if cfg.NetworkTopology.Cache.TTL <= 0 {
		return errors.New("networkTopology requires parameter ttl")
	}

	if cfg.Trainer.Enable {
		if cfg.Trainer.Addr == "" {
			return errors.New("trainer requires parameter addr")
		}

		if cfg.Trainer.Interval <= 0 {
			return errors.New("trainer requires parameter interval")
		}

		if cfg.Trainer.UploadTimeout <= 0 {
			return errors.New("trainer requires parameter uploadTimeout")
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

	return nil
}
