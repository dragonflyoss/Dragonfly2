package trainer

import (
	"errors"
	"net"
	"time"

	"d7y.io/dragonfly/v2/pkg/net/ip"
)

type Config struct {
	// Metrics configuration
	Metrics MetricsConfig `yaml:"metrics" mapstructure:"metrics"`
	// Network configuration.
	Network NetworkConfig `yaml:"network" mapstructure:"network"`
	// Server configuration.
	Server ServerConfig `yaml:"server" mapstructure:"server"`
	// Trainer configuration
	Trainer TrainerConfig `yaml:"trainer" mapstructure:"trainer"`
}

type NetworkConfig struct {
	// EnableIPv6 enables ipv6 for server.
	EnableIPv6 bool `yaml:"enableIPv6" mapstructure:"enableIPv6"`
}

type ServerConfig struct {
	// GRPC server configuration.
	GRPC GRPCConfig `yaml:"grpc" mapstructure:"grpc"`
	// KeepAlive configuration.
	KeepAlive KeepAliveConfig `yaml:"keepAlive" mapstructure:"keepAlive"`
	// ObjectStorage configuration.
	ObjectStorage ObjectStorageConfig `yaml:"objectStorage" mapstructure:"objectStorage"`
}

type ObjectStorageConfig struct {
	// Name is object storage name of type, it can be s3, oss or obs.
	Name string `yaml:"name" mapstructure:"name"`

	// Region is storage region.
	Region string `yaml:"region" mapstructure:"region"`

	// Endpoint is datacenter endpoint.
	Endpoint string `yaml:"endpoint" mapstructure:"endpoint"`

	// AccessKey is access key ID.
	AccessKey string `yaml:"accessKey" mapstructure:"accessKey"`

	// SecretKey is access key secret.
	SecretKey string `yaml:"secretKey" mapstructure:"secretKey"`
}

type GRPCConfig struct {
	// AdvertiseIP is advertise ip.
	AdvertiseIP net.IP `yaml:"advertiseIP" mapstructure:"advertiseIP"`
	// ListenIP is listen ip, like: 0.0.0.0, 192.168.0.1.
	ListenIP net.IP `yaml:"listenIP" mapstructure:"listenIP"`
	// Port is listen port.
	PortRange TCPListenPortRange `yaml:"port" mapstructure:"port"`
}

type TCPListenPortRange struct {
	Start int
	End   int
}

type KeepAliveConfig struct {
	// Keep alive interval.
	Interval time.Duration `yaml:"interval" mapstructure:"interval"`
}

type RedisConfig struct {
	// Addrs is server addresses.
	Addrs []string `yaml:"addrs" mapstructure:"addrs"`
	// MasterName is the sentinel master name.
	MasterName string `yaml:"masterName" mapstructure:"masterName"`
	// Username is server username.
	Username string `yaml:"username" mapstructure:"username"`
	// Password is server password.
	Password string `yaml:"password" mapstructure:"password"`
	// TrainerDB is trainer database name. default:3
	TrainerDB int `yaml:"trainerDB" mapstructure:"trainerDB"`
}

type TrainerConfig struct {
	// MaxBackups is the maximum number of model version to retain in redis.
	MaxBackups int `yaml:"maxBackups" mapstructure:"maxBackups"`
	// DataPath is the path of data for training
	DataPath string `yaml:"dataPath" mapstructure:"dataPath"`
}

type MetricsConfig struct {
	// Enable metrics service.
	Enable bool `yaml:"enable" mapstructure:"enable"`
	// Metrics service address.
	Addr string `yaml:"addr" mapstructure:"addr"`
}

// New default configuration
func New() *Config {
	return &Config{
		Server: ServerConfig{
			GRPC: GRPCConfig{
				PortRange: TCPListenPortRange{
					Start: DefaultGRPCPort,
					End:   DefaultGRPCPort,
				},
			},
			KeepAlive: KeepAliveConfig{
				Interval: DefaultTrainerKeepAliveInterval,
			},
		},
		Metrics: MetricsConfig{
			Enable: false,
			Addr:   DefaultMetricsAddr,
		},
		Trainer: TrainerConfig{
			MaxBackups: DefaultMaxBackups,
			DataPath:   DefaultSchemaPath,
		},
		Network: NetworkConfig{
			EnableIPv6: DefaultNetworkEnableIPv6,
		},
	}
}

func (cfg *Config) Validate() error {
	if cfg.Server.GRPC.AdvertiseIP == nil {
		return errors.New("grpc requires parameter advertiseIP")
	}

	if cfg.Server.GRPC.ListenIP == nil {
		return errors.New("grpc requires parameter listenIP")
	}

	if cfg.Trainer.MaxBackups < 1 {
		return errors.New("training requires parameter ")
	}

	if cfg.Trainer.DataPath == "" {
		return errors.New("training requires parameter Backups")
	}

	return nil
}

func (cfg *Config) Convert() error {
	if cfg.Server.GRPC.AdvertiseIP == nil {
		if cfg.Network.EnableIPv6 {
			cfg.Server.GRPC.AdvertiseIP = ip.IPv6
		} else {
			cfg.Server.GRPC.AdvertiseIP = ip.IPv4
		}
	}

	if cfg.Server.GRPC.ListenIP == nil {
		if cfg.Network.EnableIPv6 {
			cfg.Server.GRPC.ListenIP = net.IPv6zero
		} else {
			cfg.Server.GRPC.ListenIP = net.IPv4zero
		}
	}

	return nil
}
