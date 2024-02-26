/*
 *     Copyright 2023 The Dragonfly Authors
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
	"net"
	"time"

	"d7y.io/dragonfly/v2/cmd/dependency/base"
	"d7y.io/dragonfly/v2/pkg/net/ip"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/slices"
	"d7y.io/dragonfly/v2/pkg/types"
)

type Config struct {
	// Base options.
	base.Options `yaml:",inline" mapstructure:",squash"`

	// Network configuration.
	Network NetworkConfig `yaml:"network" mapstructure:"network"`

	// Server configuration.
	Server ServerConfig `yaml:"server" mapstructure:"server"`

	// Metrics configuration.
	Metrics MetricsConfig `yaml:"metrics" mapstructure:"metrics"`

	// Security configuration.
	Security SecurityConfig `yaml:"security" mapstructure:"security"`

	// Manager configuration.
	Manager ManagerConfig `yaml:"manager" mapstructure:"manager"`
}

type NetworkConfig struct {
	// EnableIPv6 enables ipv6 for server.
	EnableIPv6 bool `yaml:"enableIPv6" mapstructure:"enableIPv6"`
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

	// Server log directory.
	LogDir string `yaml:"logDir" mapstructure:"logDir"`

	// Maximum size in megabytes of log files before rotation (default: 1024)
	LogMaxSize int `yaml:"logMaxSize" mapstructure:"logMaxSize"`

	// Maximum number of days to retain old log files (default: 7)
	LogMaxAge int `yaml:"logMaxAge" mapstructure:"logMaxAge"`

	// Maximum number of old log files to keep (default: 20)
	LogMaxBackups int `yaml:"logMaxBackups" mapstructure:"logMaxBackups"`

	// Server storage data directory.
	DataDir string `yaml:"dataDir" mapstructure:"dataDir"`
}

type MetricsConfig struct {
	// Enable metrics service.
	Enable bool `yaml:"enable" mapstructure:"enable"`

	// Metrics service address.
	Addr string `yaml:"addr" mapstructure:"addr"`
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

type ManagerConfig struct {
	// Addr is manager address.
	Addr string `yaml:"addr" mapstructure:"addr"`
}

// New default configuration.
func New() *Config {
	return &Config{
		Network: NetworkConfig{
			EnableIPv6: DefaultNetworkEnableIPv6,
		},
		Server: ServerConfig{
			AdvertisePort: DefaultServerAdvertisePort,
			Port:          DefaultServerPort,
			LogMaxSize:    DefaultLogRotateMaxSize,
			LogMaxAge:     DefaultLogRotateMaxAge,
			LogMaxBackups: DefaultLogRotateMaxBackups,
		},
		Metrics: MetricsConfig{
			Enable: false,
			Addr:   DefaultMetricsAddr,
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
		Manager: ManagerConfig{},
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

	if cfg.Manager.Addr == "" {
		return errors.New("manager requires parameter addr")
	}

	return nil
}

func (cfg *Config) Convert() error {
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
