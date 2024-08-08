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
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"

	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/types"
)

var (
	mockManagerConfig = ManagerConfig{
		Addr: "localhost",
	}

	mockMetricsConfig = MetricsConfig{
		Enable: true,
		Addr:   DefaultMetricsAddr,
	}

	mockSecurityConfig = SecurityConfig{
		AutoIssueCert: true,
		CACert:        types.PEMContent("foo"),
		TLSPolicy:     rpc.PreferTLSPolicy,
		CertSpec: CertSpec{
			DNSNames:       DefaultCertDNSNames,
			IPAddresses:    DefaultCertIPAddresses,
			ValidityPeriod: DefaultCertValidityPeriod,
		},
	}
)

func TestConfig_Load(t *testing.T) {
	config := &Config{
		Network: NetworkConfig{
			EnableIPv6: true,
		},
		Server: ServerConfig{
			AdvertiseIP:   net.ParseIP("127.0.0.1"),
			AdvertisePort: 9090,
			ListenIP:      net.ParseIP("0.0.0.0"),
			Port:          9090,
			LogDir:        "foo",
			LogMaxSize:    512,
			LogMaxAge:     5,
			LogMaxBackups: 3,
			DataDir:       "foo",
		},
		Metrics: MetricsConfig{
			Enable: false,
			Addr:   ":8000",
		},
		Security: SecurityConfig{
			AutoIssueCert: true,
			CACert:        "foo",
			TLSVerify:     true,
			TLSPolicy:     "force",
			CertSpec: CertSpec{
				DNSNames:       []string{"foo"},
				IPAddresses:    []net.IP{net.IPv4zero},
				ValidityPeriod: 10 * time.Minute,
			},
		},
		Manager: ManagerConfig{
			Addr: "127.0.0.1:65003",
		},
	}

	trainerConfigYAML := &Config{}
	contentYAML, _ := os.ReadFile("./testdata/trainer.yaml")
	if err := yaml.Unmarshal(contentYAML, &trainerConfigYAML); err != nil {
		t.Fatal(err)
	}
	assert := assert.New(t)
	assert.EqualValues(config, trainerConfigYAML)
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name   string
		config *Config
		mock   func(cfg *Config)
		expect func(t *testing.T, err error)
	}{
		{
			name:   "valid config",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name:   "server requires parameter advertiseIP",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Server.AdvertiseIP = nil
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "server requires parameter advertiseIP")
			},
		},
		{
			name:   "server requires parameter advertisePort",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Server.AdvertisePort = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "server requires parameter advertisePort")
			},
		},
		{
			name:   "server requires parameter listenIP",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Server.ListenIP = nil
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "server requires parameter listenIP")
			},
		},
		{
			name:   "server requires parameter port",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Server.Port = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "server requires parameter port")
			},
		},
		{
			name:   "metrics requires parameter addr",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Metrics = mockMetricsConfig
				cfg.Metrics.Addr = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "metrics requires parameter addr")
			},
		},
		{
			name:   "security requires parameter caCert",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Security = mockSecurityConfig
				cfg.Security.CACert = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "security requires parameter caCert")
			},
		},
		{
			name:   "security requires parameter tlsPolicy",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Security = mockSecurityConfig
				cfg.Security.TLSPolicy = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "security requires parameter tlsPolicy")
			},
		},
		{
			name:   "certSpec requires parameter ipAddresses",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Security = mockSecurityConfig
				cfg.Security.CertSpec.IPAddresses = []net.IP{}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "certSpec requires parameter ipAddresses")
			},
		},
		{
			name:   "certSpec requires parameter dnsNames",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Security = mockSecurityConfig
				cfg.Security.CertSpec.DNSNames = []string{}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "certSpec requires parameter dnsNames")
			},
		},
		{
			name:   "certSpec requires parameter validityPeriod",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Security = mockSecurityConfig
				cfg.Security.CertSpec.ValidityPeriod = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "certSpec requires parameter validityPeriod")
			},
		},
		{
			name:   "manager requires parameter addr",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Manager.Addr = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "manager requires parameter addr")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.config.Convert(); err != nil {
				t.Fatal(err)
			}

			tc.mock(tc.config)
			tc.expect(t, tc.config.Validate())
		})
	}
}
