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
	"fmt"
	"net"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
	"gopkg.in/yaml.v3"

	"d7y.io/dragonfly/v2/client/util"
	"d7y.io/dragonfly/v2/cmd/dependency/base"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/pkg/unit"
)

func Test_AllUnmarshalYAML(t *testing.T) {
	var cases = []struct {
		text   string
		target any
	}{
		{
			text: `
"port": 1234
`,
			target: &struct {
				Port TCPListenPortRange `yaml:"port"`
			}{
				Port: TCPListenPortRange{
					Start: 1234,
				},
			},
		},
		{
			text: `
port:
  start: 1234
  end: 1235
`,
			target: &struct {
				Port TCPListenPortRange `yaml:"port"`
			}{
				Port: TCPListenPortRange{
					Start: 1234,
					End:   1235,
				},
			},
		},
		{
			text: `
timeout: 1000000000
`,
			target: &struct {
				Timeout util.Duration `yaml:"timeout"`
			}{
				Timeout: util.Duration{
					Duration: time.Second,
				},
			},
		},
		{
			text: `
timeout: 1s
`,
			target: &struct {
				Timeout util.Duration `yaml:"timeout"`
			}{
				Timeout: util.Duration{
					Duration: time.Second,
				},
			},
		},
		{
			text: `
limit: 100Mi
`,
			target: &struct {
				Limit util.RateLimit `yaml:"limit"`
			}{
				Limit: util.RateLimit{
					Limit: 100 * 1024 * 1024,
				},
			},
		},
		{
			text: `
limit: 2097152
`,
			target: &struct {
				Limit util.RateLimit `yaml:"limit"`
			}{
				Limit: util.RateLimit{
					Limit: 2 * 1024 * 1024,
				},
			},
		},
		{
			text: `
addr: 127.0.0.1:8002
`,
			target: &struct {
				Addr dfnet.NetAddr `yaml:"addr"`
			}{
				Addr: dfnet.NetAddr{
					Type: dfnet.TCP,
					Addr: "127.0.0.1:8002",
				},
			},
		},
		{
			text: `
listen:
  type: tcp
  addr: 127.0.0.1:8002
`,
			target: &struct {
				Listen dfnet.NetAddr `yaml:"listen"`
			}{
				Listen: dfnet.NetAddr{
					Type: dfnet.TCP,
					Addr: "127.0.0.1:8002",
				},
			},
		},
		{
			text: `
diskGCThreshold: 1Ki
`,
			target: &struct {
				Size unit.Bytes `yaml:"diskGCThreshold"`
			}{
				Size: unit.Bytes(1024),
			},
		},
	}
	for _, c := range cases {
		actual := reflect.New(reflect.TypeOf(c.target).Elem()).Interface()
		err := yaml.Unmarshal([]byte(c.text), actual)

		assert := assert.New(t)
		assert.Nil(err, "yaml.Unmarshal should return nil")
		assert.EqualValues(c.target, actual)
	}
}

func TestUnmarshalYAML(t *testing.T) {
	bytes := []byte(`
tls:
  key: ./testdata/certs/sca.key
  cert: ./testdata/certs/sca.crt
  caCert: ./testdata/certs/ca.crt
url: https://d7y.io
certs: ["./testdata/certs/ca.crt", "./testdata/certs/sca.crt"]
regx: blobs/sha256.*
port1: 1001 
port2:
  start: 1002
  end: 1003
timeout: 3m
limit: 2Mib
type: tcp
proxy1: ./testdata/config/proxy.yaml
proxy2: 
  registryMirror:
    url: https://index.docker.io
schedulers1:
  netAddrs:
    - 0.0.0.0
    - 0.0.0.1
  scheduleTimeout: 0
schedulers2:
  netAddrs:
    - type: tcp
      addr: 0.0.0.0
  scheduleTimeout: 0
`)

	var s = struct {
		TLSConfig   *TLSConfig         `yaml:"tls"`
		URL         *URL               `yaml:"url"`
		Certs       *CertPool          `yaml:"certs"`
		Regx        *Regexp            `yaml:"regx"`
		Port1       TCPListenPortRange `yaml:"port1"`
		Port2       TCPListenPortRange `yaml:"port2"`
		Timeout     util.Duration      `yaml:"timeout"`
		Limit       util.RateLimit     `yaml:"limit"`
		Type        dfnet.NetworkType  `yaml:"type"`
		Proxy1      ProxyOption        `yaml:"proxy1"`
		Proxy2      ProxyOption        `yaml:"proxy2"`
		Schedulers1 SchedulerOption    `yaml:"schedulers1"`
		Schedulers2 SchedulerOption    `yaml:"schedulers2"`
	}{}

	if err := yaml.Unmarshal(bytes, &s); err != nil {
		t.Fatal(err)
	}
}

func TestPeerHostOption_Load(t *testing.T) {
	proxyExp, _ := NewRegexp("blobs/sha256.*")
	hijackExp, _ := NewRegexp("mirror.aliyuncs.com:443")

	_caCert, _ := os.ReadFile("./testdata/certs/ca.crt")
	_cert, _ := os.ReadFile("./testdata/certs/sca.crt")
	_key, _ := os.ReadFile("./testdata/certs/sca.key")

	caCert := types.PEMContent(strings.TrimSpace(string(_caCert)))
	cert := types.PEMContent(strings.TrimSpace(string(_cert)))
	key := types.PEMContent(strings.TrimSpace(string(_key)))

	peerHostOption := &DaemonOption{
		Options: base.Options{
			Console:   true,
			Verbose:   true,
			PProfPort: -1,
			Telemetry: base.TelemetryOption{
				Jaeger:      "foo",
				ServiceName: "bar",
			},
		},
		AliveTime: util.Duration{
			Duration: 0,
		},
		GCInterval: util.Duration{
			Duration: 60000000000,
		},
		Metrics:       ":8000",
		WorkHome:      "/tmp/dragonfly/dfdaemon/",
		WorkHomeMode:  0700,
		CacheDir:      "/var/cache/dragonfly/",
		CacheDirMode:  0700,
		LogDir:        "/var/log/dragonfly/",
		PluginDir:     "/tmp/dragonfly/dfdaemon/plugins/",
		DataDir:       "/var/lib/dragonfly/",
		LogMaxSize:    512,
		LogMaxAge:     5,
		LogMaxBackups: 3,
		DataDirMode:   0700,
		KeepStorage:   false,
		Scheduler: SchedulerOption{
			Manager: ManagerOption{
				Enable: false,
				NetAddrs: []dfnet.NetAddr{
					{
						Type: dfnet.TCP,
						Addr: "127.0.0.1:65003",
					},
				},
				RefreshInterval: 5 * time.Minute,
				SeedPeer: SeedPeerOption{
					Enable:    false,
					Type:      types.HostTypeStrongSeedName,
					ClusterID: 2,
					KeepAlive: KeepAliveOption{
						Interval: 10 * time.Second,
					},
				},
			},
			NetAddrs: []dfnet.NetAddr{
				{
					Type: dfnet.TCP,
					Addr: "127.0.0.1:8002",
				},
			},
			ScheduleTimeout: util.Duration{
				Duration: 0,
			},
			DisableAutoBackSource: true,
		},
		Host: HostOption{
			Hostname:    "d7y.io",
			Location:    "0.0.0.0",
			IDC:         "d7y",
			AdvertiseIP: net.IPv4zero,
		},
		Download: DownloadOption{
			TotalRateLimit: util.RateLimit{
				Limit: 1024 * 1024 * 1024,
			},
			PerPeerRateLimit: util.RateLimit{
				Limit: 512 * 1024 * 1024,
			},
			PieceDownloadTimeout: 30 * time.Second,
			DownloadGRPC: ListenOption{
				Security: SecurityOption{
					Insecure:  true,
					CACert:    caCert,
					Cert:      cert,
					Key:       key,
					TLSVerify: true,
					TLSConfig: nil,
				},
				TCPListen: nil,
				UnixListen: &UnixListenOption{
					Socket: "/tmp/dfdaemon.sock",
				},
			},
			PeerGRPC: ListenOption{
				Security: SecurityOption{
					Insecure:  true,
					CACert:    caCert,
					Cert:      cert,
					Key:       key,
					TLSVerify: true,
					TLSConfig: nil,
				},
				TCPListen: &TCPListenOption{
					Listen: "0.0.0.0",
					PortRange: TCPListenPortRange{
						Start: 65000,
						End:   0,
					},
				},
			},
			CalculateDigest: true,
			Transport: &TransportOption{
				DialTimeout:           time.Second,
				KeepAlive:             time.Second,
				MaxIdleConns:          1,
				IdleConnTimeout:       time.Second,
				ResponseHeaderTimeout: time.Second,
				TLSHandshakeTimeout:   time.Second,
				ExpectContinueTimeout: time.Second,
			},
			GetPiecesMaxRetry: 1,
			Prefetch:          true,
			WatchdogTimeout:   time.Second,
			Concurrent: &ConcurrentOption{
				ThresholdSize: util.Size{
					Limit: 1,
				},
				ThresholdSpeed: unit.Bytes(1),
				GoroutineCount: 1,
				InitBackoff:    1,
				MaxBackoff:     1,
				MaxAttempts:    1,
			},
		},
		Upload: UploadOption{
			RateLimit: util.RateLimit{
				Limit: 1024 * 1024 * 1024,
			},
			ListenOption: ListenOption{
				Security: SecurityOption{
					Insecure:  true,
					CACert:    caCert,
					Cert:      cert,
					Key:       key,
					TLSVerify: true,
				},
				TCPListen: &TCPListenOption{
					Listen: "0.0.0.0",
					PortRange: TCPListenPortRange{
						Start: 65002,
						End:   0,
					},
				},
			},
		},
		ObjectStorage: ObjectStorageOption{
			Enable:      true,
			Filter:      "Expires&Signature&ns",
			MaxReplicas: 3,
			ListenOption: ListenOption{
				Security: SecurityOption{
					Insecure:  true,
					CACert:    caCert,
					Cert:      cert,
					Key:       key,
					TLSVerify: true,
				},
				TCPListen: &TCPListenOption{
					Listen: "0.0.0.0",
					PortRange: TCPListenPortRange{
						Start: 65004,
						End:   0,
					},
				},
			},
		},
		Storage: StorageOption{
			DataPath: "/tmp/storage/data",
			TaskExpireTime: util.Duration{
				Duration: 180000000000,
			},
			StoreStrategy:          StoreStrategy("io.d7y.storage.v2.simple"),
			DiskGCThreshold:        60 * unit.MB,
			DiskGCThresholdPercent: 0.6,
			Multiplex:              true,
		},
		Health: &HealthOption{
			Path: "/health",
		},
		Proxy: &ProxyOption{
			ListenOption: ListenOption{
				Security: SecurityOption{
					Insecure:  true,
					CACert:    caCert,
					Cert:      cert,
					Key:       key,
					TLSVerify: true,
				},
				TCPListen: &TCPListenOption{
					Listen: "0.0.0.0",
					PortRange: TCPListenPortRange{
						Start: 65001,
						End:   0,
					},
				},
			},
			BasicAuth: &BasicAuth{
				Username: "foo",
				Password: "bar",
			},
			DefaultFilter:      "baz",
			DefaultTag:         "tag",
			DefaultApplication: "application",
			MaxConcurrency:     1,
			RegistryMirror: &RegistryMirror{
				Remote: &URL{
					&url.URL{
						Host:   "index.docker.io",
						Scheme: "https",
					},
				},
				DynamicRemote: true,
				UseProxies:    true,
				Insecure:      true,
				Direct:        false,
			},
			WhiteList: []*WhiteList{
				{
					Host: "foo",
					Regx: proxyExp,
					Ports: []string{
						"1000",
						"2000",
					},
				},
			},
			ProxyRules: []*ProxyRule{
				{
					Regx:     proxyExp,
					UseHTTPS: false,
					Direct:   false,
					Redirect: "d7y.io",
				},
			},
			HijackHTTPS: &HijackConfig{
				Cert: "./testdata/certs/sca.crt",
				Key:  "./testdata/certs/sca.key",
				Hosts: []*HijackHost{
					{
						Regx:     hijackExp,
						Insecure: true,
					},
				},
				SNI: nil,
			},
			DumpHTTPContent: true,
			ExtraRegistryMirrors: []*RegistryMirror{
				{
					Remote: &URL{
						&url.URL{
							Host:   "index.docker.io",
							Scheme: "https",
						},
					},
					DynamicRemote: true,
					UseProxies:    true,
					Insecure:      true,
					Direct:        true,
				},
			},
		},
		Reload: ReloadOption{
			Interval: util.Duration{
				Duration: 180000000000,
			},
		},
		Security: GlobalSecurityOption{
			AutoIssueCert: true,
			CACert:        "-----BEGIN CERTIFICATE-----",
			TLSVerify:     true,
			TLSPolicy:     "force",
			CertSpec: &CertSpec{
				DNSNames:       []string{"foo"},
				IPAddresses:    []net.IP{net.IPv4zero},
				ValidityPeriod: 1000000000,
			},
		},
		Network: &NetworkOption{
			EnableIPv6: true,
		},
		Announcer: AnnouncerOption{
			SchedulerInterval: 1000000000,
		},
		NetworkTopology: NetworkTopologyOption{
			Enable: true,
			Probe: ProbeOption{
				Interval: 20 * time.Minute,
			},
		},
	}

	peerHostOptionYAML := &DaemonOption{}
	if err := peerHostOptionYAML.Load("./testdata/config/daemon.yaml"); err != nil {
		t.Fatal(err)
	}

	assert := assert.New(t)
	assert.EqualValues(peerHostOption, peerHostOptionYAML)
}

func TestPeerHostOption_Validate(t *testing.T) {
	tests := []struct {
		name   string
		config *DaemonConfig
		mock   func(cfg *DaemonConfig)
		expect func(t *testing.T, err error)
	}{
		{
			name:   "valid config",
			config: NewDaemonConfig(),
			mock: func(cfg *DaemonConfig) {
				cfg.Scheduler.NetAddrs = []dfnet.NetAddr{
					{
						Type: dfnet.TCP,
						Addr: "127.0.0.1:8002",
					},
				}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name:   "manager addr is not specified",
			config: NewDaemonConfig(),
			mock: func(cfg *DaemonConfig) {
				cfg.Scheduler.Manager.Enable = true
				cfg.Scheduler.Manager.NetAddrs = nil
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "manager addr is not specified")
			},
		},
		{
			name:   "manager refreshInterval not specified",
			config: NewDaemonConfig(),
			mock: func(cfg *DaemonConfig) {
				cfg.Scheduler.Manager.Enable = true
				cfg.Scheduler.Manager.RefreshInterval = 0
				cfg.Scheduler.Manager.NetAddrs = []dfnet.NetAddr{
					{
						Type: dfnet.TCP,
						Addr: "127.0.0.1:8002",
					},
				}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "manager refreshInterval is not specified")
			},
		},
		{
			name:   "empty schedulers and config server is not specified",
			config: NewDaemonConfig(),
			mock: func(cfg *DaemonConfig) {
				cfg.Scheduler.NetAddrs = nil
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "empty schedulers and config server is not specified")
			},
		},
		{
			name:   "download rate limit must be greater",
			config: NewDaemonConfig(),
			mock: func(cfg *DaemonConfig) {
				cfg.Scheduler.NetAddrs = []dfnet.NetAddr{
					{
						Type: dfnet.TCP,
						Addr: "127.0.0.1:8002",
					},
				}
				cfg.Download.TotalRateLimit.Limit = rate.Limit(10 * unit.MB)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				msg := fmt.Sprintf("rate limit must be greater than %s", DefaultMinRate.String())
				assert.EqualError(err, msg)
			},
		},
		{
			name:   "upload rate limit must be greater",
			config: NewDaemonConfig(),
			mock: func(cfg *DaemonConfig) {
				cfg.Scheduler.NetAddrs = []dfnet.NetAddr{
					{
						Type: dfnet.TCP,
						Addr: "127.0.0.1:8002",
					},
				}
				cfg.Upload.RateLimit.Limit = rate.Limit(10 * unit.MB)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				msg := fmt.Sprintf("rate limit must be greater than %s", DefaultMinRate.String())
				assert.EqualError(err, msg)
			},
		},
		{
			name:   "max replicas must be greater than 0",
			config: NewDaemonConfig(),
			mock: func(cfg *DaemonConfig) {
				cfg.Scheduler.NetAddrs = []dfnet.NetAddr{
					{
						Type: dfnet.TCP,
						Addr: "127.0.0.1:8002",
					},
				}
				cfg.ObjectStorage.Enable = true
				cfg.ObjectStorage.MaxReplicas = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "max replicas must be greater than 0")
			},
		},
		{
			name:   "reload interval too short, must great than 1 second",
			config: NewDaemonConfig(),
			mock: func(cfg *DaemonConfig) {
				cfg.Scheduler.NetAddrs = []dfnet.NetAddr{
					{
						Type: dfnet.TCP,
						Addr: "127.0.0.1:8002",
					},
				}
				cfg.Reload.Interval.Duration = time.Millisecond
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "reload interval too short, must great than 1 second")
			},
		},
		{
			name:   "gcInterval must be greater than 0",
			config: NewDaemonConfig(),
			mock: func(cfg *DaemonConfig) {
				cfg.Scheduler.NetAddrs = []dfnet.NetAddr{
					{
						Type: dfnet.TCP,
						Addr: "127.0.0.1:8002",
					},
				}
				cfg.GCInterval.Duration = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "gcInterval must be greater than 0")
			},
		},
		{
			name:   "security requires parameter caCert",
			config: NewDaemonConfig(),
			mock: func(cfg *DaemonConfig) {
				cfg.Scheduler.NetAddrs = []dfnet.NetAddr{
					{
						Type: dfnet.TCP,
						Addr: "127.0.0.1:8002",
					},
				}
				cfg.Security.AutoIssueCert = true
				cfg.Security.CACert = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "security requires parameter caCert")
			},
		},
		{
			name:   "certSpec requires parameter ipAddresses",
			config: NewDaemonConfig(),
			mock: func(cfg *DaemonConfig) {
				cfg.Scheduler.NetAddrs = []dfnet.NetAddr{
					{
						Type: dfnet.TCP,
						Addr: "127.0.0.1:8002",
					},
				}
				cfg.Security.AutoIssueCert = true
				cfg.Security.CACert = "test"
				cfg.Security.CertSpec.IPAddresses = nil
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "certSpec requires parameter ipAddresses")
			},
		},
		{
			name:   "certSpec requires parameter dnsNames",
			config: NewDaemonConfig(),
			mock: func(cfg *DaemonConfig) {
				cfg.Scheduler.NetAddrs = []dfnet.NetAddr{
					{
						Type: dfnet.TCP,
						Addr: "127.0.0.1:8002",
					},
				}
				cfg.Security.AutoIssueCert = true
				cfg.Security.CACert = "test"
				cfg.Security.CertSpec.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
				cfg.Security.CertSpec.DNSNames = nil
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "certSpec requires parameter dnsNames")
			},
		},
		{
			name:   "certSpec requires parameter validityPeriod",
			config: NewDaemonConfig(),
			mock: func(cfg *DaemonConfig) {
				cfg.Scheduler.NetAddrs = []dfnet.NetAddr{
					{
						Type: dfnet.TCP,
						Addr: "127.0.0.1:8002",
					},
				}
				cfg.Security.AutoIssueCert = true
				cfg.Security.CACert = "testcert"
				cfg.Security.CertSpec.ValidityPeriod = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "certSpec requires parameter validityPeriod")
			},
		},
		{
			name:   "probe requires parameter interval",
			config: NewDaemonConfig(),
			mock: func(cfg *DaemonConfig) {
				cfg.Scheduler.NetAddrs = []dfnet.NetAddr{
					{
						Type: dfnet.TCP,
						Addr: "127.0.0.1:8002",
					},
				}
				cfg.NetworkTopology.Enable = true
				cfg.NetworkTopology.Probe.Interval = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "probe requires parameter interval")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.mock(tc.config)
			tc.expect(t, tc.config.Validate())
		})
	}
}
