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
	"net/url"
	"reflect"
	"testing"
	"time"

	testifyassert "github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"

	"d7y.io/dragonfly/v2/client/util"
	"d7y.io/dragonfly/v2/cmd/dependency/base"
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/unit"
)

func Test_AllUnmarshalYAML(t *testing.T) {
	assert := testifyassert.New(t)
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
	assert := testifyassert.New(t)

	proxyExp, _ := NewRegexp("blobs/sha256.*")
	hijackExp, _ := NewRegexp("mirror.aliyuncs.com:443")

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
		Metrics:     ":8000",
		WorkHome:    "/tmp/dragonfly/dfdaemon/",
		DataDir:     "/var/lib/dragonfly/",
		LogDir:      "/var/log/dragonfly/",
		CacheDir:    "/var/cache/dragonfly/",
		KeepStorage: false,
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
					Type:      model.SeedPeerTypeStrongSeed,
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
			Hostname:       "d7y.io",
			SecurityDomain: "d7y.io",
			Location:       "0.0.0.0",
			IDC:            "d7y",
			NetTopology:    "d7y",
			ListenIP:       "0.0.0.0",
			AdvertiseIP:    "0.0.0.0",
		},
		Download: DownloadOption{
			DefaultPattern: PatternP2P,
			TotalRateLimit: util.RateLimit{
				Limit: 209715200,
			},
			PerPeerRateLimit: util.RateLimit{
				Limit: 20971520,
			},
			PieceDownloadTimeout: 30 * time.Second,
			DownloadGRPC: ListenOption{
				Security: SecurityOption{
					Insecure:  true,
					CACert:    "caCert",
					Cert:      "cert",
					Key:       "key",
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
					CACert:    "caCert",
					Cert:      "cert",
					Key:       "key",
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
				GoroutineCount: 1,
				InitBackoff:    1,
				MaxBackoff:     1,
				MaxAttempts:    1,
			},
		},
		Upload: UploadOption{
			RateLimit: util.RateLimit{
				Limit: 104857600,
			},
			ListenOption: ListenOption{
				Security: SecurityOption{
					Insecure:  true,
					CACert:    "caCert",
					Cert:      "cert",
					Key:       "key",
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
					CACert:    "caCert",
					Cert:      "cert",
					Key:       "key",
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
					CACert:    "caCert",
					Cert:      "cert",
					Key:       "key",
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
			DefaultFilter:  "baz",
			MaxConcurrency: 1,
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
				Cert: "cert",
				Key:  "key",
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
	}

	peerHostOptionYAML := &DaemonOption{}
	if err := peerHostOptionYAML.Load("./testdata/config/daemon.yaml"); err != nil {
		t.Fatal(err)
	}

	assert.EqualValues(peerHostOption, peerHostOptionYAML)
}
