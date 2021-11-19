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

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/unit"
)

func Test_AllUnmarshalYAML(t *testing.T) {
	assert := testifyassert.New(t)
	var cases = []struct {
		text   string
		target interface{}
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
				Timeout clientutil.Duration `yaml:"timeout"`
			}{
				Timeout: clientutil.Duration{
					Duration: time.Second,
				},
			},
		},
		{
			text: `
timeout: 1s
`,
			target: &struct {
				Timeout clientutil.Duration `yaml:"timeout"`
			}{
				Timeout: clientutil.Duration{
					Duration: time.Second,
				},
			},
		},
		{
			text: `
limit: 100Mi
`,
			target: &struct {
				Limit clientutil.RateLimit `yaml:"limit"`
			}{
				Limit: clientutil.RateLimit{
					Limit: 100 * 1024 * 1024,
				},
			},
		},
		{
			text: `
limit: 2097152
`,
			target: &struct {
				Limit clientutil.RateLimit `yaml:"limit"`
			}{
				Limit: clientutil.RateLimit{
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
		TLSConfig   *TLSConfig           `yaml:"tls"`
		URL         *URL                 `yaml:"url"`
		Certs       *CertPool            `yaml:"certs"`
		Regx        *Regexp              `yaml:"regx"`
		Port1       TCPListenPortRange   `yaml:"port1"`
		Port2       TCPListenPortRange   `yaml:"port2"`
		Timeout     clientutil.Duration  `yaml:"timeout"`
		Limit       clientutil.RateLimit `yaml:"limit"`
		Type        dfnet.NetworkType    `yaml:"type"`
		Proxy1      ProxyOption          `yaml:"proxy1"`
		Proxy2      ProxyOption          `yaml:"proxy2"`
		Schedulers1 SchedulerOption      `yaml:"schedulers1"`
		Schedulers2 SchedulerOption      `yaml:"schedulers2"`
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
		AliveTime: clientutil.Duration{
			Duration: 0,
		},
		GCInterval: clientutil.Duration{
			Duration: 60000000000,
		},
		DataDir:     "/tmp/dragonfly/dfdaemon/",
		WorkHome:    "/tmp/dragonfly/dfdaemon/",
		KeepStorage: false,
		Scheduler: SchedulerOption{
			Manager: ManagerOption{
				Enable:          false,
				Addr:            "127.0.0.1:65003",
				RefreshInterval: 5 * time.Minute,
			},
			NetAddrs: []dfnet.NetAddr{
				{
					Type: dfnet.TCP,
					Addr: "127.0.0.1:8002",
				},
			},
			ScheduleTimeout: clientutil.Duration{
				Duration: 0,
			},
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
			PieceDownloadTimeout: 30 * time.Second,
			TotalRateLimit: clientutil.RateLimit{
				Limit: 209715200,
			},
			PerPeerRateLimit: clientutil.RateLimit{
				Limit: 20971520,
			},
			DownloadGRPC: ListenOption{
				Security: SecurityOption{
					Insecure: true,
					CACert:   "caCert",
					Cert:     "cert",
					Key:      "key",
				},
				UnixListen: &UnixListenOption{
					Socket: "/tmp/dfdaemon.sock",
				},
			},
			PeerGRPC: ListenOption{
				Security: SecurityOption{
					Insecure: true,
					CACert:   "caCert",
					Cert:     "cert",
					Key:      "key",
				},
				TCPListen: &TCPListenOption{
					Listen: "0.0.0.0",
					PortRange: TCPListenPortRange{
						Start: 65000,
						End:   0,
					},
				},
			},
		},
		Upload: UploadOption{
			RateLimit: clientutil.RateLimit{
				Limit: 104857600,
			},
			ListenOption: ListenOption{
				Security: SecurityOption{
					Insecure: true,
					CACert:   "caCert",
					Cert:     "cert",
					Key:      "key",
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
		Storage: StorageOption{
			DataPath: "/tmp/storage/data",
			TaskExpireTime: clientutil.Duration{
				Duration: 180000000000,
			},
			StoreStrategy: StoreStrategy("io.d7y.storage.v2.simple"),
		},
		Proxy: &ProxyOption{
			ListenOption: ListenOption{
				Security: SecurityOption{
					Insecure: true,
					CACert:   "caCert",
					Cert:     "cert",
					Key:      "key",
				},
				TCPListen: &TCPListenOption{
					Listen: "0.0.0.0",
					PortRange: TCPListenPortRange{
						Start: 65001,
						End:   0,
					},
				},
			},
			RegistryMirror: &RegistryMirror{
				Remote: &URL{
					&url.URL{
						Host:   "index.docker.io",
						Scheme: "https",
					},
				},
				Insecure: true,
				Direct:   false,
			},
			Proxies: []*Proxy{
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
			},
		},
	}

	peerHostOptionYAML := &DaemonOption{}
	if err := peerHostOptionYAML.Load("./testdata/config/daemon.yaml"); err != nil {
		t.Fatal(err)
	}

	assert.EqualValues(peerHostOption, peerHostOptionYAML)
}

func Test_AttributeGetter(t *testing.T) {
	assert := testifyassert.New(t)
	testCases := []struct {
		name    string
		config  string
		value   string
		isError bool
	}{
		{
			name: "string attribute ok",
			config: `
attr: "a1"
`,
			value: "a1",
		},
		{
			name: "command attribute ok - 1",
			config: `
attr:
  command: |-
    echo a2
  regx: a[\d]+
`,
			value: "a2",
		},
		{
			name: "command attribute ok - 2",
			config: `
attr:
  command: |-
    attr=a2
    if [ "$attr" = "" ]; then
      exit 1
    fi
    printf $attr
  regx: a[\d]+
`,
			value: "a2",
		},
		{
			name: "command attribute ok - 3",
			config: `
attr:
  command: |-
    echo a2 | grep 3
  regx: a[\d]+
  retry: 3
  default: "3"
`,
			value:   "3",
			isError: true,
		},
		{
			name: "command attribute exec error",
			config: `
attr:
  command: |-
    exit 1
  regx: a[\d]+
  retry: 1
`,
			isError: true,
		},
		{
			name: "command attribute regx error",
			config: `
attr:
  command: |-
    echo aa
  regx: a[\d]+
  retry: 1
`,
			isError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := struct {
				Attr Attribute `yaml:"attr"`
			}{}
			err := yaml.Unmarshal([]byte(tc.config), &data)
			if tc.isError {
				assert.NotNil(err)
			} else {
				assert.Equal(string(data.Attr), tc.value)
				assert.Nil(err)
			}
		})
	}
}
