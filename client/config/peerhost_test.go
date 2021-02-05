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
	"encoding/json"
	"net/url"
	"testing"

	"gopkg.in/yaml.v3"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	testifyassert "github.com/stretchr/testify/assert"
)

func TestUnmarshalJSON(t *testing.T) {
	bytes := []byte(`{
		"tls": {
			"key": "../daemon/test/testdata/certs/sca.key",
			"cert": "../daemon/test/testdata/certs/sca.crt",
			"ca_cert": "../daemon/test/testdata/certs/mca.crt"
		},
		"url": "https://d7y.io",
    "certs": [
			"../daemon/test/testdata/certs/ca.crt",
			"../daemon/test/testdata/certs/mca.crt",
			"../daemon/test/testdata/certs/sca.crt"
    ],
		"regx": "blobs/sha256.*",
		"port1": 1001,
		"port2": {
			"start": 1002,
			"end": 1003
		},
		"timeout": "3m",
		"limit": "2Mib",
		"type": "tcp",
		"proxy1": "../daemon/test/testdata/config/proxy.json",
		"proxy2": {
			"registry_mirror": {
				"url": "https://index.docker.io"
			}
		},
		"schedulers1": [ "0.0.0.0", "0.0.0.1" ],
		"schedulers2": [{
			"type": "tcp",
			"addr": "0.0.0.0"
		}]
}`)

	var s = struct {
		TLSConfig   *TLSConfig           `json:"tls"`
		URL         *URL                 `json:"url"`
		Certs       *CertPool            `json:"certs"`
		Regx        *Regexp              `json:"regx"`
		Port1       TCPListenPortRange   `json:"port1"`
		Port2       TCPListenPortRange   `json:"port2"`
		Timeout     clientutil.Duration  `json:"timeout"`
		Limit       clientutil.RateLimit `json:"limit"`
		Type        dfnet.NetworkType    `json:"type"`
		Proxy1      ProxyOption          `json:"proxy1"`
		Proxy2      ProxyOption          `json:"proxy2"`
		Schedulers1 []dfnet.NetAddr      `json:"schedulers1" yaml:"schedulers1"`
		Schedulers2 []dfnet.NetAddr      `json:"schedulers2" yaml:"schedulers2"`
	}{}

	if err := json.Unmarshal(bytes, &s); err != nil {
		t.Fatal(err)
	}

	t.Logf("%#v\n", s)
}

func TestUnmarshalYAML(t *testing.T) {
	bytes := []byte(`
tls:
  key: ../daemon/test/testdata/certs/sca.key
  cert: ../daemon/test/testdata/certs/sca.crt
  ca_cert: ../daemon/test/testdata/certs/mca.crt
url: https://d7y.io
certs: ["../daemon/test/testdata/certs/ca.crt", "../daemon/test/testdata/certs/mca.crt", "../daemon/test/testdata/certs/sca.crt"]
regx: blobs/sha256.*
port1: 1001 
port2:
  start: 1002
  end: 1003
timeout: 3m
limit: 2Mib
type: tcp
proxy1: ../daemon/test/testdata/config/proxy.yml
proxy2: 
  registry_mirror:
    url: https://index.docker.io
schedulers1:
- 0.0.0.0
- 0.0.0.1
schedulers2:
- type: tcp
  addr: 0.0.0.0
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
		Schedulers1 []dfnet.NetAddr      `json:"schedulers1" yaml:"schedulers1"`
		Schedulers2 []dfnet.NetAddr      `json:"schedulers2" yaml:"schedulers2"`
	}{}

	if err := yaml.Unmarshal(bytes, &s); err != nil {
		t.Fatal(err)
	}

	t.Logf("%#v\n", s)
}

func TestPeerHostOption_Load(t *testing.T) {
	assert := testifyassert.New(t)
	proxyExp, _ := NewRegexp("blobs/sha256.*")
	hijackExp, _ := NewRegexp("mirror.aliyuncs.com:443")
	p1 := &PeerHostOption{
		AliveTime: clientutil.Duration{
			Duration: 0,
		},
		GCInterval: clientutil.Duration{
			Duration: 60000000000,
		},
		PidFile:     "/tmp/dfdaemon.pid",
		LockFile:    "/tmp/dfdaemon.lock",
		DataDir:     "/tmp/dragonfly/dfdaemon/",
		WorkHome:    "/tmp/dragonfly/dfdaemon/",
		KeepStorage: false,
		Verbose:     true,
		Schedulers: []dfnet.NetAddr{
			{
				Type: dfnet.TCP,
				Addr: "127.0.0.1:8002",
			},
		},
		Host: HostOption{
			SecurityDomain: "d7y.io",
			Location:       "0.0.0.0",
			IDC:            "d7y",
			NetTopology:    "d7y",
			ListenIP:       "0.0.0.0",
			AdvertiseIP:    "0.0.0.0",
		},
		Download: DownloadOption{
			RateLimit: clientutil.RateLimit{
				Limit: 209715200,
			},
			DownloadGRPC: ListenOption{
				Security: SecurityOption{
					Insecure: true,
					CACert: `-----BEGIN CERTIFICATE-----
MIIDmDCCAoACCQCrkZtbv5nPpjANBgkqhkiG9w0BAQsFADCBjDELMAkGA1UEBhMC
Q04xETAPBgNVBAgMCFpoZUppYW5nMREwDwYDVQQHDAhIYW5nWmhvdTEQMA4GA1UE
CgwHQWxpYmFiYTERMA8GA1UECwwIQW50R3JvdXAxDzANBgNVBAMMBmQ3eS5pbzEh
MB8GCSqGSIb3DQEJARYSZ2FpdXMucWlAZ21haWwuY29tMCAXDTIxMDIwMjA4NTIx
NloYDzIxMjEwMTA5MDg1MjE2WjCBjDELMAkGA1UEBhMCQ04xETAPBgNVBAgMCFpo
ZUppYW5nMREwDwYDVQQHDAhIYW5nWmhvdTEQMA4GA1UECgwHQWxpYmFiYTERMA8G
A1UECwwIQW50R3JvdXAxDzANBgNVBAMMBmQ3eS5pbzEhMB8GCSqGSIb3DQEJARYS
Z2FpdXMucWlAZ21haWwuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKC
AQEAmrxd4DOYtV2iG73rh/281z9XLxBcVznhkxkL9y3dnPUBNRPk3ChsU14UUBzQ
QtKiIJNWtvG1N8COybl6PSnnyv29XAlJChp1/TBLIcrOZNLeQwxvrgGnXfonbIiy
ez6uq555qm3Yn4PPkZUIyLEjd6tKvRpvz4JgkaQ7VclHUK2c5DJ2HEH4LoZT7/XE
0UQOim1ayGBYBmWgXKIBWgVlqptRwKPIfN4nIqFcSdTSNvIOdnqK8XmG6okIkuUg
269QcP1cYgGL/GraKwIQ4SadGWUphIp6GEFLSqkz2aIjKg31cf7DGbitN4sWS1FS
jItk5oJMjAz+muvx8HuEVVaoCwIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQAr9EhE
NpSGWKoVa2jv5uNstDCONmqSVKg1HV31/z9lMJFsCNp3kb5QhwSickV89JsEulcD
JAd/q5ur/+r2Zru+j8nlB/90CB92YtyEo+acLj8CcigALdMWffU+NIoFzeMmMGDV
iHMPZrBnpMlLlTd6Gt7+hqWK4uIDYl1tmBLr4ORZnectUwjN5awgZn2so/xr/d+I
InxjC172HWF6rbtZDaw4TRV8hDOfsyNLGZe5COe/zBVckDjdZw7o9MXwyF5kGePO
nC0C/n4PJjw8Ks5bhhM82MksjCK5jERaIaRfMIoV/DDtrIktrDLUBuXTx4e/d6Ly
HFnJAr2dMPFGo9To
-----END CERTIFICATE-----
`,
					Cert: `-----BEGIN CERTIFICATE-----
MIIDljCCAn4CCQDcMhq563xx9jANBgkqhkiG9w0BAQUFADCBjDELMAkGA1UEBhMC
Q04xETAPBgNVBAgMCFpoZUppYW5nMREwDwYDVQQHDAhIYW5nWmhvdTEQMA4GA1UE
CgwHQWxpYmFiYTERMA8GA1UECwwIQW50R3JvdXAxDzANBgNVBAMMBmQ3eS5pbzEh
MB8GCSqGSIb3DQEJARYSZ2FpdXMucWlAZ21haWwuY29tMB4XDTIxMDIwMjA4NTkw
MVoXDTMxMDEzMTA4NTkwMVowgYwxCzAJBgNVBAYTAkNOMREwDwYDVQQIDAhaaGVK
aWFuZzERMA8GA1UEBwwISGFuZ1pob3UxEDAOBgNVBAoMB0FsaWJhYmExETAPBgNV
BAsMCEFudEdyb3VwMQ8wDQYDVQQDDAZkN3kuaW8xITAfBgkqhkiG9w0BCQEWEmdh
aXVzLnFpQGdtYWlsLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
AJjKlMYlhnPTWBAvSwCxcC586co1NnQQ8uGfCVng66ah1nrP+41B0E2MsUcgv7GP
6wW87+g9/SVYbLp7ZFD716PXdUHCUP0JiRbRhbiTsK4BujPdm5R/gnzXWDpW4mV4
tyOArxPuCmhh1j6aorGzKnFunFua3M8/QozZzoHyo/EBWYEiUS069XTo4brSZFso
vGh8cSOdrth3w7+hve4//8HchS7j4TtoQPuuh2rJON4xooD02GjdR+exdgLSUj2P
hGLPNXINF/ziVvk7ZdBeVg2TkWyib6gY3M7n7P3QEfquhhdlpGWehstUqRJL2I+q
zz6jHgcJjcwzkN+0CgEHfYUCAwEAATANBgkqhkiG9w0BAQUFAAOCAQEAhYjX50mb
3mU8oapLM6C26H00cLT9V0IFyQtz2wsbB+jW45V7N+/UQ1PWXM49/Ins7694no6x
SA+82jiJlMaduMYxZCW/nZn7CrQ6HqnxwQzZwsQEHks7j6L3qGN8icEwp2OWBTEI
48XujDc86Q1TAKhWP25MypWb1HrR56Kp7pdEEOIP0ytwzQjup8cyTUCs/zyqva9s
kqfppdvFAZ93sm1XGnIvxPP08NUDZ+Fo2Dcs1+LaEb5p5xs8ASWmpFOsXSacBXiz
HY65P3PBCOpb15k48xOdrOD1PnHw2c+SfNQXe3icAeU7A5zAsLkRjgWDhP7IgSWl
4c5v7Jrmk2BNGw==
-----END CERTIFICATE-----
`,
					Key: `-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEAmMqUxiWGc9NYEC9LALFwLnzpyjU2dBDy4Z8JWeDrpqHWes/7
jUHQTYyxRyC/sY/rBbzv6D39JVhsuntkUPvXo9d1QcJQ/QmJFtGFuJOwrgG6M92b
lH+CfNdYOlbiZXi3I4CvE+4KaGHWPpqisbMqcW6cW5rczz9CjNnOgfKj8QFZgSJR
LTr1dOjhutJkWyi8aHxxI52u2HfDv6G97j//wdyFLuPhO2hA+66Hask43jGigPTY
aN1H57F2AtJSPY+EYs81cg0X/OJW+Ttl0F5WDZORbKJvqBjczufs/dAR+q6GF2Wk
ZZ6Gy1SpEkvYj6rPPqMeBwmNzDOQ37QKAQd9hQIDAQABAoIBAFjZRNZMr/jep1ES
D01h4VhHLzR06StpR7PH5YosbxxA9BYKp78mzFisPdKcypwYkpSNn/yvP2veFawD
YPxu1qDiA7+vnaTnTJ7GTDpfN9iYDI1oirY5x8mM+DNEnvZe0jCE/kpanBbC/fD9
vyoSg9XeenISheDGao65gYqzbH4SYsxOCASrczMwS/41BwNmQwFwH4ldma1NZimm
2vMv5j2Xur14tcJPRLxb29bdNJRMaOjglF1ZHHFRjqfQSXkrTc0R5zuOvHiJeb25
e+1RjKrKbRb49iZE23lJmCWUoff694mPdPi2J+UyxTFh8kFd1fe45K79e2Zy3nzz
h4Tbu2kCgYEAyPGnCo0WAwBMkpU2/165YwBkY32bA16LF+6OluRuJQcDPNlx1xro
mOZLkijJBOcV9dZm6fsjbwntxtmmPauFdw9sIxsG5o2bpmh5Xsnpu8lLSaEjysIB
5WKQ7aPTcagG9CK4WRpLkLPEMynoxo6mB/72kBVfld241S5qxiCqqh8CgYEAwqd4
c9DEtuvxGgW4KeIwMIbMXlrO6XjGesk8SSmWWwWq1hOC3XZ85TpQl2JRYTeMVJ6P
giGYMcLr8s8qika+1mpLmVmM1h+N7rvO5nsSsJ60YKqvxIQaXgKkIWMrxaCOgQmu
qTBaVTpo4QkpBWFAYSkjWwpUQHrbl3ypd/Jza9sCgYAPf8wxnSZIfvppCAdg9S55
e2tC3UpanS0/YFAxRVdVlc/jHqaQP+wW9xR3Jpwyu5xPBQWVIKDgDLUBdIJFGXjG
8TKXFpuWpu+Ni1tpO0vDB0i+WiaHmiVJSywAmHVTu/ElXZQ4kzWm6KbGh4ID5rbZ
wQnFnVBtH9gE4Xqs45ImYwKBgFf3eO7V7OBBli7NYOHEr0Ru1pLZdOJ7yy6YHJ9v
pNwznnWqUZylvGGXTe5r5x0JDmj7Ux1a8z2huiF7z9y3hey+ErYVixkFH45A34q3
GcYpopiA6nfjv0q25NeVyqVAHsZfysf46wnTIKx6CEi/H2oJPkoZS+Pr4ar/ElL7
hX+7AoGASE8vPTgNRhV/mYQO+SqqzqS42j/VxEFL9BK2TSOydeb0OljMlRCNSggY
xrHgCZQ1I6d7DIdFJC9OCSOA1342GDBSq7BtC11GCzfzl5gMGxiyXH1LtiXbgbDX
ObnPyoVaE0NngeUzY+6nWfRvHIarIIeYdC7cTN2frR6nZ856EPg=
-----END RSA PRIVATE KEY-----
`,
				},
				UnixListen: &UnixListenOption{
					Socket: "/tmp/dfdamon.sock",
				},
			},
			PeerGRPC: ListenOption{
				Security: SecurityOption{
					Insecure: true,
					CACert: `-----BEGIN CERTIFICATE-----
MIIDmDCCAoACCQCrkZtbv5nPpjANBgkqhkiG9w0BAQsFADCBjDELMAkGA1UEBhMC
Q04xETAPBgNVBAgMCFpoZUppYW5nMREwDwYDVQQHDAhIYW5nWmhvdTEQMA4GA1UE
CgwHQWxpYmFiYTERMA8GA1UECwwIQW50R3JvdXAxDzANBgNVBAMMBmQ3eS5pbzEh
MB8GCSqGSIb3DQEJARYSZ2FpdXMucWlAZ21haWwuY29tMCAXDTIxMDIwMjA4NTIx
NloYDzIxMjEwMTA5MDg1MjE2WjCBjDELMAkGA1UEBhMCQ04xETAPBgNVBAgMCFpo
ZUppYW5nMREwDwYDVQQHDAhIYW5nWmhvdTEQMA4GA1UECgwHQWxpYmFiYTERMA8G
A1UECwwIQW50R3JvdXAxDzANBgNVBAMMBmQ3eS5pbzEhMB8GCSqGSIb3DQEJARYS
Z2FpdXMucWlAZ21haWwuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKC
AQEAmrxd4DOYtV2iG73rh/281z9XLxBcVznhkxkL9y3dnPUBNRPk3ChsU14UUBzQ
QtKiIJNWtvG1N8COybl6PSnnyv29XAlJChp1/TBLIcrOZNLeQwxvrgGnXfonbIiy
ez6uq555qm3Yn4PPkZUIyLEjd6tKvRpvz4JgkaQ7VclHUK2c5DJ2HEH4LoZT7/XE
0UQOim1ayGBYBmWgXKIBWgVlqptRwKPIfN4nIqFcSdTSNvIOdnqK8XmG6okIkuUg
269QcP1cYgGL/GraKwIQ4SadGWUphIp6GEFLSqkz2aIjKg31cf7DGbitN4sWS1FS
jItk5oJMjAz+muvx8HuEVVaoCwIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQAr9EhE
NpSGWKoVa2jv5uNstDCONmqSVKg1HV31/z9lMJFsCNp3kb5QhwSickV89JsEulcD
JAd/q5ur/+r2Zru+j8nlB/90CB92YtyEo+acLj8CcigALdMWffU+NIoFzeMmMGDV
iHMPZrBnpMlLlTd6Gt7+hqWK4uIDYl1tmBLr4ORZnectUwjN5awgZn2so/xr/d+I
InxjC172HWF6rbtZDaw4TRV8hDOfsyNLGZe5COe/zBVckDjdZw7o9MXwyF5kGePO
nC0C/n4PJjw8Ks5bhhM82MksjCK5jERaIaRfMIoV/DDtrIktrDLUBuXTx4e/d6Ly
HFnJAr2dMPFGo9To
-----END CERTIFICATE-----
`,
					Cert: `-----BEGIN CERTIFICATE-----
MIIDljCCAn4CCQDcMhq563xx9jANBgkqhkiG9w0BAQUFADCBjDELMAkGA1UEBhMC
Q04xETAPBgNVBAgMCFpoZUppYW5nMREwDwYDVQQHDAhIYW5nWmhvdTEQMA4GA1UE
CgwHQWxpYmFiYTERMA8GA1UECwwIQW50R3JvdXAxDzANBgNVBAMMBmQ3eS5pbzEh
MB8GCSqGSIb3DQEJARYSZ2FpdXMucWlAZ21haWwuY29tMB4XDTIxMDIwMjA4NTkw
MVoXDTMxMDEzMTA4NTkwMVowgYwxCzAJBgNVBAYTAkNOMREwDwYDVQQIDAhaaGVK
aWFuZzERMA8GA1UEBwwISGFuZ1pob3UxEDAOBgNVBAoMB0FsaWJhYmExETAPBgNV
BAsMCEFudEdyb3VwMQ8wDQYDVQQDDAZkN3kuaW8xITAfBgkqhkiG9w0BCQEWEmdh
aXVzLnFpQGdtYWlsLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
AJjKlMYlhnPTWBAvSwCxcC586co1NnQQ8uGfCVng66ah1nrP+41B0E2MsUcgv7GP
6wW87+g9/SVYbLp7ZFD716PXdUHCUP0JiRbRhbiTsK4BujPdm5R/gnzXWDpW4mV4
tyOArxPuCmhh1j6aorGzKnFunFua3M8/QozZzoHyo/EBWYEiUS069XTo4brSZFso
vGh8cSOdrth3w7+hve4//8HchS7j4TtoQPuuh2rJON4xooD02GjdR+exdgLSUj2P
hGLPNXINF/ziVvk7ZdBeVg2TkWyib6gY3M7n7P3QEfquhhdlpGWehstUqRJL2I+q
zz6jHgcJjcwzkN+0CgEHfYUCAwEAATANBgkqhkiG9w0BAQUFAAOCAQEAhYjX50mb
3mU8oapLM6C26H00cLT9V0IFyQtz2wsbB+jW45V7N+/UQ1PWXM49/Ins7694no6x
SA+82jiJlMaduMYxZCW/nZn7CrQ6HqnxwQzZwsQEHks7j6L3qGN8icEwp2OWBTEI
48XujDc86Q1TAKhWP25MypWb1HrR56Kp7pdEEOIP0ytwzQjup8cyTUCs/zyqva9s
kqfppdvFAZ93sm1XGnIvxPP08NUDZ+Fo2Dcs1+LaEb5p5xs8ASWmpFOsXSacBXiz
HY65P3PBCOpb15k48xOdrOD1PnHw2c+SfNQXe3icAeU7A5zAsLkRjgWDhP7IgSWl
4c5v7Jrmk2BNGw==
-----END CERTIFICATE-----
`,
					Key: `-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEAmMqUxiWGc9NYEC9LALFwLnzpyjU2dBDy4Z8JWeDrpqHWes/7
jUHQTYyxRyC/sY/rBbzv6D39JVhsuntkUPvXo9d1QcJQ/QmJFtGFuJOwrgG6M92b
lH+CfNdYOlbiZXi3I4CvE+4KaGHWPpqisbMqcW6cW5rczz9CjNnOgfKj8QFZgSJR
LTr1dOjhutJkWyi8aHxxI52u2HfDv6G97j//wdyFLuPhO2hA+66Hask43jGigPTY
aN1H57F2AtJSPY+EYs81cg0X/OJW+Ttl0F5WDZORbKJvqBjczufs/dAR+q6GF2Wk
ZZ6Gy1SpEkvYj6rPPqMeBwmNzDOQ37QKAQd9hQIDAQABAoIBAFjZRNZMr/jep1ES
D01h4VhHLzR06StpR7PH5YosbxxA9BYKp78mzFisPdKcypwYkpSNn/yvP2veFawD
YPxu1qDiA7+vnaTnTJ7GTDpfN9iYDI1oirY5x8mM+DNEnvZe0jCE/kpanBbC/fD9
vyoSg9XeenISheDGao65gYqzbH4SYsxOCASrczMwS/41BwNmQwFwH4ldma1NZimm
2vMv5j2Xur14tcJPRLxb29bdNJRMaOjglF1ZHHFRjqfQSXkrTc0R5zuOvHiJeb25
e+1RjKrKbRb49iZE23lJmCWUoff694mPdPi2J+UyxTFh8kFd1fe45K79e2Zy3nzz
h4Tbu2kCgYEAyPGnCo0WAwBMkpU2/165YwBkY32bA16LF+6OluRuJQcDPNlx1xro
mOZLkijJBOcV9dZm6fsjbwntxtmmPauFdw9sIxsG5o2bpmh5Xsnpu8lLSaEjysIB
5WKQ7aPTcagG9CK4WRpLkLPEMynoxo6mB/72kBVfld241S5qxiCqqh8CgYEAwqd4
c9DEtuvxGgW4KeIwMIbMXlrO6XjGesk8SSmWWwWq1hOC3XZ85TpQl2JRYTeMVJ6P
giGYMcLr8s8qika+1mpLmVmM1h+N7rvO5nsSsJ60YKqvxIQaXgKkIWMrxaCOgQmu
qTBaVTpo4QkpBWFAYSkjWwpUQHrbl3ypd/Jza9sCgYAPf8wxnSZIfvppCAdg9S55
e2tC3UpanS0/YFAxRVdVlc/jHqaQP+wW9xR3Jpwyu5xPBQWVIKDgDLUBdIJFGXjG
8TKXFpuWpu+Ni1tpO0vDB0i+WiaHmiVJSywAmHVTu/ElXZQ4kzWm6KbGh4ID5rbZ
wQnFnVBtH9gE4Xqs45ImYwKBgFf3eO7V7OBBli7NYOHEr0Ru1pLZdOJ7yy6YHJ9v
pNwznnWqUZylvGGXTe5r5x0JDmj7Ux1a8z2huiF7z9y3hey+ErYVixkFH45A34q3
GcYpopiA6nfjv0q25NeVyqVAHsZfysf46wnTIKx6CEi/H2oJPkoZS+Pr4ar/ElL7
hX+7AoGASE8vPTgNRhV/mYQO+SqqzqS42j/VxEFL9BK2TSOydeb0OljMlRCNSggY
xrHgCZQ1I6d7DIdFJC9OCSOA1342GDBSq7BtC11GCzfzl5gMGxiyXH1LtiXbgbDX
ObnPyoVaE0NngeUzY+6nWfRvHIarIIeYdC7cTN2frR6nZ856EPg=
-----END RSA PRIVATE KEY-----
`,
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
					CACert: `-----BEGIN CERTIFICATE-----
MIIDmDCCAoACCQCrkZtbv5nPpjANBgkqhkiG9w0BAQsFADCBjDELMAkGA1UEBhMC
Q04xETAPBgNVBAgMCFpoZUppYW5nMREwDwYDVQQHDAhIYW5nWmhvdTEQMA4GA1UE
CgwHQWxpYmFiYTERMA8GA1UECwwIQW50R3JvdXAxDzANBgNVBAMMBmQ3eS5pbzEh
MB8GCSqGSIb3DQEJARYSZ2FpdXMucWlAZ21haWwuY29tMCAXDTIxMDIwMjA4NTIx
NloYDzIxMjEwMTA5MDg1MjE2WjCBjDELMAkGA1UEBhMCQ04xETAPBgNVBAgMCFpo
ZUppYW5nMREwDwYDVQQHDAhIYW5nWmhvdTEQMA4GA1UECgwHQWxpYmFiYTERMA8G
A1UECwwIQW50R3JvdXAxDzANBgNVBAMMBmQ3eS5pbzEhMB8GCSqGSIb3DQEJARYS
Z2FpdXMucWlAZ21haWwuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKC
AQEAmrxd4DOYtV2iG73rh/281z9XLxBcVznhkxkL9y3dnPUBNRPk3ChsU14UUBzQ
QtKiIJNWtvG1N8COybl6PSnnyv29XAlJChp1/TBLIcrOZNLeQwxvrgGnXfonbIiy
ez6uq555qm3Yn4PPkZUIyLEjd6tKvRpvz4JgkaQ7VclHUK2c5DJ2HEH4LoZT7/XE
0UQOim1ayGBYBmWgXKIBWgVlqptRwKPIfN4nIqFcSdTSNvIOdnqK8XmG6okIkuUg
269QcP1cYgGL/GraKwIQ4SadGWUphIp6GEFLSqkz2aIjKg31cf7DGbitN4sWS1FS
jItk5oJMjAz+muvx8HuEVVaoCwIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQAr9EhE
NpSGWKoVa2jv5uNstDCONmqSVKg1HV31/z9lMJFsCNp3kb5QhwSickV89JsEulcD
JAd/q5ur/+r2Zru+j8nlB/90CB92YtyEo+acLj8CcigALdMWffU+NIoFzeMmMGDV
iHMPZrBnpMlLlTd6Gt7+hqWK4uIDYl1tmBLr4ORZnectUwjN5awgZn2so/xr/d+I
InxjC172HWF6rbtZDaw4TRV8hDOfsyNLGZe5COe/zBVckDjdZw7o9MXwyF5kGePO
nC0C/n4PJjw8Ks5bhhM82MksjCK5jERaIaRfMIoV/DDtrIktrDLUBuXTx4e/d6Ly
HFnJAr2dMPFGo9To
-----END CERTIFICATE-----
`,
					Cert: `-----BEGIN CERTIFICATE-----
MIIDljCCAn4CCQDcMhq563xx9jANBgkqhkiG9w0BAQUFADCBjDELMAkGA1UEBhMC
Q04xETAPBgNVBAgMCFpoZUppYW5nMREwDwYDVQQHDAhIYW5nWmhvdTEQMA4GA1UE
CgwHQWxpYmFiYTERMA8GA1UECwwIQW50R3JvdXAxDzANBgNVBAMMBmQ3eS5pbzEh
MB8GCSqGSIb3DQEJARYSZ2FpdXMucWlAZ21haWwuY29tMB4XDTIxMDIwMjA4NTkw
MVoXDTMxMDEzMTA4NTkwMVowgYwxCzAJBgNVBAYTAkNOMREwDwYDVQQIDAhaaGVK
aWFuZzERMA8GA1UEBwwISGFuZ1pob3UxEDAOBgNVBAoMB0FsaWJhYmExETAPBgNV
BAsMCEFudEdyb3VwMQ8wDQYDVQQDDAZkN3kuaW8xITAfBgkqhkiG9w0BCQEWEmdh
aXVzLnFpQGdtYWlsLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
AJjKlMYlhnPTWBAvSwCxcC586co1NnQQ8uGfCVng66ah1nrP+41B0E2MsUcgv7GP
6wW87+g9/SVYbLp7ZFD716PXdUHCUP0JiRbRhbiTsK4BujPdm5R/gnzXWDpW4mV4
tyOArxPuCmhh1j6aorGzKnFunFua3M8/QozZzoHyo/EBWYEiUS069XTo4brSZFso
vGh8cSOdrth3w7+hve4//8HchS7j4TtoQPuuh2rJON4xooD02GjdR+exdgLSUj2P
hGLPNXINF/ziVvk7ZdBeVg2TkWyib6gY3M7n7P3QEfquhhdlpGWehstUqRJL2I+q
zz6jHgcJjcwzkN+0CgEHfYUCAwEAATANBgkqhkiG9w0BAQUFAAOCAQEAhYjX50mb
3mU8oapLM6C26H00cLT9V0IFyQtz2wsbB+jW45V7N+/UQ1PWXM49/Ins7694no6x
SA+82jiJlMaduMYxZCW/nZn7CrQ6HqnxwQzZwsQEHks7j6L3qGN8icEwp2OWBTEI
48XujDc86Q1TAKhWP25MypWb1HrR56Kp7pdEEOIP0ytwzQjup8cyTUCs/zyqva9s
kqfppdvFAZ93sm1XGnIvxPP08NUDZ+Fo2Dcs1+LaEb5p5xs8ASWmpFOsXSacBXiz
HY65P3PBCOpb15k48xOdrOD1PnHw2c+SfNQXe3icAeU7A5zAsLkRjgWDhP7IgSWl
4c5v7Jrmk2BNGw==
-----END CERTIFICATE-----
`,
					Key: `-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEAmMqUxiWGc9NYEC9LALFwLnzpyjU2dBDy4Z8JWeDrpqHWes/7
jUHQTYyxRyC/sY/rBbzv6D39JVhsuntkUPvXo9d1QcJQ/QmJFtGFuJOwrgG6M92b
lH+CfNdYOlbiZXi3I4CvE+4KaGHWPpqisbMqcW6cW5rczz9CjNnOgfKj8QFZgSJR
LTr1dOjhutJkWyi8aHxxI52u2HfDv6G97j//wdyFLuPhO2hA+66Hask43jGigPTY
aN1H57F2AtJSPY+EYs81cg0X/OJW+Ttl0F5WDZORbKJvqBjczufs/dAR+q6GF2Wk
ZZ6Gy1SpEkvYj6rPPqMeBwmNzDOQ37QKAQd9hQIDAQABAoIBAFjZRNZMr/jep1ES
D01h4VhHLzR06StpR7PH5YosbxxA9BYKp78mzFisPdKcypwYkpSNn/yvP2veFawD
YPxu1qDiA7+vnaTnTJ7GTDpfN9iYDI1oirY5x8mM+DNEnvZe0jCE/kpanBbC/fD9
vyoSg9XeenISheDGao65gYqzbH4SYsxOCASrczMwS/41BwNmQwFwH4ldma1NZimm
2vMv5j2Xur14tcJPRLxb29bdNJRMaOjglF1ZHHFRjqfQSXkrTc0R5zuOvHiJeb25
e+1RjKrKbRb49iZE23lJmCWUoff694mPdPi2J+UyxTFh8kFd1fe45K79e2Zy3nzz
h4Tbu2kCgYEAyPGnCo0WAwBMkpU2/165YwBkY32bA16LF+6OluRuJQcDPNlx1xro
mOZLkijJBOcV9dZm6fsjbwntxtmmPauFdw9sIxsG5o2bpmh5Xsnpu8lLSaEjysIB
5WKQ7aPTcagG9CK4WRpLkLPEMynoxo6mB/72kBVfld241S5qxiCqqh8CgYEAwqd4
c9DEtuvxGgW4KeIwMIbMXlrO6XjGesk8SSmWWwWq1hOC3XZ85TpQl2JRYTeMVJ6P
giGYMcLr8s8qika+1mpLmVmM1h+N7rvO5nsSsJ60YKqvxIQaXgKkIWMrxaCOgQmu
qTBaVTpo4QkpBWFAYSkjWwpUQHrbl3ypd/Jza9sCgYAPf8wxnSZIfvppCAdg9S55
e2tC3UpanS0/YFAxRVdVlc/jHqaQP+wW9xR3Jpwyu5xPBQWVIKDgDLUBdIJFGXjG
8TKXFpuWpu+Ni1tpO0vDB0i+WiaHmiVJSywAmHVTu/ElXZQ4kzWm6KbGh4ID5rbZ
wQnFnVBtH9gE4Xqs45ImYwKBgFf3eO7V7OBBli7NYOHEr0Ru1pLZdOJ7yy6YHJ9v
pNwznnWqUZylvGGXTe5r5x0JDmj7Ux1a8z2huiF7z9y3hey+ErYVixkFH45A34q3
GcYpopiA6nfjv0q25NeVyqVAHsZfysf46wnTIKx6CEi/H2oJPkoZS+Pr4ar/ElL7
hX+7AoGASE8vPTgNRhV/mYQO+SqqzqS42j/VxEFL9BK2TSOydeb0OljMlRCNSggY
xrHgCZQ1I6d7DIdFJC9OCSOA1342GDBSq7BtC11GCzfzl5gMGxiyXH1LtiXbgbDX
ObnPyoVaE0NngeUzY+6nWfRvHIarIIeYdC7cTN2frR6nZ856EPg=
-----END RSA PRIVATE KEY-----
`,
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
			Option: storage.Option{
				DataPath: "/tmp/storage/data",
				TaskExpireTime: clientutil.Duration{
					Duration: 180000000000,
				},
			},
			StoreStrategy: storage.StoreStrategy("io.d7y.storage.v2.simple"),
		},
		Proxy: &ProxyOption{
			ListenOption: ListenOption{
				Security: SecurityOption{
					Insecure: true,
					CACert: `-----BEGIN CERTIFICATE-----
MIIDmDCCAoACCQCrkZtbv5nPpjANBgkqhkiG9w0BAQsFADCBjDELMAkGA1UEBhMC
Q04xETAPBgNVBAgMCFpoZUppYW5nMREwDwYDVQQHDAhIYW5nWmhvdTEQMA4GA1UE
CgwHQWxpYmFiYTERMA8GA1UECwwIQW50R3JvdXAxDzANBgNVBAMMBmQ3eS5pbzEh
MB8GCSqGSIb3DQEJARYSZ2FpdXMucWlAZ21haWwuY29tMCAXDTIxMDIwMjA4NTIx
NloYDzIxMjEwMTA5MDg1MjE2WjCBjDELMAkGA1UEBhMCQ04xETAPBgNVBAgMCFpo
ZUppYW5nMREwDwYDVQQHDAhIYW5nWmhvdTEQMA4GA1UECgwHQWxpYmFiYTERMA8G
A1UECwwIQW50R3JvdXAxDzANBgNVBAMMBmQ3eS5pbzEhMB8GCSqGSIb3DQEJARYS
Z2FpdXMucWlAZ21haWwuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKC
AQEAmrxd4DOYtV2iG73rh/281z9XLxBcVznhkxkL9y3dnPUBNRPk3ChsU14UUBzQ
QtKiIJNWtvG1N8COybl6PSnnyv29XAlJChp1/TBLIcrOZNLeQwxvrgGnXfonbIiy
ez6uq555qm3Yn4PPkZUIyLEjd6tKvRpvz4JgkaQ7VclHUK2c5DJ2HEH4LoZT7/XE
0UQOim1ayGBYBmWgXKIBWgVlqptRwKPIfN4nIqFcSdTSNvIOdnqK8XmG6okIkuUg
269QcP1cYgGL/GraKwIQ4SadGWUphIp6GEFLSqkz2aIjKg31cf7DGbitN4sWS1FS
jItk5oJMjAz+muvx8HuEVVaoCwIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQAr9EhE
NpSGWKoVa2jv5uNstDCONmqSVKg1HV31/z9lMJFsCNp3kb5QhwSickV89JsEulcD
JAd/q5ur/+r2Zru+j8nlB/90CB92YtyEo+acLj8CcigALdMWffU+NIoFzeMmMGDV
iHMPZrBnpMlLlTd6Gt7+hqWK4uIDYl1tmBLr4ORZnectUwjN5awgZn2so/xr/d+I
InxjC172HWF6rbtZDaw4TRV8hDOfsyNLGZe5COe/zBVckDjdZw7o9MXwyF5kGePO
nC0C/n4PJjw8Ks5bhhM82MksjCK5jERaIaRfMIoV/DDtrIktrDLUBuXTx4e/d6Ly
HFnJAr2dMPFGo9To
-----END CERTIFICATE-----
`,
					Cert: `-----BEGIN CERTIFICATE-----
MIIDljCCAn4CCQDcMhq563xx9jANBgkqhkiG9w0BAQUFADCBjDELMAkGA1UEBhMC
Q04xETAPBgNVBAgMCFpoZUppYW5nMREwDwYDVQQHDAhIYW5nWmhvdTEQMA4GA1UE
CgwHQWxpYmFiYTERMA8GA1UECwwIQW50R3JvdXAxDzANBgNVBAMMBmQ3eS5pbzEh
MB8GCSqGSIb3DQEJARYSZ2FpdXMucWlAZ21haWwuY29tMB4XDTIxMDIwMjA4NTkw
MVoXDTMxMDEzMTA4NTkwMVowgYwxCzAJBgNVBAYTAkNOMREwDwYDVQQIDAhaaGVK
aWFuZzERMA8GA1UEBwwISGFuZ1pob3UxEDAOBgNVBAoMB0FsaWJhYmExETAPBgNV
BAsMCEFudEdyb3VwMQ8wDQYDVQQDDAZkN3kuaW8xITAfBgkqhkiG9w0BCQEWEmdh
aXVzLnFpQGdtYWlsLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
AJjKlMYlhnPTWBAvSwCxcC586co1NnQQ8uGfCVng66ah1nrP+41B0E2MsUcgv7GP
6wW87+g9/SVYbLp7ZFD716PXdUHCUP0JiRbRhbiTsK4BujPdm5R/gnzXWDpW4mV4
tyOArxPuCmhh1j6aorGzKnFunFua3M8/QozZzoHyo/EBWYEiUS069XTo4brSZFso
vGh8cSOdrth3w7+hve4//8HchS7j4TtoQPuuh2rJON4xooD02GjdR+exdgLSUj2P
hGLPNXINF/ziVvk7ZdBeVg2TkWyib6gY3M7n7P3QEfquhhdlpGWehstUqRJL2I+q
zz6jHgcJjcwzkN+0CgEHfYUCAwEAATANBgkqhkiG9w0BAQUFAAOCAQEAhYjX50mb
3mU8oapLM6C26H00cLT9V0IFyQtz2wsbB+jW45V7N+/UQ1PWXM49/Ins7694no6x
SA+82jiJlMaduMYxZCW/nZn7CrQ6HqnxwQzZwsQEHks7j6L3qGN8icEwp2OWBTEI
48XujDc86Q1TAKhWP25MypWb1HrR56Kp7pdEEOIP0ytwzQjup8cyTUCs/zyqva9s
kqfppdvFAZ93sm1XGnIvxPP08NUDZ+Fo2Dcs1+LaEb5p5xs8ASWmpFOsXSacBXiz
HY65P3PBCOpb15k48xOdrOD1PnHw2c+SfNQXe3icAeU7A5zAsLkRjgWDhP7IgSWl
4c5v7Jrmk2BNGw==
-----END CERTIFICATE-----
`,
					Key: `-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEAmMqUxiWGc9NYEC9LALFwLnzpyjU2dBDy4Z8JWeDrpqHWes/7
jUHQTYyxRyC/sY/rBbzv6D39JVhsuntkUPvXo9d1QcJQ/QmJFtGFuJOwrgG6M92b
lH+CfNdYOlbiZXi3I4CvE+4KaGHWPpqisbMqcW6cW5rczz9CjNnOgfKj8QFZgSJR
LTr1dOjhutJkWyi8aHxxI52u2HfDv6G97j//wdyFLuPhO2hA+66Hask43jGigPTY
aN1H57F2AtJSPY+EYs81cg0X/OJW+Ttl0F5WDZORbKJvqBjczufs/dAR+q6GF2Wk
ZZ6Gy1SpEkvYj6rPPqMeBwmNzDOQ37QKAQd9hQIDAQABAoIBAFjZRNZMr/jep1ES
D01h4VhHLzR06StpR7PH5YosbxxA9BYKp78mzFisPdKcypwYkpSNn/yvP2veFawD
YPxu1qDiA7+vnaTnTJ7GTDpfN9iYDI1oirY5x8mM+DNEnvZe0jCE/kpanBbC/fD9
vyoSg9XeenISheDGao65gYqzbH4SYsxOCASrczMwS/41BwNmQwFwH4ldma1NZimm
2vMv5j2Xur14tcJPRLxb29bdNJRMaOjglF1ZHHFRjqfQSXkrTc0R5zuOvHiJeb25
e+1RjKrKbRb49iZE23lJmCWUoff694mPdPi2J+UyxTFh8kFd1fe45K79e2Zy3nzz
h4Tbu2kCgYEAyPGnCo0WAwBMkpU2/165YwBkY32bA16LF+6OluRuJQcDPNlx1xro
mOZLkijJBOcV9dZm6fsjbwntxtmmPauFdw9sIxsG5o2bpmh5Xsnpu8lLSaEjysIB
5WKQ7aPTcagG9CK4WRpLkLPEMynoxo6mB/72kBVfld241S5qxiCqqh8CgYEAwqd4
c9DEtuvxGgW4KeIwMIbMXlrO6XjGesk8SSmWWwWq1hOC3XZ85TpQl2JRYTeMVJ6P
giGYMcLr8s8qika+1mpLmVmM1h+N7rvO5nsSsJ60YKqvxIQaXgKkIWMrxaCOgQmu
qTBaVTpo4QkpBWFAYSkjWwpUQHrbl3ypd/Jza9sCgYAPf8wxnSZIfvppCAdg9S55
e2tC3UpanS0/YFAxRVdVlc/jHqaQP+wW9xR3Jpwyu5xPBQWVIKDgDLUBdIJFGXjG
8TKXFpuWpu+Ni1tpO0vDB0i+WiaHmiVJSywAmHVTu/ElXZQ4kzWm6KbGh4ID5rbZ
wQnFnVBtH9gE4Xqs45ImYwKBgFf3eO7V7OBBli7NYOHEr0Ru1pLZdOJ7yy6YHJ9v
pNwznnWqUZylvGGXTe5r5x0JDmj7Ux1a8z2huiF7z9y3hey+ErYVixkFH45A34q3
GcYpopiA6nfjv0q25NeVyqVAHsZfysf46wnTIKx6CEi/H2oJPkoZS+Pr4ar/ElL7
hX+7AoGASE8vPTgNRhV/mYQO+SqqzqS42j/VxEFL9BK2TSOydeb0OljMlRCNSggY
xrHgCZQ1I6d7DIdFJC9OCSOA1342GDBSq7BtC11GCzfzl5gMGxiyXH1LtiXbgbDX
ObnPyoVaE0NngeUzY+6nWfRvHIarIIeYdC7cTN2frR6nZ856EPg=
-----END RSA PRIVATE KEY-----
`,
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
						Host: "https://index.docker.io",
					},
				},
				// TODO
				Certs:    &CertPool{},
				Insecure: false,
				Direct:   false,
			},
			Proxies: []*Proxy{
				{
					Regx:     proxyExp,
					UseHTTPS: false,
					Direct:   false,
					Redirect: "d7s.io",
				},
			},
			HijackHTTPS: &HijackConfig{
				Cert: `-----BEGIN CERTIFICATE-----
MIIDmDCCAoACCQCrkZtbv5nPpjANBgkqhkiG9w0BAQsFADCBjDELMAkGA1UEBhMC
Q04xETAPBgNVBAgMCFpoZUppYW5nMREwDwYDVQQHDAhIYW5nWmhvdTEQMA4GA1UE
CgwHQWxpYmFiYTERMA8GA1UECwwIQW50R3JvdXAxDzANBgNVBAMMBmQ3eS5pbzEh
MB8GCSqGSIb3DQEJARYSZ2FpdXMucWlAZ21haWwuY29tMCAXDTIxMDIwMjA4NTIx
NloYDzIxMjEwMTA5MDg1MjE2WjCBjDELMAkGA1UEBhMCQ04xETAPBgNVBAgMCFpo
ZUppYW5nMREwDwYDVQQHDAhIYW5nWmhvdTEQMA4GA1UECgwHQWxpYmFiYTERMA8G
A1UECwwIQW50R3JvdXAxDzANBgNVBAMMBmQ3eS5pbzEhMB8GCSqGSIb3DQEJARYS
Z2FpdXMucWlAZ21haWwuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKC
AQEAmrxd4DOYtV2iG73rh/281z9XLxBcVznhkxkL9y3dnPUBNRPk3ChsU14UUBzQ
QtKiIJNWtvG1N8COybl6PSnnyv29XAlJChp1/TBLIcrOZNLeQwxvrgGnXfonbIiy
ez6uq555qm3Yn4PPkZUIyLEjd6tKvRpvz4JgkaQ7VclHUK2c5DJ2HEH4LoZT7/XE
0UQOim1ayGBYBmWgXKIBWgVlqptRwKPIfN4nIqFcSdTSNvIOdnqK8XmG6okIkuUg
269QcP1cYgGL/GraKwIQ4SadGWUphIp6GEFLSqkz2aIjKg31cf7DGbitN4sWS1FS
jItk5oJMjAz+muvx8HuEVVaoCwIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQAr9EhE
NpSGWKoVa2jv5uNstDCONmqSVKg1HV31/z9lMJFsCNp3kb5QhwSickV89JsEulcD
JAd/q5ur/+r2Zru+j8nlB/90CB92YtyEo+acLj8CcigALdMWffU+NIoFzeMmMGDV
iHMPZrBnpMlLlTd6Gt7+hqWK4uIDYl1tmBLr4ORZnectUwjN5awgZn2so/xr/d+I
InxjC172HWF6rbtZDaw4TRV8hDOfsyNLGZe5COe/zBVckDjdZw7o9MXwyF5kGePO
nC0C/n4PJjw8Ks5bhhM82MksjCK5jERaIaRfMIoV/DDtrIktrDLUBuXTx4e/d6Ly
HFnJAr2dMPFGo9To
-----END CERTIFICATE-----
`,
				Key: `-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAmrxd4DOYtV2iG73rh/281z9XLxBcVznhkxkL9y3dnPUBNRPk
3ChsU14UUBzQQtKiIJNWtvG1N8COybl6PSnnyv29XAlJChp1/TBLIcrOZNLeQwxv
rgGnXfonbIiyez6uq555qm3Yn4PPkZUIyLEjd6tKvRpvz4JgkaQ7VclHUK2c5DJ2
HEH4LoZT7/XE0UQOim1ayGBYBmWgXKIBWgVlqptRwKPIfN4nIqFcSdTSNvIOdnqK
8XmG6okIkuUg269QcP1cYgGL/GraKwIQ4SadGWUphIp6GEFLSqkz2aIjKg31cf7D
GbitN4sWS1FSjItk5oJMjAz+muvx8HuEVVaoCwIDAQABAoIBADtP0ugavwFeN8JM
hVjmURls5R1cNxkUTbwV0LlZsmX0oBbZXlNph0RZfo3KzaWfmZfFAmszsVQCknkO
iKWjR39OzePWh1HBHNMFTPdSBtUs4xduT+yyI5ZpSe1XZJrLRybvoxFJ88wJOWpD
Z0+OXtsDJK3h0VQIVL3e5wOWGKj6msf3l0IGQ6Al8PGzmB0Ynl9zf68ImpZlFxrW
HubeLf08OWCZ4ztW0NYpwOIjuOecp500uFhf9TQhmQ19/NzTN4Y1hOGrUCvsVTZw
vn1MrehXANcxg3bX5gHljquR+grJCqDqK8ezMX+ovc6bNiz80/iplNYvzTkgRsVg
LhIfPhkCgYEAzkro8HgZcR+Eo3Q6JcB1ZdZ8cmMkKBaISICMhDy33zTGP8HnIgvA
tCjThtl+l1OFl8xpPNexUq78P43nABId84GoUijvYCQmTtZe4O3zG2ehso4aVI0V
vqYjRxrF4YjdaXbhDmSKtGv4dXE9o9XzMIagAwrfNVWWbTIgMr6tbXUCgYEAwAUx
Rq/pvHa4toew9rqgn3gsjW2UDh03Zz+VWpQQbGNnx3uMtGk04snCWS9a2yyTrMnW
tphOcJ8lPGMiPX8VVsPjjj3Bt63Jsbs81TY2n3Nyzbv4deyPCESSeVMblc6bz0U5
hPSd53uVbQ8uKPqeBh0zYSOfNuci9BSMyjZlj38CgYEAs8KrAQBmiyC/3/8jvEnd
AwTIJJUnqJcZ4rPBvml6gFHofx5kXlKHdXYt/NABc7QgMYq6GX6K0lYREQCCTpl6
5/oPxqhNDyXxC7nZq38t3K+NQ554az0Vua/kBc3aqREufvxMWNJb26RKOWyYHfAN
njdxr7UFk+Ak2LpZPF7TmRECgYBK47uMLh+i+UqhJdrG35K1n1EA97O+rXZmNk4x
vX2KSFq5Wl5OHpoZuUarMeTlhkUyJqYvIMe29Nq46pD4GxBffpbJaxyaXpVDVBst
rK8xEP29b4o/s7s1JklaOCeSDbqG5CDC0gSju3dTyY/fO59WEx1uzU3TQ+JF/53J
X93MUQKBgCw+riF50h7CvZZgjU6p3HePIBDUf1FK1CIG30+uTS6UhL8YF8/qtVFH
oDeAe34g8jeiCuOPgEV55yz45JqfsiMHPv7Kczx0tHWhfFejYqQ1KSF8+iC6bR8a
DBEmmd5L+p8pm/WP9Lvqkj2NQPgdjkuIXQ+yOS0Ede3M65i6CyTS
-----END RSA PRIVATE KEY-----
`,
				Hosts: []*HijackHost{
					{
						Regx:     hijackExp,
						Insecure: false,
						// TODO
						Certs: &CertPool{},
					},
				},
			},
		},
	}

	p2 := &PeerHostOption{}
	if err := p2.Load("../daemon/test/testdata/config/daemon.yml"); err != nil {
		t.Fatal(err)
	}

	// t.Logf("%#v\n", p1)

	// s, _ := json.MarshalIndent(p1, "", "\t")
	// fmt.Printf("%s", s)

	assert.EqualValues(p1, p2)
}
