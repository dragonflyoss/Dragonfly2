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
	"testing"

	"gopkg.in/yaml.v3"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
)

func Test_UnmarshalJSON(t *testing.T) {
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
				"remote": "https://index.docker.io"
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
	json.Unmarshal(bytes, &s)
	t.Logf("%#v\n", s)
}

func Test_UnmarshalYAML(t *testing.T) {
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
    remote: https://index.docker.io
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
	err := yaml.Unmarshal(bytes, &s)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%#v\n", s)
}
