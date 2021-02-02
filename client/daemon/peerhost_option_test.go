package daemon

import (
	"encoding/json"
	"testing"

	"gopkg.in/yaml.v3"
)

func Test_UnmarshalJSON(t *testing.T) {
	bytes := []byte(`{
		"tls": {
			"key": "test/testdata/certs/sca.key",
			"cert": "test/testdata/certs/sca.crt",
			"ca_cert": "test/testdata/certs/mca.crt"
		},
		"url": "https://d7y.io",
    "certs": [
			"test/testdata/certs/ca.crt",
			"test/testdata/certs/mca.crt",
			"test/testdata/certs/sca.crt"
    ],
		"regx": "blobs/sha256.*",
		"port1": 1001,
		"port2": {
			"start": 1002,
			"end": 1003
		},
		"timeout": "3m",
		"limit": "2Mib"
}`)

	var s = struct {
		TLSConfig *TLSConfig         `json:"tls"`
		URL       *URL               `json:"url"`
		Certs     *CertPool          `json:"certs"`
		Regx      *Regexp            `json:"regx"`
		Port1     TCPListenPortRange `json:"port1"`
		Port2     TCPListenPortRange `json:"port2"`
		Timeout   Duration           `json:"timeout"`
		Limit     RateLimit          `json:"limit"`
	}{}
	json.Unmarshal(bytes, &s)
	t.Logf("%#v", s)
}

func Test_UnmarshalYAML(t *testing.T) {
	bytes := []byte(`
tls:
  key: test/testdata/certs/sca.key
  cert: test/testdata/certs/sca.crt
  ca_cert: test/testdata/certs/mca.crt
url: https://d7y.io
certs: ["test/testdata/certs/ca.crt", "test/testdata/certs/mca.crt", "test/testdata/certs/sca.crt"]
regx: blobs/sha256.*
port1: 1001 
port2:
  start: 1002
  end: 1003
timeout: 3m
limit: 2Mib
`)

	var s = struct {
		TLSConfig *TLSConfig         `yaml:"tls"`
		URL       *URL               `yaml:"url"`
		Certs     *CertPool          `yaml:"certs"`
		Regx      *Regexp            `yaml:"regx"`
		Port1     TCPListenPortRange `yaml:"port1"`
		Port2     TCPListenPortRange `yaml:"port2"`
		Timeout   Duration           `yaml:"timeout"`
		Limit     RateLimit          `yaml:"limit"`
	}{}
	err := yaml.Unmarshal(bytes, &s)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%#v", s)
}
