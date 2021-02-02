package daemon

import (
	"encoding/json"
	"testing"

	"gopkg.in/yaml.v3"
)

func Test_TLSConfig(t *testing.T) {
	bytes := []byte(`{
    "tlsconfig": {
        "key": "/Users/jim/Code/go/src/gitlab.alipay-inc.com/criu-daemon/criu-agent/config/example/sca.key",
        "cert": "/Users/jim/Code/go/src/gitlab.alipay-inc.com/criu-daemon/criu-agent/config/example/sca.crt",
        "ca_cert": "/Users/jim/Code/go/src/gitlab.alipay-inc.com/criu-daemon/criu-agent/config/example/ca.crt"
    },
    "port": {
        "start": 1234,
        "end": 1235
    }
}`)
	var x = struct {
		TLSConfig *TLSConfig         `json:"tlsconfig"`
		Port      TCPListenPortRange `json:"port"`
	}{}
	json.Unmarshal(bytes, &x)
	t.Logf("%#v", x)
}

func Test_LimitRate(t *testing.T) {
	bytes := []byte(`
timeout: 3m
limit: 2MiB
port1: 1024
port2:
  start: 1025
  end: 1026
`)
	s := &struct {
		Timeout Duration           `yaml:"timeout"`
		Limit   RateLimit          `yaml:"limit"`
		Port1   TCPListenPortRange `yaml:"port1"`
		Port2   TCPListenPortRange `yaml:"port2"`
	}{

	}
	err := yaml.Unmarshal(bytes, s)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%#v", s)
}
