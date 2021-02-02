package daemon

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io/ioutil"
	"strings"
	"time"

	"github.com/docker/go-units"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"
	"gopkg.in/yaml.v3"

	"github.com/dragonflyoss/Dragonfly2/client/config"
	"github.com/dragonflyoss/Dragonfly2/client/daemon/storage"
	"github.com/dragonflyoss/Dragonfly2/pkg/basic/dfnet"
)

type PeerHostOption struct {
	Schedulers []dfnet.NetAddr `json:"schedulers" yaml:"schedulers"`

	// AliveTime indicates alive duration for which daemon keeps no accessing by any uploading and download requests,
	// after this period daemon will automatically exit
	// when AliveTime == 0, will run infinitely
	// TODO keepalive detect
	AliveTime  Duration `json:"alive_time" yaml:"alive_time"`
	GCInterval Duration `json:"gc_interval" yaml:"gc_interval"`

	KeepStorage bool `json:"keep_storage" yaml:"keep_storage"`

	Download DownloadOption `json:"download" yaml:"download"`
	Proxy    *ProxyOption   `json:"proxy,omitempty" yaml:"proxy,omitempty"`
	Upload   UploadOption   `json:"upload" yaml:"upload"`
	Storage  StorageOption  `json:"storage" yaml:"storage"`
}

type DownloadOption struct {
	RateLimit    RateLimit    `json:"rate_limit" yaml:"rate_limit"`
	DownloadGRPC ListenOption `json:"download_grpc" yaml:"download_grpc"`
	PeerGRPC     ListenOption `json:"peer_grpc" yaml:"peer_grpc"`
}

type ProxyOption struct {
	ListenOption   `yaml:",inline"`
	RegistryMirror *config.RegistryMirror `json:"registry_mirror" yaml:"registry_mirror"`
	Proxies        []*config.Proxy        `json:"proxies" yaml:"proxies"`
	HijackHTTPS    *config.HijackConfig   `json:"hijack_https" yaml:"hijack_https"`
}

type UploadOption struct {
	ListenOption `yaml:",inline"`
	RateLimit    RateLimit `json:"rate_limit" yaml:"rate_limit"`
}

type ListenOption struct {
	Security   SecurityOption    `json:"security" yaml:"security"`
	TCPListen  *TCPListenOption  `json:"tcp_listen,omitempty" yaml:"tcp_listen,omitempty"`
	UnixListen *UnixListenOption `json:"unix_listen,omitempty" yaml:"unix_listen,omitempty"`
}

type TCPListenOption struct {
	// Listen stands listen interface, like: 0.0.0.0, 192.168.0.1
	Listen string `json:"listen"`

	// PortRange stands listen port
	// yaml example 1:
	//   port: 12345
	// yaml example 2:
	//   port:
	//     start: 12345
	//     end: 12346
	PortRange TCPListenPortRange `json:"port"`
}

type TCPListenPortRange struct {
	Start int
	End   int
}

func (t *TCPListenPortRange) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	return t.unmarshal(v)
}

func (t *TCPListenPortRange) UnmarshalYAML(node *yaml.Node) error {
	var v interface{}
	switch node.Kind {
	case yaml.MappingNode:
		var m = make(map[string]interface{})
		for i := 0; i < len(node.Content); i += 2 {
			var (
				key   string
				value int
			)
			if err := node.Content[i].Decode(&key); err != nil {
				return err
			}
			if err := node.Content[i+1].Decode(&value); err != nil {
				return err
			}
			m[key] = value
		}
		v = m
	case yaml.ScalarNode:
		var i int
		if err := node.Decode(&i); err != nil {
			return err
		}
		v = i
	}
	return t.unmarshal(v)
}

func (t *TCPListenPortRange) unmarshal(v interface{}) error {
	switch value := v.(type) {
	case int:
		t.Start = value
		return nil
	case float64:
		t.Start = int(value)
		return nil
	case map[string]interface{}:
		if s, ok := value["start"]; ok {
			switch start := s.(type) {
			case float64:
				t.Start = int(start)
			case int:
				t.Start = start
			default:
				return errors.New("invalid start port")
			}
		} else {
			return errors.New("empty start port")
		}
		if e, ok := value["end"]; ok {
			switch end := e.(type) {
			case float64:
				t.End = int(end)
			case int:
				t.End = end
			default:
				return errors.New("invalid end port")
			}
		}
		return nil
	default:
		return errors.New("invalid port")
	}
}

type UnixListenOption struct {
	Socket string `json:"socket" yaml:"socket"`
}

type SecurityOption struct {
	Insecure  bool        `json:"insecure"`
	CACert    string      `json:"ca_cert"`
	Cert      string      `json:"cert"`
	Key       string      `json:"key"`
	TLSConfig *tls.Config `json:"tls_config" yaml:"tls_config"`
}

type StorageOption struct {
	storage.Option `yaml:",inline"`
	StoreStrategy  storage.StoreStrategy `json:"strategy" yaml:"strategy"`
}

// RateLimit is a wrapper for rate.Limit, support json and yaml unmarshal function
// yaml example 1:
//   rate_limit: 2097152 # 2MiB
// yaml example 2:
//   rate_limit: 2MiB
type RateLimit struct {
	rate.Limit
}

func (r *RateLimit) UnmarshalJSON(b []byte) error {
	return r.unmarshal(json.Unmarshal, b)
}

func (r *RateLimit) UnmarshalYAML(node *yaml.Node) error {
	return r.unmarshal(yaml.Unmarshal, []byte(node.Value))
}

func (r *RateLimit) unmarshal(unmarshal func(in []byte, out interface{}) (err error), b []byte) error {
	var v interface{}
	if err := unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		r.Limit = rate.Limit(value)
		return nil
	case string:
		limit, err := units.RAMInBytes(value)
		if err != nil {
			return errors.WithMessage(err, "invalid port")
		}
		r.Limit = rate.Limit(limit)
		return nil
	default:
		return errors.New("invalid port")
	}
}

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	return d.unmarshal(v)
}

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var v interface{}
	switch node.Kind {
	case yaml.ScalarNode:
		switch node.Tag {
		case "!!int":
			var i int
			if err := node.Decode(&i); err != nil {
				return err
			}
			v = i
		case "!!str":
			var i string
			if err := node.Decode(&i); err != nil {
				return err
			}
			v = i
		default:
			return errors.New("invalid duration")
		}
	default:
		return errors.New("invalid duration")
	}
	return d.unmarshal(v)
}

func (d *Duration) unmarshal(v interface{}) error {
	switch value := v.(type) {
	case float64:
		d.Duration = time.Duration(value)
		return nil
	case int:
		d.Duration = time.Duration(value)
		return nil
	case string:
		var err error
		d.Duration, err = time.ParseDuration(value)
		if err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid duration")
	}
}

type FileString string

func (f *FileString) UnmarshalJSON(b []byte) error {
	var s string
	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}
	file, err := ioutil.ReadFile(s)
	if err != nil {
		return err
	}
	val := strings.TrimSpace(string(file))
	*f = FileString(val)
	return nil
}

type tlsConfigFiles struct {
	Cert   string     `json:"cert"`
	Key    string     `json:"key"`
	CACert FileString `json:"ca_cert"`
}

type TLSConfig struct {
	tls.Config
}

func (t *TLSConfig) UnmarshalJSON(b []byte) error {
	var cf tlsConfigFiles
	err := json.Unmarshal(b, &cf)
	if err != nil {
		return err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(cf.CACert)) {
		return errors.New("invalid CA Cert")
	}
	cert, err := tls.LoadX509KeyPair(cf.Cert, cf.Key)
	if err != nil {
		return err
	}
	t.Config = tls.Config{
		RootCAs:      pool,
		Certificates: []tls.Certificate{cert},
	}
	return nil
}
