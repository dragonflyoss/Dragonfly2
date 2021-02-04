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

// Package config holds all options of peerhost.
package config

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	"github.com/dragonflyoss/Dragonfly/v2/client/clientutil"
	"github.com/dragonflyoss/Dragonfly/v2/client/daemon/storage"
	"github.com/dragonflyoss/Dragonfly/v2/pkg/basic/dfnet"
	"github.com/dragonflyoss/Dragonfly/v2/pkg/util/jsonutils"
)

type PeerHostOption struct {
	// AliveTime indicates alive duration for which daemon keeps no accessing by any uploading and download requests,
	// after this period daemon will automatically exit
	// when AliveTime == 0, will run infinitely
	AliveTime  clientutil.Duration `json:"alive_time" yaml:"alive_time"`
	GCInterval clientutil.Duration `json:"gc_interval" yaml:"gc_interval"`

	// Pid file location
	PidFile string `json:"pid_file" yaml:"pid_file"`
	// Lock file location
	LockFile string `json:"lock_file" yaml:"lock_file"`

	DataDir     string          `json:"data_dir" yaml:"data_dir"`
	WorkHome    string          `json:"work_home" yaml:"work_home"`
	KeepStorage bool            `json:"keep_storage" yaml:"keep_storage"`
	Schedulers  []dfnet.NetAddr `json:"schedulers" yaml:"schedulers"`
	Verbose     bool            `yaml:"verbose" json:"verbose"`

	Host     HostOption     `json:"host" yaml:"host"`
	Download DownloadOption `json:"download" yaml:"download"`
	Proxy    *ProxyOption   `json:"proxy,omitempty" yaml:"proxy,omitempty"`
	Upload   UploadOption   `json:"upload" yaml:"upload"`
	Storage  StorageOption  `json:"storage" yaml:"storage"`
}

func (p *PeerHostOption) Load(path string) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("unable to load peer host configuration from %q [%v]", path, err)
	}

	switch filepath.Ext(path)[1:] {
	case "json":
		err := json.Unmarshal(data, &p)
		if err != nil {
			return err
		}
		return nil
	case "yml", "yaml":
		err := yaml.Unmarshal(data, &p)
		if err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("extension of %s is not in 'yml/yaml/json'", path)
}

type HostOption struct {
	SecurityDomain string `json:"security_domain" yaml:"security_domain"`
	// Peerhost location for scheduler
	Location string `json:"location" yaml:"location"`
	// Peerhost idc for scheduler
	IDC string `json:"idc" yaml:"idc"`
	// Peerhost net topology for scheduler
	NetTopology string `json:"net_topology" yaml:"net_topology"`
	// The listen ip for all tcp services of daemon
	ListenIP string `json:"listen_ip" yaml:"listen_ip"`
	// The ip report to scheduler, normal same with listen ip
	AdvertiseIP string `json:"advertise_ip" yaml:"advertise_ip"`
}

type DownloadOption struct {
	RateLimit    clientutil.RateLimit `json:"rate_limit" yaml:"rate_limit"`
	DownloadGRPC ListenOption         `json:"download_grpc" yaml:"download_grpc"`
	PeerGRPC     ListenOption         `json:"peer_grpc" yaml:"peer_grpc"`
}

type ProxyOption struct {
	ListenOption   `yaml:",inline"`
	RegistryMirror *RegistryMirror `json:"registry_mirror" yaml:"registry_mirror"`
	Proxies        []*Proxy        `json:"proxies" yaml:"proxies"`
	HijackHTTPS    *HijackConfig   `json:"hijack_https" yaml:"hijack_https"`
}

func (p *ProxyOption) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}

	switch value := v.(type) {
	case string:
		file, err := ioutil.ReadFile(value)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(file, &p); err != nil {
			return err
		}
		return nil
	case map[string]interface{}:
		if err := p.unmarshal(json.Unmarshal, v.(map[string]interface{})); err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid proxy")
	}
}

func (p *ProxyOption) UnmarshalYAML(node *yaml.Node) error {
	switch node.Kind {
	case yaml.ScalarNode:
		var path string
		if err := node.Decode(&path); err != nil {
			return err
		}

		file, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}
		if err := yaml.Unmarshal(file, &p); err != nil {
			return err
		}
		return nil
	case yaml.MappingNode:
		var m = make(map[string]interface{})
		for i := 0; i < len(node.Content); i += 2 {
			var (
				key   string
				value interface{}
			)
			if err := node.Content[i].Decode(&key); err != nil {
				return err
			}
			if err := node.Content[i+1].Decode(&value); err != nil {
				return err
			}
			m[key] = value
		}
		if err := p.unmarshal(yaml.Unmarshal, m); err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid proxy")
	}
}

func (p *ProxyOption) unmarshal(unmarshal func(in []byte, out interface{}) (err error), m map[string]interface{}) error {
	mb, err := jsonutils.MarshalMap(m)
	if err != nil {
		return err
	}

	// ListenOption
	if mb["listen_option"] != nil {
		var l ListenOption
		if err := unmarshal(mb["listen_option"], &l); err != nil {
			return err
		}
		p.ListenOption = l
	}

	// RegistryMirror
	if mb["registry_mirror"] != nil {
		var r *RegistryMirror
		if err := unmarshal(mb["registry_mirror"], &r); err != nil {
			return err
		}
		p.RegistryMirror = r
	}

	// Proxies
	if mb["proxies"] != nil {
		var ps []*Proxy
		if err := unmarshal(mb["proxies"], &ps); err != nil {
			return err
		}
		p.Proxies = ps
	}

	// HijackHTTPS
	if mb["hijack_https"] != nil {
		var h *HijackConfig
		if err := unmarshal(mb["hijack_https"], &h); err != nil {
			return err
		}
		p.HijackHTTPS = h
	}

	return nil
}

type UploadOption struct {
	ListenOption `yaml:",inline"`
	RateLimit    clientutil.RateLimit `json:"rate_limit" yaml:"rate_limit"`
}

type ListenOption struct {
	Security   SecurityOption    `json:"security" yaml:"security"`
	TCPListen  *TCPListenOption  `json:"tcp_listen,omitempty" yaml:"tcp_listen,omitempty"`
	UnixListen *UnixListenOption `json:"unix_listen,omitempty" yaml:"unix_listen,omitempty"`
}

type TCPListenOption struct {
	// Listen stands listen interface, like: 0.0.0.0, 192.168.0.1
	Listen string `json:"listen" yaml:"listen"`

	// PortRange stands listen port
	// yaml example 1:
	//   port: 12345
	// yaml example 2:
	//   port:
	//     start: 12345
	//     end: 12346
	PortRange TCPListenPortRange `json:"port" yaml:"port"`
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

func (f *FileString) UnmarshalYAML(node *yaml.Node) error {
	var s string
	switch node.Kind {
	case yaml.ScalarNode:
		if err := node.Decode(&s); err != nil {
			return err
		}
	default:
		return errors.New("invalid filestring")
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

// RegistryMirror configures the mirror of the official docker registry
type RegistryMirror struct {
	// Remote url for the registry mirror, default is https://index.docker.io
	Remote *URL `yaml:"url" json:"url"`

	// Optional certificates if the mirror uses self-signed certificates
	Certs *CertPool `yaml:"certs" json:"certs"`

	// Whether to ignore certificates errors for the registry
	Insecure bool `yaml:"insecure" json:"insecure"`

	// Request the remote registry directly.
	Direct bool `yaml:"direct" json:"direct"`
}

// TLSConfig returns the tls.Config used to communicate with the mirror.
func (r *RegistryMirror) TLSConfig() *tls.Config {
	if r == nil {
		return nil
	}
	cfg := &tls.Config{
		InsecureSkipVerify: r.Insecure,
	}
	if r.Certs != nil {
		cfg.RootCAs = r.Certs.CertPool
	}
	return cfg
}

// URL is simple wrapper around url.URL to make it unmarshallable from a string.
type URL struct {
	*url.URL
}

// UnmarshalJSON implements json.Unmarshaler.
func (u *URL) UnmarshalJSON(b []byte) error {
	return u.unmarshal(func(v interface{}) error { return json.Unmarshal(b, v) })
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (u *URL) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return u.unmarshal(unmarshal)
}

// MarshalJSON implements json.Marshaller to print the url.
func (u *URL) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.String())
}

// MarshalYAML implements yaml.Marshaller to print the url.
func (u *URL) MarshalYAML() (interface{}, error) {
	return u.String(), nil
}

func (u *URL) unmarshal(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}

	parsed, err := url.Parse(s)
	if err != nil {
		return err
	}

	u.URL = parsed
	return nil
}

// CertPool is a wrapper around x509.CertPool, which can be unmarshalled and
// constructed from a list of filenames.
type CertPool struct {
	Files []string
	*x509.CertPool
}

// UnmarshalJSON implements json.Unmarshaler.
func (cp *CertPool) UnmarshalJSON(b []byte) error {
	return cp.unmarshal(func(v interface{}) error { return json.Unmarshal(b, v) })
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (cp *CertPool) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return cp.unmarshal(unmarshal)
}

// MarshalJSON implements json.Marshaller to print the cert pool.
func (cp *CertPool) MarshalJSON() ([]byte, error) {
	return json.Marshal(cp.Files)
}

// MarshalYAML implements yaml.Marshaller to print the cert pool.
func (cp *CertPool) MarshalYAML() (interface{}, error) {
	return cp.Files, nil
}

func (cp *CertPool) unmarshal(unmarshal func(interface{}) error) error {
	var cf []FileString
	if err := unmarshal(&cf); err != nil {
		return err
	}

	pool := x509.NewCertPool()
	for _, cert := range cf {
		if !pool.AppendCertsFromPEM([]byte(cert)) {
			return errors.Errorf("invalid cert: %s", cert)
		}
	}

	cp.CertPool = pool
	return nil
}

// Proxy describes a regular expression matching rule for how to proxy a request.
type Proxy struct {
	Regx     *Regexp `yaml:"regx" json:"regx"`
	UseHTTPS bool    `yaml:"use_https" json:"use_https"`
	Direct   bool    `yaml:"direct" json:"direct"`

	// Redirect is the host to redirect to, if not empty
	Redirect string `yaml:"redirect" json:"redirect"`
}

func NewProxy(regx string, useHTTPS bool, direct bool, redirect string) (*Proxy, error) {
	exp, err := NewRegexp(regx)
	if err != nil {
		return nil, errors.Wrap(err, "invalid regexp")
	}

	return &Proxy{
		Regx:     exp,
		UseHTTPS: useHTTPS,
		Direct:   direct,
		Redirect: redirect,
	}, nil
}

// Match checks if the given url matches the rule.
func (r *Proxy) Match(url string) bool {
	return r.Regx != nil && r.Regx.MatchString(url)
}

// Regexp is a simple wrapper around regexp. Regexp to make it unmarshallable from a string.
type Regexp struct {
	*regexp.Regexp
}

// NewRegexp returns a new Regexp instance compiled from the given string.
func NewRegexp(exp string) (*Regexp, error) {
	r, err := regexp.Compile(exp)
	if err != nil {
		return nil, err
	}
	return &Regexp{r}, nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (r *Regexp) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return r.unmarshal(unmarshal)
}

// UnmarshalJSON implements json.Unmarshaler.
func (r *Regexp) UnmarshalJSON(b []byte) error {
	return r.unmarshal(func(v interface{}) error { return json.Unmarshal(b, v) })
}

func (r *Regexp) unmarshal(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	exp, err := regexp.Compile(s)
	if err == nil {
		r.Regexp = exp
	}
	return err
}

// MarshalJSON implements json.Marshaller to print the regexp.
func (r *Regexp) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.String())
}

// MarshalYAML implements yaml.Marshaller to print the regexp.
func (r *Regexp) MarshalYAML() (interface{}, error) {
	return r.String(), nil
}

// HijackConfig represents how dfdaemon hijacks http requests.
type HijackConfig struct {
	Cert  string        `yaml:"cert" json:"cert"`
	Key   string        `yaml:"key" json:"key"`
	Hosts []*HijackHost `yaml:"hosts" json:"hosts"`
}

// HijackHost is a hijack rule for the hosts that matches Regx.
type HijackHost struct {
	Regx     *Regexp   `yaml:"regx" json:"regx"`
	Insecure bool      `yaml:"insecure" json:"insecure"`
	Certs    *CertPool `yaml:"certs" json:"certs"`
}
