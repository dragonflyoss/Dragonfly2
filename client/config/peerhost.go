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
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/cmd/dependency/base"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

type DaemonConfig = DaemonOption
type DaemonOption struct {
	base.Options `yaml:",inline" mapstructure:",squash"`
	// AliveTime indicates alive duration for which daemon keeps no accessing by any uploading and download requests,
	// after this period daemon will automatically exit
	// when AliveTime == 0, will run infinitely
	AliveTime  clientutil.Duration `mapstructure:"aliveTime" yaml:"aliveTime"`
	GCInterval clientutil.Duration `mapstructure:"gcInterval" yaml:"gcInterval"`
	Metrics    string              `yaml:"metrics" mapstructure:"metrics"`

	WorkHome    string `mapstructure:"workHome" yaml:"workHome"`
	CacheDir    string `mapstructure:"cacheDir" yaml:"cacheDir"`
	LogDir      string `mapstructure:"logDir" yaml:"logDir"`
	DataDir     string `mapstructure:"dataDir" yaml:"dataDir"`
	KeepStorage bool   `mapstructure:"keepStorage" yaml:"keepStorage"`

	Scheduler SchedulerOption `mapstructure:"scheduler" yaml:"scheduler"`
	Host      HostOption      `mapstructure:"host" yaml:"host"`
	Download  DownloadOption  `mapstructure:"download" yaml:"download"`
	Proxy     *ProxyOption    `mapstructure:"proxy" yaml:"proxy"`
	Upload    UploadOption    `mapstructure:"upload" yaml:"upload"`
	Storage   StorageOption   `mapstructure:"storage" yaml:"storage"`
	Health    *HealthOption   `mapstructure:"health" yaml:"health"`
	// TODO WIP, did not use
	Reload ReloadOption `mapstructure:"reloadOption" yaml:"reloadOption"`
}

func NewDaemonConfig() *DaemonOption {
	return &peerHostConfig
}

func (p *DaemonOption) Load(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("unable to load peer host configuration from %q [%v]", path, err)
	}

	switch filepath.Ext(path) {
	case ".json":
		err := json.Unmarshal(data, p)
		if err != nil {
			return err
		}
	case ".yml", ".yaml":
		err := yaml.Unmarshal(data, p)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("extension of %s is not in 'yml/yaml/json'", path)
	}

	return nil
}

func (p *DaemonOption) Convert() error {
	// AdvertiseIP
	ip := net.ParseIP(p.Host.AdvertiseIP)
	if ip == nil || net.IPv4zero.Equal(ip) {
		p.Host.AdvertiseIP = iputils.IPv4
	} else {
		p.Host.AdvertiseIP = ip.String()
	}

	// ScheduleTimeout should not great then AliveTime
	if p.AliveTime.Duration > 0 && p.Scheduler.ScheduleTimeout.Duration > p.AliveTime.Duration {
		p.Scheduler.ScheduleTimeout.Duration = p.AliveTime.Duration - time.Second
	}

	return nil
}

func (p *DaemonOption) Validate() error {
	if p.Scheduler.Manager.Enable {
		if len(p.Scheduler.Manager.NetAddrs) == 0 {
			return errors.New("manager addr is not specified")
		}

		if p.Scheduler.Manager.RefreshInterval == 0 {
			return errors.New("manager refreshInterval is not specified")
		}
		return nil
	}
	if len(p.Scheduler.NetAddrs) == 0 {
		return errors.New("empty schedulers and config server is not specified")
	}
	return nil
}

type SchedulerOption struct {
	// Manager is to get the scheduler configuration remotely
	Manager ManagerOption `mapstructure:"manager" yaml:"manager"`
	// NetAddrs is scheduler addresses.
	NetAddrs []dfnet.NetAddr `mapstructure:"netAddrs" yaml:"netAddrs"`
	// ScheduleTimeout is request timeout.
	ScheduleTimeout clientutil.Duration `mapstructure:"scheduleTimeout" yaml:"scheduleTimeout"`
	// DisableAutoBackSource indicates not back source normally, only scheduler says back source
	DisableAutoBackSource bool `mapstructure:"disableAutoBackSource" yaml:"disableAutoBackSource"`
}

type ManagerOption struct {
	// Enable get configuration from manager
	Enable bool `mapstructure:"enable" yaml:"enable"`
	// NetAddrs is manager addresses.
	NetAddrs []dfnet.NetAddr `mapstructure:"netAddrs" yaml:"netAddrs"`
	// RefreshInterval is the refresh interval
	RefreshInterval time.Duration `mapstructure:"refreshInterval" yaml:"refreshInterval"`
}

type HostOption struct {
	// SecurityDomain is the security domain
	SecurityDomain string `mapstructure:"securityDomain" yaml:"securityDomain"`
	// Location for scheduler
	Location string `mapstructure:"location" yaml:"location"`
	// IDC for scheduler
	IDC string `mapstructure:"idc" yaml:"idc"`
	// Peerhost net topology for scheduler
	NetTopology string `mapstructure:"netTopology" yaml:"netTopology"`
	// Hostname is daemon host name
	Hostname string `mapstructure:"hostname" yaml:"hostname"`
	// The listen ip for all tcp services of daemon
	ListenIP string `mapstructure:"listenIP" yaml:"listenIP"`
	// The ip report to scheduler, normal same with listen ip
	AdvertiseIP string `mapstructure:"advertiseIP" yaml:"advertiseIP"`
}

type DownloadOption struct {
	TotalRateLimit       clientutil.RateLimit `mapstructure:"totalRateLimit" yaml:"totalRateLimit"`
	PerPeerRateLimit     clientutil.RateLimit `mapstructure:"perPeerRateLimit" yaml:"perPeerRateLimit"`
	PieceDownloadTimeout time.Duration        `mapstructure:"pieceDownloadTimeout" yaml:"pieceDownloadTimeout"`
	DownloadGRPC         ListenOption         `mapstructure:"downloadGRPC" yaml:"downloadGRPC"`
	PeerGRPC             ListenOption         `mapstructure:"peerGRPC" yaml:"peerGRPC"`
	CalculateDigest      bool                 `mapstructure:"calculateDigest" yaml:"calculateDigest"`
	TransportOption      *TransportOption     `mapstructure:"transportOption" yaml:"transportOption"`
	GetPiecesMaxRetry    int                  `mapstructure:"getPiecesMaxRetry" yaml:"getPiecesMaxRetry"`
	Prefetch             bool                 `mapstructure:"prefetch" yaml:"prefetch"`
}

type TransportOption struct {
	DialTimeout           time.Duration `mapstructure:"dialTimeout" yaml:"dialTimeout"`
	KeepAlive             time.Duration `mapstructure:"keepAlive" yaml:"keepAlive"`
	MaxIdleConns          int           `mapstructure:"maxIdleConns" yaml:"maxIdleConns"`
	IdleConnTimeout       time.Duration `mapstructure:"idleConnTimeout" yaml:"idleConnTimeout"`
	ResponseHeaderTimeout time.Duration `mapstructure:"responseHeaderTimeout" yaml:"responseHeaderTimeout"`
	TLSHandshakeTimeout   time.Duration `mapstructure:"tlsHandshakeTimeout" yaml:"tlsHandshakeTimeout"`
	ExpectContinueTimeout time.Duration `mapstructure:"expectContinueTimeout" yaml:"expectContinueTimeout"`
}

type ProxyOption struct {
	// WARNING: when add more option, please update ProxyOption.unmarshal function
	ListenOption    `mapstructure:",squash" yaml:",inline"`
	BasicAuth       *BasicAuth      `mapstructure:"basicAuth" yaml:"basicAuth"`
	DefaultFilter   string          `mapstructure:"defaultFilter" yaml:"defaultFilter"`
	MaxConcurrency  int64           `mapstructure:"maxConcurrency" yaml:"maxConcurrency"`
	RegistryMirror  *RegistryMirror `mapstructure:"registryMirror" yaml:"registryMirror"`
	WhiteList       []*WhiteList    `mapstructure:"whiteList" yaml:"whiteList"`
	Proxies         []*ProxyRule    `mapstructure:"proxies" yaml:"proxies"`
	HijackHTTPS     *HijackConfig   `mapstructure:"hijackHTTPS" yaml:"hijackHTTPS"`
	DumpHTTPContent bool            `mapstructure:"dumpHTTPContent" yaml:"dumpHTTPContent"`
	// ExtraRegistryMirrors add more mirror for different ports
	ExtraRegistryMirrors []*RegistryMirror `mapstructure:"extraRegistryMirrors" yaml:"extraRegistryMirrors"`
}

func (p *ProxyOption) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}

	switch value := v.(type) {
	case string:
		file, err := os.ReadFile(value)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(file, p); err != nil {
			return err
		}
		return nil
	case map[string]interface{}:
		if err := p.unmarshal(json.Unmarshal, b); err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid proxy option")
	}
}

func (p *ProxyOption) UnmarshalYAML(node *yaml.Node) error {
	switch node.Kind {
	case yaml.ScalarNode:
		var path string
		if err := node.Decode(&path); err != nil {
			return err
		}

		file, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := yaml.Unmarshal(file, p); err != nil {
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

		b, err := yaml.Marshal(m)
		if err != nil {
			return err
		}

		if err := p.unmarshal(yaml.Unmarshal, b); err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid proxy")
	}
}

func (p *ProxyOption) unmarshal(unmarshal func(in []byte, out interface{}) (err error), b []byte) error {
	pt := struct {
		ListenOption    `mapstructure:",squash" yaml:",inline"`
		BasicAuth       *BasicAuth      `mapstructure:"basicAuth" yaml:"basicAuth"`
		DefaultFilter   string          `mapstructure:"defaultFilter" yaml:"defaultFilter"`
		MaxConcurrency  int64           `mapstructure:"maxConcurrency" yaml:"maxConcurrency"`
		RegistryMirror  *RegistryMirror `mapstructure:"registryMirror" yaml:"registryMirror"`
		WhiteList       []*WhiteList    `mapstructure:"whiteList" yaml:"whiteList"`
		Proxies         []*ProxyRule    `mapstructure:"proxies" yaml:"proxies"`
		HijackHTTPS     *HijackConfig   `mapstructure:"hijackHTTPS" yaml:"hijackHTTPS"`
		DumpHTTPContent bool            `mapstructure:"dumpHTTPContent" yaml:"dumpHTTPContent"`
	}{}

	if err := unmarshal(b, &pt); err != nil {
		return err
	}

	p.ListenOption = pt.ListenOption
	p.RegistryMirror = pt.RegistryMirror
	p.Proxies = pt.Proxies
	p.HijackHTTPS = pt.HijackHTTPS
	p.WhiteList = pt.WhiteList
	p.MaxConcurrency = pt.MaxConcurrency
	p.DefaultFilter = pt.DefaultFilter
	p.BasicAuth = pt.BasicAuth
	p.DumpHTTPContent = pt.DumpHTTPContent

	return nil
}

type UploadOption struct {
	ListenOption `yaml:",inline" mapstructure:",squash"`
	RateLimit    clientutil.RateLimit `mapstructure:"rateLimit" yaml:"rateLimit"`
}

type ListenOption struct {
	Security   SecurityOption    `mapstructure:"security" yaml:"security"`
	TCPListen  *TCPListenOption  `mapstructure:"tcpListen,omitempty" yaml:"tcpListen,omitempty"`
	UnixListen *UnixListenOption `mapstructure:"unixListen,omitempty" yaml:"unixListen,omitempty"`
}

type TCPListenOption struct {
	// Listen stands listen interface, like: 0.0.0.0, 192.168.0.1
	Listen string `mapstructure:"listen" yaml:"listen"`

	// PortRange stands listen port
	// yaml example 1:
	//   port: 12345
	// yaml example 2:
	//   port:
	//     start: 12345
	//     end: 12346
	PortRange TCPListenPortRange `mapstructure:"port" yaml:"port"`

	// Namespace stands the linux net namespace, like /proc/1/ns/net
	// It's useful for running daemon in pod with ip allocated and listen in host
	Namespace string `mapstructure:"namespace" yaml:"namespace"`
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
	Socket string `mapstructure:"socket" yaml:"socket"`
}

type SecurityOption struct {
	// Insecure indicate enable tls or not
	Insecure  bool        `mapstructure:"insecure" yaml:"insecure"`
	CACert    string      `mapstructure:"caCert" yaml:"caCert"`
	Cert      string      `mapstructure:"cert" yaml:"cert"`
	Key       string      `mapstructure:"key" yaml:"key"`
	TLSConfig *tls.Config `mapstructure:"tlsConfig" yaml:"tlsConfig"`
}

type StorageOption struct {
	// DataPath indicates directory which stores temporary files for p2p uploading
	DataPath string `mapstructure:"dataPath" yaml:"dataPath"`
	// TaskExpireTime indicates caching duration for which cached file keeps no accessed by any process,
	// after this period cache file will be gc
	TaskExpireTime clientutil.Duration `mapstructure:"taskExpireTime" yaml:"taskExpireTime"`
	// DiskGCThreshold indicates the threshold to gc the oldest tasks
	DiskGCThreshold unit.Bytes `mapstructure:"diskGCThreshold" yaml:"diskGCThreshold"`
	// DiskGCThresholdPercent indicates the threshold to gc the oldest tasks according the disk usage
	// Eg, DiskGCThresholdPercent=80, when the disk usage is above 80%, start to gc the oldest tasks
	DiskGCThresholdPercent float64 `mapstructure:"diskGCThresholdPercent" yaml:"diskGCThresholdPercent"`
	// Multiplex indicates reusing underlying storage for same task id
	Multiplex     bool          `mapstructure:"multiplex" yaml:"multiplex"`
	StoreStrategy StoreStrategy `mapstructure:"strategy" yaml:"strategy"`
}

type StoreStrategy string

type HealthOption struct {
	ListenOption `yaml:",inline" mapstructure:",squash"`
	Path         string `mapstructure:"path" yaml:"pash"`
}

type ReloadOption struct {
	Interval clientutil.Duration
}

type FileString string

func (f *FileString) UnmarshalJSON(b []byte) error {
	var s string
	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}

	file, err := os.ReadFile(s)
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

	file, err := os.ReadFile(s)
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
	CACert FileString `json:"caCert"`
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
	Remote *URL `yaml:"url" mapstructure:"url"`

	// DynamicRemote indicates using header "X-Dragonfly-Registry" for remote instead of Remote
	// if header "X-Dragonfly-Registry" does not exist, use Remote by default
	DynamicRemote bool `yaml:"dynamic" mapstructure:"dynamic"`

	// Optional certificates if the mirror uses self-signed certificates
	Certs *CertPool `yaml:"certs" mapstructure:"certs"`

	// Whether to ignore certificates errors for the registry
	Insecure bool `yaml:"insecure" mapstructure:"insecure"`

	// Request the remote registry directly.
	Direct bool `yaml:"direct" mapstructure:"direct"`

	// Whether to use proxies to decide when to use dragonfly
	UseProxies bool `yaml:"useProxies" mapstructure:"useProxies"`
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
	if err := unmarshal(&cp.Files); err != nil {
		return err
	}

	pool, err := certPoolFromFiles(cp.Files...)
	if err != nil {
		return err
	}

	cp.CertPool = pool
	return nil
}

// certPoolFromFiles returns an *x509.CertPool constructed from the given files.
// If no files are given, (nil, nil) will be returned.
func certPoolFromFiles(files ...string) (*x509.CertPool, error) {
	if len(files) == 0 {
		return nil, nil
	}

	roots := x509.NewCertPool()
	for _, f := range files {
		cert, err := os.ReadFile(f)
		if err != nil {
			return nil, errors.Wrapf(err, "read cert file %s", f)
		}
		if !roots.AppendCertsFromPEM(cert) {
			return nil, errors.Errorf("invalid cert: %s", f)
		}
	}
	return roots, nil
}

// ProxyRule describes a regular expression matching rule for how to proxy a request.
type ProxyRule struct {
	Regx     *Regexp `yaml:"regx" mapstructure:"regx"`
	UseHTTPS bool    `yaml:"useHTTPS" mapstructure:"useHTTPS"`
	Direct   bool    `yaml:"direct" mapstructure:"direct"`

	// Redirect is the host to redirect to, if not empty
	Redirect string `yaml:"redirect" mapstructure:"redirect"`
}

func NewProxyRule(regx string, useHTTPS bool, direct bool, redirect string) (*ProxyRule, error) {
	exp, err := NewRegexp(regx)
	if err != nil {
		return nil, errors.Wrap(err, "invalid regexp")
	}

	return &ProxyRule{
		Regx:     exp,
		UseHTTPS: useHTTPS,
		Direct:   direct,
		Redirect: redirect,
	}, nil
}

// Match checks if the given url matches the rule.
func (r *ProxyRule) Match(url string) bool {
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
	Cert  string             `yaml:"cert" mapstructure:"cert"`
	Key   string             `yaml:"key" mapstructure:"key"`
	Hosts []*HijackHost      `yaml:"hosts" mapstructure:"hosts"`
	SNI   []*TCPListenOption `yaml:"sni" mapstructure:"sni"`
}

// HijackHost is a hijack rule for the hosts that matches Regx.
type HijackHost struct {
	Regx     *Regexp   `yaml:"regx" mapstructure:"regx"`
	Insecure bool      `yaml:"insecure" mapstructure:"insecure"`
	Certs    *CertPool `yaml:"certs" mapstructure:"certs"`
}

// TelemetryOption is the option for telemetry
type TelemetryOption struct {
	Jaeger string `yaml:"jaeger" mapstructure:"jaeger"`
}

type WhiteList struct {
	Host  string   `yaml:"host" mapstructure:"host"`
	Regx  *Regexp  `yaml:"regx" mapstructure:"regx"`
	Ports []string `yaml:"ports" mapstructure:"ports"`
}

type BasicAuth struct {
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
}
