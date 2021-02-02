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

// Package config holds all DaemonConfig of dfget.
package config

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/docker/go-units"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"
	"gopkg.in/yaml.v3"

	"github.com/dragonflyoss/Dragonfly2/client/daemon/storage"
	"github.com/dragonflyoss/Dragonfly2/pkg/basic/dfnet"
	"github.com/dragonflyoss/Dragonfly2/pkg/dferrors"
	drate "github.com/dragonflyoss/Dragonfly2/pkg/rate"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/stringutils"
)

// DaemonConfig holds all configurable config of daemon.
type DaemonConfig struct {
	// DataExpireTime specifies the caching duration for which
	// cached files keep no accessed by any process.
	// After this period, the cached files will be deleted.
	DataExpireTime time.Duration

	// DaemonAliveTime specifies the alive duration for which
	// uploader keeps no accessing by any uploading requests.
	// After this period, the uploader will automatically exit.
	DaemonAliveTime time.Duration

	// Schedulers specify schedulers.
	// E.g. ["192.168.33.21:8002", "192.168.33.22:8002"]
	Schedulers []string `yaml:"schedulers,omitempty" json:"schedulers,omitempty"`

	// LocalLimit rate limit about a single download task, format: G(B)/g/M(B)/m/K(B)/k/B
	// pure number will also be parsed as Byte.
	LocalLimit drate.Rate `yaml:"local_limit,omitempty" json:"local_limit,omitempty"`

	// Minimal rate about a single download task, format: G(B)/g/M(B)/m/K(B)/k/B
	// pure number will also be parsed as Byte.
	MinRate drate.Rate `yaml:"min_rate,omitempty" json:"min_rate,omitempty"`

	// TotalLimit rate limit about the whole host, format: G(B)/g/M(B)/m/K(B)/k/B
	// pure number will also be parsed as Byte.
	TotalLimit drate.Rate `yaml:"total_limit,omitempty" json:"total_limit,omitempty"`

	// WorkHome work home path,
	// default: `$HOME/.small-dragonfly`.
	WorkHome string `yaml:"work_home" json:"work_home,omitempty"`
}

// ClientConfig holds all the runtime config information.
type ClientConfig struct {
	// Embedded GlobalConfig holds all configurable properties.
	DaemonConfig

	// URL download URL.
	URL string `json:"url"`

	// Output full output path.
	Output string `json:"output"`

	// Timeout download timeout(second).
	Timeout time.Duration `json:"timeout,omitempty"`

	// Md5 expected file md5.
	// Deprecated: Md5 is deprecated, use DigestMethod with DigestValue instead
	Md5 string `json:"md5,omitempty"`

	// DigestMethod indicates digest method, like md5, sha256
	DigestMethod string `json:"digestMethod,omitempty"`

	// DigestValue indicates digest value
	DigestValue string `json:"digestValue,omitempty"`

	// Identifier identify download task, it is available merely when md5 param not exist.
	Identifier string `json:"identifier,omitempty"`

	// CallSystem system name that executes dfget.
	CallSystem string `json:"callSystem,omitempty"`

	// Pattern download pattern, must be 'p2p' or 'cdn' or 'source',
	// default:`p2p`.
	Pattern string `json:"pattern,omitempty"`

	// CA certificate to verify when supernode interact with the source.
	Cacerts []string `json:"cacert,omitempty"`

	// Filter filter some query params of url, use char '&' to separate different params.
	// eg: -f 'key&sign' will filter 'key' and 'sign' query param.
	// in this way, different urls correspond one same download task that can use p2p mode.
	Filter []string `json:"filter,omitempty"`

	// Header of http request.
	// eg: --header='Accept: *' --header='Host: abc'.
	Header []string `json:"header,omitempty"`

	// NotBackSource indicates whether to not back source to download when p2p fails.
	NotBackSource bool `json:"notBackSource,omitempty"`

	// Insecure indicates whether skip secure verify when supernode interact with the source.
	Insecure bool `json:"insecure,omitempty"`

	// ShowBar shows progress bar, it's conflict with `--console`.
	ShowBar bool `json:"showBar,omitempty"`

	// Console shows log on console, it's conflict with `--showbar`.
	Console bool `json:"console,omitempty"`

	// Verbose indicates whether to be verbose.
	// If set true, log level will be 'debug'.
	Verbose bool `json:"verbose,omitempty"`

	// Username of the system currently logged in.
	User string `json:"-"`

	// Config file paths,
	// default:["/etc/dragonfly/dfget.yml","/etc/dragonfly.conf"].
	//
	// NOTE: It is recommended to use `/etc/dragonfly/dfget.yml` as default,
	// and the `/etc/dragonfly.conf` is just to ensure compatibility with previous versions.
	//ConfigFiles []string `json:"-"`
}

func (cfg *ClientConfig) String() string {
	js, _ := json.Marshal(cfg)
	return string(js)
}

// NewClientConfig creates and initializes a ClientConfig.
func NewClientConfig() *ClientConfig {
	return &ClientConfig{
		DaemonConfig:  DaemonConfig{},
		URL:           "",
		Output:        "",
		Timeout:       0,
		Md5:           "",
		DigestMethod:  "",
		DigestValue:   "",
		Identifier:    "",
		CallSystem:    "",
		Pattern:       "",
		Cacerts:       nil,
		Filter:        nil,
		Header:        nil,
		NotBackSource: false,
		Insecure:      false,
		ShowBar:       false,
		Console:       false,
		Verbose:       false,
	}
}

// CheckConfig checks the config and return errors.
func CheckConfig(cfg *ClientConfig) (err error) {
	if cfg == nil {
		return errors.Wrap(dferrors.ErrInvalidArgument, "runtime config")
	}

	if !IsValidURL(cfg.URL) {
		return errors.Wrapf(dferrors.ErrInvalidArgument, "url: %v", cfg.URL)
	}

	if err = checkOutput(cfg); err != nil {
		return errors.Wrapf(dferrors.ErrInvalidArgument, "output: %v", err)
	}
	return nil
}

// IsValidURL returns whether the string url is a valid HTTP URL.
func IsValidURL(urlStr string) bool {
	u, err := url.Parse(urlStr)
	if err != nil {
		return false
	}
	if len(u.Host) == 0 || len(u.Scheme) == 0 {
		return false
	}
	return true
}

// This function must be called after checkURL
func checkOutput(cfg *ClientConfig) error {
	if stringutils.IsEmptyStr(cfg.Output) {
		url := strings.TrimRight(cfg.URL, "/")
		idx := strings.LastIndexByte(url, '/')
		if idx < 0 {
			return fmt.Errorf("get output from url[%s] error", cfg.URL)
		}
		cfg.Output = url[idx+1:]
	}

	if !filepath.IsAbs(cfg.Output) {
		absPath, err := filepath.Abs(cfg.Output)
		if err != nil {
			return fmt.Errorf("get absolute path[%s] error: %v", cfg.Output, err)
		}
		cfg.Output = absPath
	}

	if f, err := os.Stat(cfg.Output); err == nil && f.IsDir() {
		return fmt.Errorf("path[%s] is directory but requires file path", cfg.Output)
	}

	// check permission
	for dir := cfg.Output; !stringutils.IsEmptyStr(dir); dir = filepath.Dir(dir) {
		if err := syscall.Access(dir, syscall.O_RDWR); err == nil {
			break
		} else if os.IsPermission(err) || dir == "/" {
			return fmt.Errorf("user[%s] path[%s] %v", cfg.User, cfg.Output, err)
		}
	}
	return nil
}

// RuntimeVariable stores the variables that are initialized and used
// at downloading task executing.
type DeprecatedRuntimeVariable struct {
	// MetaPath specify the path of meta file which store the meta info of the peer that should be persisted.
	// Only server port information is stored currently.
	MetaPath string

	// SystemDataDir specifies a default directory to store temporary files.
	SystemDataDir string

	// DataDir specifies a directory to store temporary files.
	// For now, the value of `DataDir` always equals `SystemDataDir`,
	// and there is no difference between them.
	// TODO: If there is insufficient disk space, we should set it to the `TargetDir`.
	DataDir string

	// RealTarget specifies the full target path whose value is equal to the `Output`.
	RealTarget string

	// TargetDir is the directory of the RealTarget path.
	TargetDir string

	// TempTarget is a temp file path that try to determine
	// whether the `TargetDir` and the `DataDir` belong to the same disk by making a hard link.
	TempTarget string

	// TaskURL is generated from rawURL which may contains some queries or parameter.
	// Dfget will filter some volatile queries such as timestamps via --filter parameter of dfget.
	TaskURL string

	// TaskFileName is a string composed of `the last element of RealTarget path + "-" + sign`.
	TaskFileName string

	// LocalIP is the native IP which can connect supernode successfully.
	LocalIP string

	// PeerPort is the TCP port on which the file upload service listens as a peer node.
	PeerPort int

	// FileLength the length of the file to download.
	FileLength int64
}

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
	RegistryMirror *RegistryMirror `json:"registry_mirror" yaml:"registry_mirror"`
	Proxies        []*Proxy        `json:"proxies" yaml:"proxies"`
	HijackHTTPS    *HijackConfig   `json:"hijack_https" yaml:"hijack_https"`
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

// UnmarshalJSON implements json.Unmarshaller.
func (u *URL) UnmarshalJSON(b []byte) error {
	return u.unmarshal(func(v interface{}) error { return json.Unmarshal(b, v) })
}

// UnmarshalYAML implements yaml.Unmarshaller.
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

// UnmarshalJSON implements json.Unmarshaller.
func (cp *CertPool) UnmarshalJSON(b []byte) error {
	return cp.unmarshal(func(v interface{}) error { return json.Unmarshal(b, v) })
}

// UnmarshalYAML implements yaml.Unmarshaller.
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

// UnmarshalYAML implements yaml.Unmarshaller.
func (r *Regexp) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return r.unmarshal(unmarshal)
}

// UnmarshalJSON implements json.Unmarshaller.
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
	Regx     *regexp.Regexp `yaml:"regx" json:"regx"`
	Insecure bool           `yaml:"insecure" json:"insecure"`
	Certs    *CertPool      `yaml:"certs" json:"certs"`
}
