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
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/afero"

	"github.com/dragonflyoss/Dragonfly2/pkg/dferrors"
	"github.com/dragonflyoss/Dragonfly2/pkg/rate"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/fileutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/stringutils"
)

var fs = afero.NewOsFs()

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
	LocalLimit rate.Rate `yaml:"local_limit,omitempty" json:"local_limit,omitempty"`

	// Minimal rate about a single download task, format: G(B)/g/M(B)/m/K(B)/k/B
	// pure number will also be parsed as Byte.
	MinRate rate.Rate `yaml:"min_rate,omitempty" json:"min_rate,omitempty"`

	// TotalLimit rate limit about the whole host, format: G(B)/g/M(B)/m/K(B)/k/B
	// pure number will also be parsed as Byte.
	TotalLimit rate.Rate `yaml:"total_limit,omitempty" json:"total_limit,omitempty"`

	// WorkHome work home path,
	// default: `$HOME/.small-dragonfly`.
	WorkHome string `yaml:"work_home" json:"work_home,omitempty"`

	// Registry mirror settings
	RegistryMirror *RegistryMirror `yaml:"registry_mirror" json:"registry_mirror"`

	// Proxies is the list of rules for the transparent proxy. If no rules
	// are provided, all requests will be proxied directly. Request will be
	// proxied with the first matching rule.
	Proxies []*Proxy `yaml:"proxies" json:"proxies"`

	// HijackHTTPS is the list of hosts whose https requests should be hijacked
	// by dfdaemon. Dfdaemon will be able to proxy requests from them with dfget
	// if the url matches the proxy rules. The first matched rule will be used.
	HijackHTTPS *HijackConfig `yaml:"hijack_https" json:"hijack_https"`
}

// NewDaemonConfig creates a new DaemonConfig with default values.
func NewDaemonConfig() *DaemonConfig {
	// don't set Supernodes as default value, the SupernodeLocator will
	// do this in a better way.
	return &DaemonConfig{
		LocalLimit: DefaultLocalLimit,
		MinRate:    DefaultMinRate,
	}
}

func (p *DaemonConfig) String() string {
	str, _ := json.Marshal(p)
	return string(str)
}

// Load loads properties from config file.
func (p *DaemonConfig) Load(path string) error {
	switch p.fileType(path) {
	case "json":
		panic("TODO")
	case "yaml":
		return fileutils.LoadYaml(path, p)
	}
	return fmt.Errorf("extension of %s is not in 'json/yaml/yml'", path)
}

func (p *DaemonConfig) fileType(path string) string {
	ext := filepath.Ext(path)
	switch v := strings.ToLower(ext); v {
	case ".json":
		return "json"
	case ".yaml", ".yml":
		return "yaml"
	default:
		return v
	}
}

// RegistryMirror configures the mirror of the official docker registry
type RegistryMirror struct {
	// Remote url for the registry mirror, default is https://index.docker.io
	Remote *URL `yaml:"remote" json:"remote"`

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

// URL is simple wrapper around url.URL to make it unmarshallable from a string.
type URL struct {
	*url.URL
}

// NewURL parses url from the given string.
func NewURL(s string) (*URL, error) {
	u, err := url.Parse(s)
	if err != nil {
		return nil, err
	}

	return &URL{u}, nil
}

// UnmarshalYAML implements yaml.Unmarshaller.
func (u *URL) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return u.unmarshal(unmarshal)
}

// UnmarshalJSON implements json.Unmarshaller.
func (u *URL) UnmarshalJSON(b []byte) error {
	return u.unmarshal(func(v interface{}) error { return json.Unmarshal(b, v) })
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

// MarshalJSON implements json.Marshaller to print the url.
func (u *URL) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.String())
}

// MarshalYAML implements yaml.Marshaller to print the url.
func (u *URL) MarshalYAML() (interface{}, error) {
	return u.String(), nil
}

// CertPool is a wrapper around x509.CertPool, which can be unmarshalled and
// constructed from a list of filenames.
type CertPool struct {
	Files []string
	*x509.CertPool
}

// UnmarshalYAML implements yaml.Unmarshaller.
func (cp *CertPool) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return cp.unmarshal(unmarshal)
}

// UnmarshalJSON implements json.Unmarshaller.
func (cp *CertPool) UnmarshalJSON(b []byte) error {
	return cp.unmarshal(func(v interface{}) error { return json.Unmarshal(b, v) })
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

// MarshalJSON implements json.Marshaller to print the cert pool.
func (cp *CertPool) MarshalJSON() ([]byte, error) {
	return json.Marshal(cp.Files)
}

// MarshalYAML implements yaml.Marshaller to print the cert pool.
func (cp *CertPool) MarshalYAML() (interface{}, error) {
	return cp.Files, nil
}

// certPoolFromFiles returns an *x509.CertPool constructed from the given files.
// If no files are given, (nil, nil) will be returned.
func certPoolFromFiles(files ...string) (*x509.CertPool, error) {
	if len(files) == 0 {
		return nil, nil
	}

	roots := x509.NewCertPool()
	for _, f := range files {
		cert, err := afero.ReadFile(fs, f)
		if err != nil {
			return nil, errors.Wrapf(err, "read cert file %s", f)
		}
		if !roots.AppendCertsFromPEM(cert) {
			return nil, errors.Errorf("invalid cert: %s", f)
		}
	}
	return roots, nil
}

// Proxy describes a regular expression matching rule for how to proxy a request.
type Proxy struct {
	Regx     *Regexp `yaml:"regx" json:"regx"`
	UseHTTPS bool    `yaml:"use_https" json:"use_https"`
	Direct   bool    `yaml:"direct" json:"direct"`
	// Redirect is the host to redirect to, if not empty
	Redirect string `yaml:"redirect" json:"redirect"`
}

// NewProxy returns a new proxy rule with given attributes.
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
