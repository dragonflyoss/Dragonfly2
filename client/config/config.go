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
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/errors"

	"github.com/dragonflyoss/Dragonfly2/pkg/dferrors"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/stringutils"
)

// ClientConfig holds all the runtime config information.
type ClientConfig struct {
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
