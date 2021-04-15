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
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/errors"

	"d7y.io/dragonfly/v2/pkg/basic"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"d7y.io/dragonfly/v2/pkg/util/net/urlutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
)

// ClientOption holds all the runtime config information.
type ClientOption struct {
	// URL download URL.
	URL string `json:"url"`

	// Lock file location
	LockFile string `json:"lock_file" yaml:"lock_file"`

	// Output full output path.
	Output string `json:"output"`

	// Timeout download timeout(second).
	Timeout time.Duration `json:"timeout,omitempty"`

	// Md5 expected file md5.
	// Deprecated: Md5 is deprecated, use DigestMethod with DigestValue instead
	Md5 string `json:"md5,omitempty"`

	// DigestMethod indicates digest method, like md5, sha256
	DigestMethod string `json:"digest_method,omitempty"`

	// DigestValue indicates digest value
	DigestValue string `json:"digest_value,omitempty"`

	// Identifier identify download task, it is available merely when md5 param not exist.
	Identifier string `json:"identifier,omitempty"`

	// CallSystem system name that executes dfget.
	CallSystem string `json:"call_system,omitempty"`

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
	NotBackSource bool `json:"not_back_source,omitempty"`

	// Insecure indicates whether skip secure verify when supernode interact with the source.
	Insecure bool `json:"insecure,omitempty"`

	// ShowBar shows progress bar, it's conflict with `--console`.
	ShowBar bool `json:"show_bar,omitempty"`

	// Console shows log on console, it's conflict with `--showbar`.
	Console bool `json:"console,omitempty" yaml:"console,omitempty"`

	// Verbose indicates whether to be verbose.
	// If set true, log level will be 'debug'.
	Verbose bool `json:"verbose,omitempty"`

	// Config file paths,
	// default:["/etc/dragonfly/dfget.yaml","/etc/dragonfly.conf"].
	//
	// NOTE: It is recommended to use `/etc/dragonfly/dfget.yaml` as default,
	// and the `/etc/dragonfly.conf` is just to ensure compatibility with previous versions.
	//ConfigFiles []string `json:"-"`

	// MoreDaemonOptions indicates more options passed to daemon by command line.
	MoreDaemonOptions string `json:"more_daemon_options,omitempty"`
}

func (cfg *ClientOption) Validate() error {
	if cfg == nil {
		return errors.Wrap(dferrors.ErrInvalidArgument, "runtime config")
	}

	if !urlutils.IsValidURL(cfg.URL) {
		return errors.Wrapf(dferrors.ErrInvalidArgument, "url: %v", cfg.URL)
	}

	if err := cfg.checkOutput(); err != nil {
		return errors.Wrapf(dferrors.ErrInvalidArgument, "output: %v", err)
	}

	return nil
}

func (cfg *ClientOption) String() string {
	js, _ := json.Marshal(cfg)
	return string(js)
}

// This function must be called after checkURL
func (cfg *ClientOption) checkOutput() error {
	if stringutils.IsBlank(cfg.Output) {
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
	for dir := cfg.Output; !stringutils.IsBlank(dir); dir = filepath.Dir(dir) {
		if err := syscall.Access(dir, syscall.O_RDWR); err == nil {
			break
		} else if os.IsPermission(err) || dir == "/" {
			return fmt.Errorf("user[%s] path[%s] %v", basic.Username, cfg.Output, err)
		}
	}
	return nil
}
