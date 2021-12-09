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
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/cmd/dependency/base"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/basic"
	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/pkg/util/net/urlutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
)

type DfgetConfig = ClientOption

// ClientOption holds all the runtime config information.
type ClientOption struct {
	base.Options `yaml:",inline" mapstructure:",squash"`
	// URL download URL.
	URL string `yaml:"url,omitempty" mapstructure:"url,omitempty"`

	// Output full output path.
	Output string `yaml:"output,omitempty" mapstructure:"output,omitempty"`

	// Timeout download timeout(second).
	Timeout time.Duration `yaml:"timeout,omitempty" mapstructure:"timeout,omitempty"`

	BenchmarkRate unit.Bytes `yaml:"benchmarkRate,omitempty" mapstructure:"benchmarkRate,omitempty"`

	// Md5 expected file md5.
	// Deprecated: Md5 is deprecated, use DigestMethod with DigestValue instead
	Md5    string `yaml:"md5,omitempty" mapstructure:"md5,omitempty"`
	Digest string `yaml:"digest,omitempty" mapstructure:"digest,omitempty"`
	// DigestMethod indicates digest method, like md5, sha256
	DigestMethod string `yaml:"digestMethod,omitempty" mapstructure:"digestMethod,omitempty"`

	// DigestValue indicates digest value
	DigestValue string `yaml:"digestValue,omitempty" mapstructure:"digestValue,omitempty"`

	// Tag identify download task, it is available merely when md5 param not exist.
	Tag string `yaml:"tag,omitempty" mapstructure:"tag,omitempty"`

	// CallSystem system name that executes dfget.
	CallSystem string `yaml:"callSystem,omitempty" mapstructure:"callSystem,omitempty"`

	// Pattern download pattern, must be 'p2p' or 'cdn' or 'source',
	// default:`p2p`.
	Pattern string `yaml:"pattern,omitempty" mapstructure:"pattern,omitempty"`

	// CA certificate to verify when supernode interact with the source.
	Cacerts []string `yaml:"cacert,omitempty" mapstructure:"cacert,omitempty"`

	// Filter filter some query params of url, use char '&' to separate different params.
	// eg: -f 'key&sign' will filter 'key' and 'sign' query param.
	// in this way, different urls correspond one same download task that can use p2p mode.
	Filter string `yaml:"filter,omitempty" mapstructure:"filter,omitempty"`

	// Header of http request.
	// eg: --header='Accept: *' --header='Host: abc'.
	Header []string `yaml:"header,omitempty" mapstructure:"header,omitempty"`

	// DisableBackSource indicates whether to not back source to download when p2p fails.
	DisableBackSource bool `yaml:"disableBackSource,omitempty" mapstructure:"disableBackSource,omitempty"`

	// Insecure indicates whether skip secure verify when supernode interact with the source.
	Insecure bool `yaml:"insecure,omitempty" mapstructure:"insecure,omitempty"`

	// ShowProgress shows progress bar, it's conflict with `--console`.
	ShowProgress bool `yaml:"show-progress,omitempty" mapstructure:"show-progress,omitempty"`

	// LogDir is log directory of dfget.
	LogDir string `yaml:"logDir,omitempty" mapstructure:"logDir,omitempty"`

	// WorkHome is working directory of dfget.
	WorkHome string `yaml:"workHome,omitempty" mapstructure:"workHome,omitempty"`

	RateLimit rate.Limit `yaml:"rateLimit,omitempty" mapstructure:"rateLimit,omitempty"`

	// Config file paths,
	// default:["/etc/dragonfly/dfget.yaml","/etc/dragonfly.conf"].
	//
	// NOTE: It is recommended to use `/etc/dragonfly/dfget.yaml` as default,
	// and the `/etc/dragonfly.conf` is just to ensure compatibility with previous versions.
	//ConfigFiles []string `json:"-"`

	// MoreDaemonOptions indicates more options passed to daemon by command line.
	MoreDaemonOptions string `yaml:"moreDaemonOptions,omitempty" mapstructure:"moreDaemonOptions,omitempty"`
}

func NewDfgetConfig() *ClientOption {
	return &dfgetConfig
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

func (cfg *ClientOption) Convert(args []string) error {
	var err error

	if cfg.Output, err = filepath.Abs(cfg.Output); err != nil {
		return err
	}

	if cfg.URL == "" && len(args) > 0 {
		cfg.URL = args[0]
	}

	if cfg.Digest != "" {
		cfg.Tag = ""
	}

	if cfg.Console {
		cfg.ShowProgress = false
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

	dir, _ := path.Split(cfg.Output)
	if err := MkdirAll(dir, 0777, basic.UserID, basic.UserGroup); err != nil {
		return err
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

// MkdirAll make directories recursive, and changes uid, gid to latest directory.
// For example: the path /data/x exists, uid=1, gid=1
// when call MkdirAll("/data/x/y/z", 0755, 2, 2)
// MkdirAll creates /data/x/y and change owner to 2:2, creates /data/x/y/z and change owner to 2:2
func MkdirAll(dir string, perm os.FileMode, uid, gid int) error {
	var (
		// directories to create
		dirs []string
		err  error
	)
	subDir := dir
	// find not exist directories from bottom to top
	for {
		if subDir == "" {
			break
		}
		_, err = os.Stat(subDir)
		// directory exists
		if err == nil {
			break
		}
		if os.IsNotExist(err) {
			dirs = append(dirs, subDir)
		} else {
			logger.Errorf("stat error: %s", err)
			return err
		}
		subDir = path.Dir(subDir)
	}

	// no directory to create
	if len(dirs) == 0 {
		return nil
	}

	err = os.MkdirAll(dir, perm)
	if err != nil {
		logger.Errorf("mkdir error: %s", err)
		return err
	}

	// update owner from top to bottom
	for _, d := range dirs {
		err = os.Chown(d, uid, gid)
		if err != nil {
			logger.Errorf("chown error: %s", err)
			return err
		}
	}
	return nil
}
