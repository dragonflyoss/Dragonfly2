/*
 *     Copyright 2022 The Dragonfly Authors
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

package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/cmd/dependency/base"
	"d7y.io/dragonfly/v2/internal/dferrors"
	"d7y.io/dragonfly/v2/pkg/os/user"
	"d7y.io/dragonfly/v2/pkg/strings"
)

type DfcacheConfig = CacheOption

// CacheOption holds all the runtime config information.
type CacheOption struct {
	base.Options `yaml:",inline" mapstructure:",squash"`

	// Cid content/cache ID
	Cid string `yaml:"cid,omitempty" mapstructure:"cid,omitempty"`

	// Tag identify task
	Tag string `yaml:"tag,omitempty" mapstructure:"tag,omitempty"`

	// Timeout operation timeout(second).
	Timeout time.Duration `yaml:"timeout,omitempty" mapstructure:"timeout,omitempty"`

	// LogDir is log directory of dfcache.
	LogDir string `yaml:"logDir,omitempty" mapstructure:"logDir,omitempty"`

	// Maximum size in megabytes of log files before rotation (default: 1024)
	LogMaxSize int `yaml:"logMaxSize" mapstructure:"logMaxSize"`

	// Maximum number of days to retain old log files (default: 7)
	LogMaxAge int `yaml:"logMaxAge" mapstructure:"logMaxAge"`

	// Maximum number of old log files to keep (default: 20)
	LogMaxBackups int `yaml:"logMaxBackups" mapstructure:"logMaxBackups"`

	// WorkHome is working directory of dfcache.
	WorkHome string `yaml:"workHome,omitempty" mapstructure:"workHome,omitempty"`

	// DaemonSock is socket path of dfdaemon to connect.
	DaemonSock string `yaml:"daemonSocket,omitempty" mapstructure:"daemon-sock,omitempty"`

	// Output full output path for export task
	Output string `yaml:"output,omitempty" mapstructure:"output,omitempty"`

	// Path full input path for import task
	// TODO: change to Input
	Path string `yaml:"path,omitempty" mapstructure:"path,omitempty"`

	// RateLimit limits export task
	RateLimit rate.Limit `yaml:"rateLimit,omitempty" mapstructure:"rateLimit,omitempty"`

	// LocalOnly indicates check local cache only
	LocalOnly bool `yaml:"localOnly,omitempty" mapstructure:"localOnly,omitempty"`
}

func NewDfcacheConfig() *CacheOption {
	return &CacheOption{}
}

func validateCacheStat(cfg *CacheOption) error {
	return nil
}

func validateCacheImport(cfg *CacheOption) error {
	if err := cfg.checkInput(); err != nil {
		return fmt.Errorf("input path %s: %w", err.Error(), dferrors.ErrInvalidArgument)
	}
	return nil
}

func ValidateCacheExport(cfg *CacheOption) error {
	if err := cfg.checkOutput(); err != nil {
		return fmt.Errorf("output %s: %w", err.Error(), dferrors.ErrInvalidArgument)
	}
	return nil
}

func ValidateCacheDelete(cfg *CacheOption) error {
	return nil
}

func (cfg *CacheOption) Validate(cmd string) error {
	// Some common validations
	if cfg == nil {
		return fmt.Errorf("runtime config: %w", dferrors.ErrInvalidArgument)
	}
	if cfg.Cid == "" {
		return fmt.Errorf("missing Cid: %w", dferrors.ErrInvalidArgument)
	}
	if strings.IsBlank(cfg.Cid) {
		return fmt.Errorf("Cid are all blanks: %w", dferrors.ErrInvalidArgument)
	}

	// cmd specific validations
	switch cmd {
	case CmdStat:
		return validateCacheStat(cfg)
	case CmdImport:
		return validateCacheImport(cfg)
	case CmdExport:
		return ValidateCacheExport(cfg)
	case CmdDelete:
		return ValidateCacheDelete(cfg)
	default:
		return fmt.Errorf("unknown cache subcommand %s: %w", cmd, dferrors.ErrInvalidArgument)
	}
}

func ConvertCacheStat(cfg *CacheOption, args []string) error {
	return nil
}

func convertCacheImport(cfg *CacheOption, args []string) error {
	var err error
	if cfg.Path == "" && len(args) > 0 {
		cfg.Path = args[0]
	}
	if cfg.Path == "" {
		return fmt.Errorf("missing input file: %w", dferrors.ErrInvalidArgument)
	}

	if cfg.Path, err = filepath.Abs(cfg.Path); err != nil {
		return fmt.Errorf("get absulate path for %s: %w", cfg.Path, err)
	}
	return nil
}

func ConvertCacheExport(cfg *CacheOption, args []string) error {
	var err error
	if cfg.Output == "" && len(args) > 0 {
		cfg.Output = args[0]
	}
	if cfg.Output == "" {
		return fmt.Errorf("missing output file: %w", dferrors.ErrInvalidArgument)
	}

	if cfg.Output, err = filepath.Abs(cfg.Output); err != nil {
		return fmt.Errorf("get absulate path for %s: %w", cfg.Output, err)
	}
	return nil
}

func ConvertCacheDelete(cfg *CacheOption, args []string) error {
	return nil
}

func (cfg *CacheOption) Convert(cmd string, args []string) error {
	if cfg == nil {
		return fmt.Errorf("runtime config: %w", dferrors.ErrInvalidArgument)
	}

	switch cmd {
	case CmdStat:
		return ConvertCacheStat(cfg, args)
	case CmdImport:
		return convertCacheImport(cfg, args)
	case CmdExport:
		return ConvertCacheExport(cfg, args)
	case CmdDelete:
		return ConvertCacheDelete(cfg, args)
	default:
		return fmt.Errorf("unknown cache subcommand %s: %w", cmd, dferrors.ErrInvalidArgument)
	}
}

func (cfg *CacheOption) String() string {
	data, _ := json.Marshal(cfg)
	return string(data)
}

func (cfg *CacheOption) checkInput() error {
	stat, err := os.Stat(cfg.Path)
	if err != nil {
		return fmt.Errorf("stat input path %q: %w", cfg.Path, err)
	}
	if stat.IsDir() {
		return fmt.Errorf("path[%q] is directory but requires file path", cfg.Path)
	}
	if err := syscall.Access(cfg.Path, syscall.O_RDONLY); err != nil {
		return fmt.Errorf("access %q: %w", cfg.Path, err)
	}
	return nil
}

func (cfg *CacheOption) checkOutput() error {
	if cfg.Output == "" {
		return errors.New("no output file path specified")
	}

	if !filepath.IsAbs(cfg.Output) {
		absPath, err := filepath.Abs(cfg.Output)
		if err != nil {
			return fmt.Errorf("get absolute path[%s] error: %v", cfg.Output, err)
		}
		cfg.Output = absPath
	}

	outputDir, _ := path.Split(cfg.Output)
	if err := MkdirAll(outputDir, 0700, os.Getuid(), os.Getgid()); err != nil {
		return err
	}

	f, err := os.Stat(cfg.Output)
	if err == nil && f.IsDir() {
		return fmt.Errorf("path[%s] is directory but requires file path", cfg.Output)
	}

	// check permission
	for dir := cfg.Output; !strings.IsBlank(dir); dir = filepath.Dir(dir) {
		if err := syscall.Access(dir, syscall.O_RDWR); err == nil {
			break
		} else if os.IsPermission(err) || dir == "/" {
			return fmt.Errorf("user[%s] path[%s] %v", user.Username(), cfg.Output, err)
		}
	}
	return nil
}
