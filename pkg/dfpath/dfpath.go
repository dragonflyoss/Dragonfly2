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

//go:generate mockgen -destination mocks/dfpath_mock.go -source dfpath.go -package mocks

package dfpath

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/hashicorp/go-multierror"
)

// Dfpath is the interface used for init project path.
type Dfpath interface {
	WorkHome() string
	WorkHomeMode() fs.FileMode
	CacheDir() string
	CacheDirMode() fs.FileMode
	LogDir() string
	DataDir() string
	DataDirMode() fs.FileMode
	PluginDir() string
	DaemonSockPath() string
	DaemonLockPath() string
	DfgetLockPath() string
}

// Dfpath provides init project path function.
type dfpath struct {
	workHome       string
	workHomeMode   fs.FileMode
	cacheDir       string
	cacheDirMode   fs.FileMode
	logDir         string
	dataDir        string
	dataDirMode    fs.FileMode
	pluginDir      string
	daemonSockPath string
	daemonLockPath string
	dfgetLockPath  string
}

// Cache of the dfpath.
var cache struct {
	sync.Once
	d   *dfpath
	err *multierror.Error
}

// Option is a functional option for configuring the dfpath.
type Option func(d *dfpath)

// WithWorkHome set the workhome directory.
func WithWorkHome(dir string) Option {
	return func(d *dfpath) {
		d.workHome = dir
	}
}

// WithWorkHomeMode sets the workHome directory mode
func WithWorkHomeMode(mode fs.FileMode) Option {
	return func(d *dfpath) {
		d.workHomeMode = mode
	}
}

// WithCacheDir set the cache directory.
func WithCacheDir(dir string) Option {
	return func(d *dfpath) {
		d.cacheDir = dir
	}
}

// WithCacheDirMode sets the cacheDir mode
func WithCacheDirMode(mode fs.FileMode) Option {
	return func(d *dfpath) {
		d.cacheDirMode = mode
	}
}

// WithLogDir set the log directory.
func WithLogDir(dir string) Option {
	return func(d *dfpath) {
		d.logDir = dir
	}
}

// WithDataDir set download data directory.
func WithDataDir(dir string) Option {
	return func(d *dfpath) {
		d.dataDir = dir
	}
}

// WithDataDirMode sets the dataDir mode
func WithDataDirMode(mode fs.FileMode) Option {
	return func(d *dfpath) {
		d.dataDirMode = mode
	}
}

// WithPluginDir set plugin directory.
func WithPluginDir(dir string) Option {
	return func(d *dfpath) {
		d.pluginDir = dir
	}
}

// WithDownloadUnixSocketPath set unix socket path.
func WithDownloadUnixSocketPath(path string) Option {
	return func(d *dfpath) {
		d.daemonSockPath = path
	}
}

// New returns a new dfpath interface.
func New(options ...Option) (Dfpath, error) {
	cache.Do(func() {
		d := &dfpath{
			workHome:       DefaultWorkHome,
			workHomeMode:   DefaultWorkHomeMode,
			logDir:         DefaultLogDir,
			dataDir:        DefaultDataDir,
			dataDirMode:    DefaultDataDirMode,
			pluginDir:      DefaultPluginDir,
			cacheDir:       DefaultCacheDir,
			cacheDirMode:   DefaultCacheDirMode,
			daemonSockPath: DefaultDownloadUnixSocketPath,
		}

		for _, opt := range options {
			opt(d)
		}

		// Initialize dfdaemon path.
		d.daemonLockPath = filepath.Join(d.workHome, "daemon.lock")
		d.dfgetLockPath = filepath.Join(d.workHome, "dfget.lock")

		// Create workhome directory.
		if err := os.MkdirAll(d.workHome, d.workHomeMode); err != nil {
			cache.err = multierror.Append(cache.err, err)
		}

		// Create log directory.
		if err := os.MkdirAll(d.logDir, fs.FileMode(0700)); err != nil {
			cache.err = multierror.Append(cache.err, err)
		}

		// Create plugin directory.
		if err := os.MkdirAll(d.pluginDir, fs.FileMode(0700)); err != nil {
			cache.err = multierror.Append(cache.err, err)
		}

		// Create unix socket directory.
		if err := os.MkdirAll(path.Dir(d.daemonSockPath), fs.FileMode(0700)); err != nil {
			cache.err = multierror.Append(cache.err, err)
		}

		// Create cache directory.
		if err := os.MkdirAll(d.cacheDir, d.cacheDirMode); err != nil {
			cache.err = multierror.Append(cache.err, err)
		}

		// Create data directory.
		if err := os.MkdirAll(d.dataDir, d.dataDirMode); err != nil {
			cache.err = multierror.Append(cache.err, err)
		}

		cache.d = d
	})

	if cache.err.ErrorOrNil() != nil {
		return nil, cache.err
	}

	d := *cache.d
	return &d, nil
}

func (d *dfpath) WorkHome() string {
	return d.workHome
}

func (d *dfpath) WorkHomeMode() fs.FileMode {
	return d.workHomeMode
}

func (d *dfpath) CacheDir() string {
	return d.cacheDir
}

func (d *dfpath) CacheDirMode() fs.FileMode {
	return d.cacheDirMode
}

func (d *dfpath) LogDir() string {
	return d.logDir
}

func (d *dfpath) DataDir() string {
	return d.dataDir
}

func (d *dfpath) DataDirMode() fs.FileMode {
	return d.dataDirMode
}

func (d *dfpath) PluginDir() string {
	return d.pluginDir
}

func (d *dfpath) DaemonSockPath() string {
	return d.daemonSockPath
}

func (d *dfpath) DaemonLockPath() string {
	return d.daemonLockPath
}

func (d *dfpath) DfgetLockPath() string {
	return d.dfgetLockPath
}
