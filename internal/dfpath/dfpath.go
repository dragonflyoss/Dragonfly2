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

package dfpath

import (
	"path/filepath"
	"sync"

	"d7y.io/dragonfly/v2/pkg/util/fileutils"
)

// Dfpath is the interface used for init project path
type Dfpath interface {
	WorkHome() string
	CacheDir() string
	LogDir() string
	DataDir() string
	PluginDir() string
	DaemonSockPath() string
	DaemonLockPath() string
	DfgetLockPath() string
}

// Dfpath provides init project path function
type dfpath struct {
	workHome       string
	cacheDir       string
	logDir         string
	dataDir        string
	pluginDir      string
	daemonSockPath string
	daemonLockPath string
	dfgetLockPath  string
}

// Cache of the dfpath
var cache struct {
	sync.Once
	d   *dfpath
	err error
}

// Option is a functional option for configuring the dfpath
type Option func(d *dfpath)

// WithWorkHome set the workhome directory
func WithWorkHome(dir string) Option {
	return func(d *dfpath) {
		d.workHome = dir
	}
}

// WithCacheDir set the cache directory
func WithCacheDir(dir string) Option {
	return func(d *dfpath) {
		d.cacheDir = dir
	}
}

// WithLogDir set the log directory
func WithLogDir(dir string) Option {
	return func(d *dfpath) {
		d.logDir = dir
	}
}

// New returns a new dfpath interface
func New(options ...Option) (Dfpath, error) {
	cache.Do(func() {
		d := &dfpath{
			workHome: DefaultWorkHome,
			cacheDir: DefaultCacheDir,
			logDir:   DefaultLogDir,
		}

		for _, opt := range options {
			opt(d)
		}

		d.dataDir = filepath.Join(d.workHome, "data")
		d.pluginDir = filepath.Join(d.workHome, "plugins")
		d.daemonSockPath = filepath.Join(d.workHome, "daemon.sock")
		d.daemonLockPath = filepath.Join(d.workHome, "daemon.lock")
		d.dfgetLockPath = filepath.Join(d.workHome, "dfget.lock")

		// Create directories
		for _, dir := range []string{d.workHome, d.cacheDir, d.logDir, d.dataDir, d.pluginDir} {
			if err := fileutils.MkdirAll(dir); err != nil {
				cache.err = err
			}
		}

		cache.d = d
	})

	if cache.err != nil {
		return nil, cache.err
	}

	d := *cache.d
	return &d, nil
}

func (d *dfpath) WorkHome() string {
	return d.workHome
}

func (d *dfpath) CacheDir() string {
	return d.cacheDir
}

func (d *dfpath) LogDir() string {
	return d.logDir
}

func (d *dfpath) DataDir() string {
	return d.dataDir
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
