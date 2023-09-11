//go:build linux

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

import "os"

var (
	DefaultWorkHome               = "/usr/local/dragonfly"
	DefaultWorkHomeMode           = os.FileMode(0700)
	DefaultCacheDir               = "/var/cache/dragonfly"
	DefaultCacheDirMode           = os.FileMode(0700)
	DefaultConfigDir              = "/etc/dragonfly"
	DefaultLogDir                 = "/var/log/dragonfly"
	DefaultDataDir                = "/var/lib/dragonfly"
	DefaultDataDirMode            = os.FileMode(0700)
	DefaultPluginDir              = "/usr/local/dragonfly/plugins"
	DefaultDownloadUnixSocketPath = "/var/run/dfdaemon.sock"
)
