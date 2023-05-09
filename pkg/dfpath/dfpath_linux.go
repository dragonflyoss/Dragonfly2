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

var DefaultWorkHome = "/usr/local/dragonfly"
var DefaultWorkHomeMode = os.FileMode(0755)
var DefaultCacheDir = "/var/cache/dragonfly"
var DefaultCacheDirMode = os.FileMode(0755)
var DefaultConfigDir = "/etc/dragonfly"
var DefaultLogDir = "/var/log/dragonfly"
var DefaultDataDir = "/var/lib/dragonfly"
var DefaultDataDirMode = os.FileMode(0755)
var DefaultPluginDir = "/usr/local/dragonfly/plugins"
var DefaultDownloadUnixSocketPath = "/var/run/dfdaemon.sock"
