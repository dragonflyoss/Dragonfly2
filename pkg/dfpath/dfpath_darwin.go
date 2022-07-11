//go:build darwin
// +build darwin

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

	"d7y.io/dragonfly/v2/pkg/basic"
)

var DefaultWorkHome = filepath.Join(basic.HomeDir, ".dragonfly")
var DefaultCacheDir = filepath.Join(DefaultWorkHome, "cache")
var DefaultConfigDir = filepath.Join(DefaultWorkHome, "config")
var DefaultLogDir = filepath.Join(DefaultWorkHome, "logs")
var DefaultDataDir = filepath.Join(DefaultWorkHome, "data")
var DefaultPluginDir = filepath.Join(DefaultWorkHome, "plugins")
