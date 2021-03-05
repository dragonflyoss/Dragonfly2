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

package config

// PluginType defines the type of plugin.
type PluginType string

const (
	// StoragePlugin the storage plugin type.
	StoragePlugin = PluginType("storage")

	// SourceClientPlugin the source client plugin type
	SourceClientPlugin = PluginType("sourceClient")
)

// PluginTypes explicitly stores all available plugin types.
var PluginTypes = []PluginType{
	StoragePlugin, SourceClientPlugin,
}

// PluginProperties the properties of a plugin.
type PluginProperties struct {
	Name    string `yaml:"name"`
	Enable bool   `yaml:"enable"`
	Config  string `yaml:"config"`
}
