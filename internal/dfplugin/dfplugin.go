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

package dfplugin

import (
	"errors"
	"fmt"
	"path"
	"plugin"

	"d7y.io/dragonfly/v2/internal/dfpath"
)

const (
	// PluginFormat indicates the lookup name of a plugin in plugin directory.
	PluginFormat = "d7y-%s-plugin-%s.so"

	// PluginInitFuncName indicates the function `DragonflyPluginInit` must be implemented in plugin
	PluginInitFuncName = "DragonflyPluginInit"

	// PluginMetaKeyType indicates the type of a plugin, currently support: resource
	PluginMetaKeyType = "type"
	// PluginMetaKeyName indicates the name of a plugin
	PluginMetaKeyName = "name"
)

type PluginType string

const (
	PluginTypeResource = PluginType("resource")
)

type PluginInitFunc func(option map[string]string) (plugin interface{}, meta map[string]string, err error)

func Load(typ PluginType, name string, option map[string]string) (interface{}, map[string]string, error) {
	soName := fmt.Sprintf(PluginFormat, string(typ), name)
	p, err := plugin.Open(path.Join(dfpath.PluginsDir, soName))
	if err != nil {
		return nil, nil, err
	}

	symbol, err := p.Lookup(PluginInitFuncName)
	if err != nil {
		return nil, nil, err
	}

	// FIXME when use symbol.(PluginInitFunc), ok is always false
	f, ok := symbol.(func(option map[string]string) (plugin interface{}, meta map[string]string, err error))
	if !ok {
		return nil, nil, errors.New("invalid plugin init function signature")
	}

	i, meta, err := f(option)
	if err != nil {
		return nil, nil, err
	}

	if meta == nil {
		return nil, nil, errors.New("empty plugin metadata")
	}

	if meta[PluginMetaKeyType] != string(typ) {
		return nil, nil, errors.New("plugin type not match")
	}

	if meta[PluginMetaKeyName] != name {
		return nil, nil, errors.New("plugin name not match")
	}
	return i, meta, nil
}
