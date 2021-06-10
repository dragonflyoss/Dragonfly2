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

package plugins

import (
	"fmt"
	"sync"
)

// NewManager creates a default plugin manager instant.
func NewManager() Manager {
	return &managerIml{
		builders: NewRepository(),
		plugins:  NewRepository(),
	}
}

// NewRepository creates a default repository instant.
func NewRepository() Repository {
	return &repositoryIml{
		repos: make(map[PluginType]*sync.Map),
	}
}

// Manager manages all plugin builders and plugin instants.
type Manager interface {
	// GetBuilder adds a Builder object with the giving plugin type and name.
	AddBuilder(pt PluginType, name string, b Builder) error

	// GetBuilder returns a Builder object with the giving plugin type and name.
	GetBuilder(pt PluginType, name string) (Builder, bool)

	// DeleteBuilder deletes a builder with the giving plugin type and name.
	DeleteBuilder(pt PluginType, name string)

	// AddPlugin adds a plugin into this manager.
	AddPlugin(p Plugin) error

	// GetPlugin returns a plugin with the giving plugin type and name.
	GetPlugin(pt PluginType, name string) (Plugin, bool)

	// DeletePlugin deletes a plugin with the giving plugin type and name.
	DeletePlugin(pt PluginType, name string)
}

// Plugin defines methods that plugins need to implement.
type Plugin interface {
	// Type returns the type of this plugin.
	Type() PluginType

	// Name returns the name of this plugin.
	Name() string
}

// Builder is a function that creates a new plugin instant with the giving conf.
type Builder func(conf interface{}) (Plugin, error)

// Repository stores data related to plugin.
type Repository interface {
	// Add adds a data to this repository.
	Add(pt PluginType, name string, data interface{}) error

	// Get gets a data with the giving type and name from this
	// repository.
	Get(pt PluginType, name string) (interface{}, bool)

	// Delete deletes a data with the giving type and name from
	// this repository.
	Delete(pt PluginType, name string)
}

// -----------------------------------------------------------------------------
// implementation of Manager

type managerIml struct {
	builders Repository
	plugins  Repository
}

var _ Manager = (*managerIml)(nil)

func (m *managerIml) AddBuilder(pt PluginType, name string, b Builder) error {
	if b == nil {
		return fmt.Errorf("builder is nil")
	}
	return m.builders.Add(pt, name, b)
}

func (m *managerIml) GetBuilder(pt PluginType, name string) (Builder, bool) {
	data, ok := m.builders.Get(pt, name)
	if ok {
		return data.(Builder), true
	}
	return nil, false
}

func (m *managerIml) DeleteBuilder(pt PluginType, name string) {
	m.builders.Delete(pt, name)
}

func (m *managerIml) AddPlugin(p Plugin) error {
	if p == nil {
		return fmt.Errorf("plugin is nil")
	}
	return m.plugins.Add(p.Type(), p.Name(), p)
}

func (m *managerIml) GetPlugin(pt PluginType, name string) (Plugin, bool) {
	data, ok := m.plugins.Get(pt, name)
	if !ok {
		return nil, false
	}
	return data.(Plugin), true
}

func (m *managerIml) DeletePlugin(pt PluginType, name string) {
	m.plugins.Delete(pt, name)
}

// -----------------------------------------------------------------------------
// implementation of Repository

type repositoryIml struct {
	repos map[PluginType]*sync.Map
	lock  sync.Mutex
}

var _ Repository = (*repositoryIml)(nil)

func (r *repositoryIml) Add(pt PluginType, name string, data interface{}) error {
	if data == nil {
		return fmt.Errorf("data is nil")
	}
	if !validate(pt, name) {
		return fmt.Errorf("invalid pluginType %s, name %s", pt, name)
	}
	m := r.getRepo(pt)
	m.Store(name, data)
	return nil
}

func (r *repositoryIml) Get(pt PluginType, name string) (interface{}, bool) {
	m := r.getRepo(pt)
	if v, ok := m.Load(name); ok && v != nil {
		return v, true
	}
	return nil, false
}

func (r *repositoryIml) Delete(pt PluginType, name string) {
	m := r.getRepo(pt)
	m.Delete(name)
}

func (r *repositoryIml) getRepo(pt PluginType) *sync.Map {
	var (
		m  *sync.Map
		ok bool
	)
	if m, ok = r.repos[pt]; ok && m != nil {
		return m
	}

	r.lock.Lock()
	if m, ok = r.repos[pt]; !ok || m == nil {
		m = &sync.Map{}
		r.repos[pt] = m
	}
	r.lock.Unlock()
	return m
}

/*
   helper functions
*/

func validate(pt PluginType, name string) bool {
	if name == "" {
		return false
	}
	for i := len(PluginTypes) - 1; i >= 0; i-- {
		if pt == PluginTypes[i] {
			return true
		}
	}
	return false
}
