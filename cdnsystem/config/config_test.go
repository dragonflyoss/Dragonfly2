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

import (
	"reflect"
	"testing"

	"d7y.io/dragonfly/v2/cdnsystem/plugins"
	"d7y.io/dragonfly/v2/cmd/dependency/base"
)

func TestConfigLoad(t *testing.T) {
	type fields struct {
		Options        base.Options
		BaseProperties *BaseProperties
		Plugins        map[plugins.PluginType][]*plugins.PluginProperties
		ConfigServer   string
	}
	type args struct {
		path string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "",
			fields: fields{
				Options:        base.Options{},
				BaseProperties: nil,
				Plugins:        nil,
				ConfigServer:   "",
			},
			args:    args{path: DefaultMemoryBaseDir},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{
				Options:        tt.fields.Options,
				BaseProperties: tt.fields.BaseProperties,
				Plugins:        tt.fields.Plugins,
				ConfigServer:   tt.fields.ConfigServer,
			}
			if err := c.Load(tt.args.path); (err != nil) != tt.wantErr {
				t.Errorf("Load() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConfigString(t *testing.T) {
	type fields struct {
		Options        base.Options
		BaseProperties *BaseProperties
		Plugins        map[plugins.PluginType][]*plugins.PluginProperties
		ConfigServer   string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{
				Options:        tt.fields.Options,
				BaseProperties: tt.fields.BaseProperties,
				Plugins:        tt.fields.Plugins,
				ConfigServer:   tt.fields.ConfigServer,
			}
			if got := c.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNew(t *testing.T) {
	tests := []struct {
		name string
		want *Config
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := New(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("New() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewDefaultBaseProperties(t *testing.T) {
	tests := []struct {
		name string
		want *BaseProperties
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewDefaultBaseProperties(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewDefaultBaseProperties() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewDefaultPlugins(t *testing.T) {
	tests := []struct {
		name string
		want map[plugins.PluginType][]*plugins.PluginProperties
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewDefaultPlugins(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewDefaultPlugins() = %v, want %v", got, tt.want)
			}
		})
	}
}
