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

package source

import (
	"errors"

	"d7y.io/dragonfly/v2/internal/dfplugin"
)

const (
	pluginMetadataSchema = "schema"
)

func LoadPlugin(schema string) (ResourceClient, error) {
	// TODO init option
	client, meta, err := dfplugin.Load(dfplugin.PluginTypeResource, schema, map[string]string{})
	if err != nil {
		return nil, err
	}
	if meta[pluginMetadataSchema] != schema {
		return nil, errors.New("support schema not match")
	}
	if rc, ok := client.(ResourceClient); ok {
		return rc, err
	}
	return nil, errors.New("invalid client, not a ResourceClient")
}
