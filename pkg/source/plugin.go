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

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dfplugin"
)

const (
	pluginMetadataScheme = "scheme"
)

func LoadPlugin(dir, scheme string) (ResourceClient, error) {
	// TODO init option
	logger.Debugf("try to load source plugin: %s", scheme)
	client, meta, err := dfplugin.Load(dir, dfplugin.PluginTypeResource, scheme, map[string]string{})
	if err != nil {
		logger.Errorf("load source plugin error: %s", err)
		return nil, err
	}

	if meta[pluginMetadataScheme] != scheme {
		logger.Errorf("load source plugin error: support scheme not match")
		return nil, errors.New("support schema not match")
	}

	rc, ok := client.(ResourceClient)
	if !ok {
		logger.Errorf("invalid client, not a ResourceClient")
		return nil, errors.New("invalid client, not a ResourceClient")
	}

	logger.Debugf("loaded source plugin %s", scheme)
	return rc, nil
}
