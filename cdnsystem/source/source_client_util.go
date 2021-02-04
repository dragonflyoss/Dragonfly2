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
	"fmt"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"strings"
)

func (s *ResourceClientAdaptor) getSchema(url string) (string, error) {
	if stringutils.IsEmptyStr(url) {
		return "", dferrors.ErrEmptyValue
	}
	parts := strings.Split(url, ":")
	if len(parts) == 0 {
		return "", fmt.Errorf("failed to get schema for url(%s)", url)
	}
	return parts[0], nil
}

// Get a source client from manager with specified schema.
func (s *ResourceClientAdaptor) getSourceClient(url string) (ResourceClient, error) {
	schema, err := s.getSchema(url)
	if err != nil {
		return nil, err
	}
	client, ok := s.clients[schema]
	if !ok || client == nil {
		return nil, fmt.Errorf("does not support schema(%s) client", schema)
	}
	return client, nil
}
