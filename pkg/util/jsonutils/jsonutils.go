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

// Package jsonutils provides utilities supplementing the standard 'encoding/json' package.
package jsonutils

import "encoding/json"

func Marshal(v interface{}) (string, error) {
	if v == nil {
		return "", nil
	}

	if bytes, err := json.Marshal(v); err == nil {
		return string(bytes), nil
	} else {
		return "", err
	}
}

func (n *NetworkType) UnmarshalJSON(b []byte) error {
	var t string
	err := json.Unmarshal(b, &t)
	if err != nil {
		return err
	}

	*n = NetworkType(t)
	return nil
}
