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

package dynconfig

import (
	"os"

	"gopkg.in/yaml.v3"
)

type dynconfigLocal struct {
	filepath string
}

// newDynconfigLocal returns a new local dynconfig instence
func newDynconfigLocal(path string) (*dynconfigLocal, error) {
	d := &dynconfigLocal{
		filepath: path,
	}

	return d, nil
}

// Unmarshal unmarshals the config into a Struct. Make sure that the tags
// on the fields of the structure are properly set.
func (d *dynconfigLocal) Unmarshal(rawVal any) error {
	b, err := os.ReadFile(d.filepath)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(b, rawVal)
}
