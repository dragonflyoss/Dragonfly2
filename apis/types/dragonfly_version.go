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

package types

import (
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
)

// DragonflyVersion Version and build information of Dragonfly components.
//
// swagger:model DragonflyVersion
type DragonflyVersion struct {

	// Dragonfly components's architecture target
	Arch string `json:"Arch,omitempty"`

	// Build Date of Dragonfly components
	BuildDate string `json:"BuildDate,omitempty"`

	// Golang runtime version
	GoVersion string `json:"GoVersion,omitempty"`

	// Dragonfly components's operating system
	OS string `json:"OS,omitempty"`

	// Git commit when building Dragonfly components
	Revision string `json:"Revision,omitempty"`

	// Version of Dragonfly components
	Version string `json:"Version,omitempty"`
}

// Validate validates this dragonfly version
func (m *DragonflyVersion) Validate(formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *DragonflyVersion) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *DragonflyVersion) UnmarshalBinary(b []byte) error {
	var res DragonflyVersion
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}

