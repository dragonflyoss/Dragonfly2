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

package version

import "fmt"

var (
	Major      = "2"
	Minor      = "0"
	GitVersion = "v2.0.0"
	GitCommit  = "3f8535a797da131a47d7a678b8f4a28f4ba42a31"
	Platform   = osArch
	GoVersion  = "go1.15"
)

func Version() string {
	return fmt.Sprintf("Major: %s, Minor: %s, GitVersion: %s, GitCommit: %s, Platform: %s, GoVersion: %s", Major,
		Minor, GitVersion, GitCommit, Platform, GoVersion)
}
