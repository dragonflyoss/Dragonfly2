/*
 *     Copyright 2023 The Dragonfly Authors
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

package util

import "k8s.io/component-base/featuregate"

var (
	FeatureGates = featuregate.NewFeatureGate()

	FeatureGateRange                 featuregate.Feature = "dfget-range"
	FeatureGateOpenRange             featuregate.Feature = "dfget-open-range"
	FeatureGateCommit                featuregate.Feature = "dfget-commit"
	FeatureGateNoLength              featuregate.Feature = "dfget-no-length"
	FeatureGateEmptyFile             featuregate.Feature = "dfget-empty-file"
	FeatureGateRecursive             featuregate.Feature = "dfget-recursive"
	FeatureGatePreheatMultiArchImage featuregate.Feature = "manager-preheat-multi-arch-image"

	defaultFeatureGates = map[featuregate.Feature]featuregate.FeatureSpec{
		FeatureGateCommit: {
			Default:       true,
			LockToDefault: false,
			PreRelease:    featuregate.Alpha,
		},
		FeatureGateNoLength: {
			Default:       true,
			LockToDefault: false,
			PreRelease:    featuregate.Alpha,
		},
		FeatureGateRange: {
			Default:       false,
			LockToDefault: false,
			PreRelease:    featuregate.Alpha,
		},
		FeatureGateOpenRange: {
			Default:       false,
			LockToDefault: false,
			PreRelease:    featuregate.Alpha,
		},
		FeatureGateEmptyFile: {
			Default:       false,
			LockToDefault: false,
			PreRelease:    featuregate.Alpha,
		},
		FeatureGateRecursive: {
			Default:       false,
			LockToDefault: false,
			PreRelease:    featuregate.Alpha,
		},
		FeatureGatePreheatMultiArchImage: {
			Default:       false,
			LockToDefault: false,
			PreRelease:    featuregate.Alpha,
		},
	}
)

func init() {
	_ = FeatureGates.Add(defaultFeatureGates)
}
