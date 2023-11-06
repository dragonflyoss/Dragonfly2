package e2eutil

import "k8s.io/component-base/featuregate"

var (
	FeatureGates = featuregate.NewFeatureGate()

	FeatureGateRange                 featuregate.Feature = "dfget-range"
	FeatureGateOpenRange             featuregate.Feature = "dfget-open-range"
	FeatureGateCommit                featuregate.Feature = "dfget-commit"
	FeatureGateNoLength              featuregate.Feature = "dfget-no-length"
	FeatureGateEmptyFile             featuregate.Feature = "dfget-empty-file"
	FeatureGateRecursive             featuregate.Feature = "dfget-recursive"
	FeatureGatePreheatMultiArchImage featuregate.Feature = "dfget-preheat-multi-arch-image"

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
