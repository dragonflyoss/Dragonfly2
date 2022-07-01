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

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"d7y.io/dragonfly/v2/internal/constants"
)

var (
	Major      = "2"
	Minor      = "0"
	GitVersion = "v2.0.4"
	GitCommit  = "unknown"
	Platform   = osArch
	BuildTime  = "unknown"
	GoVersion  = "unknown"
	Gotags     = "unknown"
	Gogcflags  = "unknown"
)

var (
	versionGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: constants.MetricsNamespace,
		Name:      "version",
		Help:      "Version info of dragonfly components.",
	}, []string{"major", "minor", "git_version", "git_commit", "platform", "build_time", "go_version", "go_tags", "go_gcflags"})
)

func init() {
	versionGauge.WithLabelValues(Major, Minor, GitVersion, GitCommit, Platform, BuildTime, GoVersion, Gotags, Gogcflags).Set(1)
}

func Version() string {
	return fmt.Sprintf("Major: %s, Minor: %s, GitVersion: %s, GitCommit: %s, Platform: %s, BuildTime: %s, GoVersion: %s, Gotags: %s, Gogcflags: %s", Major,
		Minor, GitVersion, GitCommit, Platform, BuildTime, GoVersion, Gotags, Gogcflags)
}
