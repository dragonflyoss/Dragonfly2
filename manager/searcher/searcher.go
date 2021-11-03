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

package searcher

import (
	"sort"

	"github.com/mitchellh/mapstructure"
	"gonum.org/v1/gonum/stat"

	"d7y.io/dragonfly/v2/manager/model"
)

const (
	conditionSecurityDomain = "security_domain"
	conditionLocation       = "location"
	conditionIDC            = "idc"
)

const (
	conditionLocationWeight = 0.7
	conditionIDCWeight      = 0.3
)

type Scopes struct {
	Location []string `mapstructure:"location"`
	IDC      []string `mapstructure:"idc"`
}

type Searcher interface {
	FindSchedulerCluster([]model.SchedulerCluster, map[string]string) (model.SchedulerCluster, bool)
}

type searcher struct{}

func New() Searcher {
	s, err := LoadPlugin()
	if err != nil {
		return &searcher{}
	}

	return s
}

func (s *searcher) FindSchedulerCluster(schedulerClusters []model.SchedulerCluster, conditions map[string]string) (model.SchedulerCluster, bool) {
	if len(schedulerClusters) <= 0 || len(conditions) <= 0 {
		return model.SchedulerCluster{}, false
	}

	// If there are security domain conditions, match clusters of the same security domain.
	// If the security domain condition does not exist, it matches clusters that does not have a security domain.
	// Then use clusters sets to score according to scopes.
	securityDomain := conditions[conditionSecurityDomain]
	var clusters []model.SchedulerCluster
	for _, v := range schedulerClusters {
		if v.SecurityGroup.Domain == securityDomain {
			clusters = append(clusters, v)
		}
	}

	switch len(clusters) {
	case 0:
		return model.SchedulerCluster{}, false
	case 1:
		return clusters[0], true
	default:
		var maxMean float64 = 0
		cluster := clusters[0]
		for _, v := range clusters {
			mean := calculateSchedulerClusterMean(conditions, v.Scopes)
			if mean > maxMean {
				maxMean = mean
				cluster = v
			}
		}
		return cluster, true
	}
}

func calculateSchedulerClusterMean(conditions map[string]string, rawScopes map[string]interface{}) float64 {
	var scopes Scopes
	if err := mapstructure.Decode(rawScopes, &scopes); err != nil {
		return 0
	}

	location := conditions[conditionLocation]
	lx := calculateConditionScore(location, scopes.Location)

	idc := conditions[conditionIDC]
	ix := calculateConditionScore(idc, scopes.IDC)

	return stat.Mean([]float64{lx, ix}, []float64{conditionLocationWeight, conditionIDCWeight})
}

func calculateConditionScore(value string, scope []string) float64 {
	if value == "" {
		return 0
	}

	if len(scope) <= 0 {
		return 0
	}

	i := sort.SearchStrings(scope, value)
	if i < 0 {
		return 0
	}

	return 1
}
