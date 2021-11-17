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
	"strings"

	"github.com/mitchellh/mapstructure"
	"gonum.org/v1/gonum/stat"

	"d7y.io/dragonfly/v2/manager/model"
)

const (
	conditionIDC            = "idc"
	conditionLocation       = "location"
	conditionNetTopology    = "net_topology"
	conditionSecurityDomain = "security_domain"
)

const (
	conditionLocationWeight = 0.7
	conditionIDCWeight      = 0.3
)

type Scopes struct {
	IDC         string `mapstructure:"idc"`
	Location    string `mapstructure:"location"`
	NetTopology string `mapstructure:"net_topology"`
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
		// If the security domain does not match, there is no cluster available
		return model.SchedulerCluster{}, false
	case 1:
		// If only one cluster matches the security domain, return the cluster directly
		return clusters[0], true
	default:
		// If there are multiple clusters matching the security domain,
		// select the schuelder cluster with a higher score
		var maxScore float64 = 0
		result := clusters[0]
		for _, cluster := range clusters {
			var scopes Scopes
			if err := mapstructure.Decode(cluster.Scopes, &scopes); err != nil {
				// Scopes parse failed to skip this evaluation
				continue
			}

			score := evaluate(conditions, scopes)
			if score > maxScore {
				maxScore = score
				result = cluster
			}
		}
		return result, true
	}
}

func evaluate(conditions map[string]string, scopes Scopes) float64 {
	location := conditions[conditionLocation]
	lx := calculateConditionScore(location, scopes.Location)

	idc := conditions[conditionIDC]
	ix := calculateConditionScore(idc, scopes.IDC)

	return stat.Mean([]float64{lx, ix}, []float64{conditionLocationWeight, conditionIDCWeight})
}

// calculateIDCAffinity 0.0~1.0 larger and better
func calculateIDCAffinity(dst, src string) float64 {
	if dst != "" && src != "" && strings.Compare(dst, src) == 0 {
		return maxScore
	}

	return minScore
}

// calculateMultiElementAffinity 0.0~1.0 larger and better
func calculateMultiElementAffinity(dst, src string) float64 {
	if dst == "" || src == "" {
		return minScore
	}

	if strings.Compare(dst, src) == 0 {
		return maxScore
	}

	// Calculate the number of multi-element matches divided by "|"
	var score, elementLen int
	dstElements := strings.Split(dst, "|")
	srcElements := strings.Split(src, "|")
	elementLen = mathutils.MaxInt(len(dstElements), len(srcElements))

	// Maximum element length is 5
	if elementLen > maxElementLen {
		elementLen = maxElementLen
	}

	for i := 0; i < elementLen; i++ {
		if strings.Compare(dstElements[i], srcElements[i]) != 0 {
			break
		}
		score++
	}

	return float64(score) / float64(maxElementLen)
}
