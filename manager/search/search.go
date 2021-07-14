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

package search

import (
	"sort"

	"d7y.io/dragonfly/v2/manager/model"
	"gonum.org/v1/gonum/stat"
	"gorm.io/datatypes"
)

const (
	conditionSecurityDomain = "security_domain"
	conditionLocation       = "location"
	conditionIDC            = "idc"
)

const (
	scopeLocation = "location"
	scopeIDC      = "idc"
)

const (
	conditionLocationWeight = 0.7
	conditionIDCWeight      = 0.3
)

type Search interface {
	SchedulerCluster([]model.SchedulerCluster, map[string]string) (model.SchedulerCluster, bool)
}

type search struct{}

func New() Search {
	s, err := loadPlugin()
	if err != nil {
		return &search{}
	}

	return s
}

func (s *search) SchedulerCluster(schedulerClusters []model.SchedulerCluster, conditions map[string]string) (model.SchedulerCluster, bool) {
	if len(schedulerClusters) <= 0 || len(conditions) <= 0 {
		return model.SchedulerCluster{}, false
	}

	// If there are security domain conditions, match clusters of the same security domain.
	// If the security domain condition does not exist, it matches clusters that does not have a security domain.
	// Then use clusters sets to score according to scopes.
	securityDomain, _ := conditions[conditionSecurityDomain]
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
		var cluster model.SchedulerCluster
		for _, v := range clusters {
			lx := calculateConditionScore(conditionLocation, conditions, scopeLocation, v.Scopes)
			ix := calculateConditionScore(conditionIDC, conditions, scopeIDC, v.Scopes)
			mean := stat.Mean([]float64{lx, ix}, []float64{conditionLocationWeight, conditionIDCWeight})
			if mean > maxMean {
				maxMean = mean
				cluster = v
			}
		}
		return cluster, true
	}
}

func calculateConditionScore(condition string, conditions map[string]string, scope string, scopes datatypes.JSONMap) float64 {
	cv, ok := conditions[condition]
	if !ok {
		return 0
	}

	if scopes == nil {
		return 0
	}

	// TODO mapstructure for sv
	rawSV, ok := scopes[scope].([]interface{})
	if !ok {
		return 0
	}

	sv := make([]string, len(rawSV))
	for i, v := range rawSV {
		sv[i] = v.(string)
	}

	i := sort.SearchStrings(sv, cv)
	if i < 0 {
		return 0
	}

	return 1
}
