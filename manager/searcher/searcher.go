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

//go:generate mockgen -destination mocks/searcher_mock.go -source searcher.go -package mocks

package searcher

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"sort"
	"strings"

	"github.com/mitchellh/mapstructure"
	"github.com/yl2chen/cidranger"
	"go.uber.org/zap"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/pkg/math"
	"d7y.io/dragonfly/v2/pkg/types"
)

const (
	// Condition IDC key.
	ConditionIDC = "idc"

	// Condition location key.
	ConditionLocation = "location"
)

const (
	// cidrAffinityWeight is CIDR affinity weight.
	cidrAffinityWeight float64 = 0.3

	// hostnameAffinityWeight is hostname affinity weight.
	hostnameAffinityWeight = 0.3

	// idcAffinityWeight is IDC affinity weight.
	idcAffinityWeight float64 = 0.25

	// locationAffinityWeight is location affinity weight.
	locationAffinityWeight = 0.14

	// clusterTypeWeight is cluster type weight.
	clusterTypeWeight float64 = 0.01
)

const (
	// Maximum score.
	maxScore float64 = 1.0

	// Minimum score.
	minScore = 0
)

const (
	// Maximum number of elements.
	maxElementLen = 5
)

// Scheduler cluster scopes.
type Scopes struct {
	IDC       string   `mapstructure:"idc"`
	Location  string   `mapstructure:"location"`
	CIDRs     []string `mapstructure:"cidrs"`
	Hostnames []string `mapstructure:"hostnames"`
}

type Searcher interface {
	// FindSchedulerClusters finds scheduler clusters that best matches the evaluation.
	FindSchedulerClusters(ctx context.Context, schedulerClusters []models.SchedulerCluster, ip, hostname string,
		conditions map[string]string, log *zap.SugaredLogger) ([]models.SchedulerCluster, error)
}

type searcher struct{}

func New(pluginDir string) Searcher {
	s, err := LoadPlugin(pluginDir)
	if err != nil {
		logger.Info("use default searcher")
		return &searcher{}
	}

	logger.Info("use searcher plugin")
	return s
}

// FindSchedulerClusters finds scheduler clusters that best matches the evaluation.
func (s *searcher) FindSchedulerClusters(ctx context.Context, schedulerClusters []models.SchedulerCluster, ip, hostname string,
	conditions map[string]string, log *zap.SugaredLogger) ([]models.SchedulerCluster, error) {
	log = log.With("ip", ip, "hostname", hostname, "conditions", conditions)

	if len(schedulerClusters) <= 0 {
		return nil, errors.New("empty scheduler clusters")
	}

	clusters := FilterSchedulerClusters(conditions, schedulerClusters)
	if len(clusters) == 0 {
		return nil, fmt.Errorf("conditions %#v does not match any scheduler cluster", conditions)
	}

	sort.Slice(
		clusters,
		func(i, j int) bool {
			var si, sj Scopes
			if err := mapstructure.Decode(clusters[i].Scopes, &si); err != nil {
				log.Errorf("cluster %s decode scopes failed: %v", clusters[i].Name, err)
				return false
			}

			if err := mapstructure.Decode(clusters[j].Scopes, &sj); err != nil {
				log.Errorf("cluster %s decode scopes failed: %v", clusters[i].Name, err)
				return false
			}

			return Evaluate(ip, hostname, conditions, si, clusters[i], log) > Evaluate(ip, hostname, conditions, sj, clusters[j], log)
		},
	)

	return clusters, nil
}

// Filter the scheduler clusters that dfdaemon can be used.
func FilterSchedulerClusters(conditions map[string]string, schedulerClusters []models.SchedulerCluster) []models.SchedulerCluster {
	var clusters []models.SchedulerCluster
	for _, schedulerCluster := range schedulerClusters {
		// There are no active schedulers in the scheduler cluster
		if len(schedulerCluster.Schedulers) == 0 {
			continue
		}

		clusters = append(clusters, schedulerCluster)
	}

	return clusters
}

// Evaluate the degree of matching between scheduler cluster and dfdaemon.
func Evaluate(ip, hostname string, conditions map[string]string, scopes Scopes, cluster models.SchedulerCluster, log *zap.SugaredLogger) float64 {
	return cidrAffinityWeight*calculateCIDRAffinityScore(ip, scopes.CIDRs, log) +
		hostnameAffinityWeight*calculateHostnameAffinityScore(hostname, scopes.Hostnames, log) +
		idcAffinityWeight*calculateIDCAffinityScore(conditions[ConditionIDC], scopes.IDC) +
		locationAffinityWeight*calculateMultiElementAffinityScore(conditions[ConditionLocation], scopes.Location) +
		clusterTypeWeight*calculateClusterTypeScore(cluster)
}

// calculateCIDRAffinityScore 0.0~1.0 larger and better.
func calculateCIDRAffinityScore(ip string, cidrs []string, log *zap.SugaredLogger) float64 {
	// Construct CIDR ranger.
	ranger := cidranger.NewPCTrieRanger()
	for _, cidr := range cidrs {
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			log.Error(err)
			continue
		}

		if err := ranger.Insert(cidranger.NewBasicRangerEntry(*network)); err != nil {
			log.Error(err)
			continue
		}
	}

	// Determine whether an IP is contained in the constructed networks ranger.
	contains, err := ranger.Contains(net.ParseIP(ip))
	if err != nil {
		log.Error(err)
		return minScore
	}

	if !contains {
		return minScore
	}

	return maxScore
}

// calculateHostnameAffinityScore 0.0~1.0 larger and better.
func calculateHostnameAffinityScore(hostname string, hostnames []string, log *zap.SugaredLogger) float64 {
	if hostname == "" {
		return minScore
	}

	if len(hostnames) == 0 {
		return minScore
	}

	for _, v := range hostnames {
		regex, err := regexp.Compile(v)
		if err != nil {
			log.Error(err)
			continue
		}

		if regex.MatchString(hostname) {
			return maxScore
		}
	}

	return minScore
}

// calculateIDCAffinityScore 0.0~1.0 larger and better.
func calculateIDCAffinityScore(dst, src string) float64 {
	if dst == "" || src == "" {
		return minScore
	}

	if strings.EqualFold(dst, src) {
		return maxScore
	}

	// Dst has only one element, src has multiple elements separated by "|".
	// When dst element matches one of the multiple elements of src,
	// it gets the max score of idc.
	srcElements := strings.Split(src, types.AffinitySeparator)
	for _, srcElement := range srcElements {
		if strings.EqualFold(dst, srcElement) {
			return maxScore
		}
	}

	return minScore
}

// calculateMultiElementAffinityScore 0.0~1.0 larger and better.
func calculateMultiElementAffinityScore(dst, src string) float64 {
	if dst == "" || src == "" {
		return minScore
	}

	if strings.EqualFold(dst, src) {
		return maxScore
	}

	// Calculate the number of multi-element matches divided by "|".
	var score, elementLen int
	dstElements := strings.Split(dst, types.AffinitySeparator)
	srcElements := strings.Split(src, types.AffinitySeparator)
	elementLen = math.Min(len(dstElements), len(srcElements))

	// Maximum element length is 5.
	if elementLen > maxElementLen {
		elementLen = maxElementLen
	}

	for i := 0; i < elementLen; i++ {
		if !strings.EqualFold(dstElements[i], srcElements[i]) {
			break
		}

		score++
	}

	return float64(score) / float64(maxElementLen)
}

// calculateClusterTypeScore 0.0~1.0 larger and better.
func calculateClusterTypeScore(cluster models.SchedulerCluster) float64 {
	if cluster.IsDefault {
		return maxScore
	}

	return minScore
}
