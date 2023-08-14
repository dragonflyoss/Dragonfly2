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

package training

import (
	"fmt"
	"math/rand"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/math"
	"d7y.io/dragonfly/v2/pkg/slices"
	"github.com/gocarina/gocsv"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// Batch size for each optimization step for GNN model.
	defaultGNNBatchSize int = 512

	// The number of training step of training GNN model.
	defaultGNNTrainingStep = 1000

	// The number of negative sample size of GNN model.
	defaultNegativeSampleSize = 25

	// The number of sample number of first order in subgraph.
	defaultFirstOrderSampleSize = 25

	// The number of sample number of second order in subgraph.
	defaultSecondOrderSampleSize = 10
)

const (
	// GNNVertexObservationFilePrefix is gnn vertex observation of file name.
	GNNVertexObservationFilePrefix = "gnn_vertex_data"

	// GNNEdgeObservationFilePrefix is prefix of gnn edge observation file name.
	GNNEdgeObservationFilePrefix = "gnn_edge_data"
)

// gnnVertexFeature maps from host id to index and constructs GNN vertex feature matrix.
func gnnVertexFeature(baseBar, hostID string) (map[string]int, [][]uint32, error) {
	var (
		index             int
		hostID2Index      = make(map[string]int)
		gnnVertexFeatures = make([][]uint32, 0)
	)

	GNNVertexObservationFile, err := openGNNVertexObservationFile(baseBar, hostID)
	if err != nil {
		msg := fmt.Sprintf("open gnn vertex observation failed: %s", err.Error())
		logger.Error(msg)
		return nil, nil, status.Error(codes.Internal, msg)
	}
	defer GNNVertexObservationFile.Close()

	gvc := make(chan GNNVertexObservation)
	go func() {
		if gocsv.UnmarshalToChanWithoutHeaders(GNNVertexObservationFile, gvc) != nil {
			logger.Errorf("prase gnn vertex observation file filed: %s", err.Error())
		}
	}()

	for gnnVertexObservation := range gvc {
		_, ok := hostID2Index[gnnVertexObservation.HostID]
		if !ok {
			hostID2Index[gnnVertexObservation.HostID] = index

			var gnnVertexFeature = make([]uint32, 0)
			gnnVertexFeature = append(gnnVertexFeature, gnnVertexObservation.IP...)
			gnnVertexFeature = append(gnnVertexFeature, gnnVertexObservation.Location...)
			gnnVertexFeature = append(gnnVertexFeature, gnnVertexObservation.IDC)
			gnnVertexFeatures = append(gnnVertexFeatures, gnnVertexFeature)
			index++
		}
	}

	return hostID2Index, gnnVertexFeatures, nil
}

// gnnNeighbor constructs GNN vertex neighbor matrix.
func gnnNeighbor(baseBar, hostID string, hostID2Index map[string]int) ([][]int, [][]float64, [][]int64, error) {
	var (
		rawNeighbors     = make(map[int][]int, 0)
		neighbors        = make([][]int, 0)
		N                = len(hostID2Index)
		gnnEdgeBandwidth = make([][]float64, N)
		gnnEdgeRTT       = make([][]int64, N)
	)

	for i := 0; i < N; i++ {
		gnnEdgeBandwidth[i] = make([]float64, N)
		gnnEdgeRTT[i] = make([]int64, N)
	}

	GNNEdgeObservationFile, err := openGNNEdgeObservationFile(baseBar, hostID)
	if err != nil {
		msg := fmt.Sprintf("open gnn edge observation failed: %s", err.Error())
		logger.Error(msg)
		return nil, nil, nil, status.Error(codes.Internal, msg)
	}
	defer GNNEdgeObservationFile.Close()

	gec := make(chan GNNEdgeObservation)
	go func() {
		if gocsv.UnmarshalToChanWithoutHeaders(GNNEdgeObservationFile, gec) != nil {
			logger.Errorf("prase gnn edge observation file filed: %s", err.Error())
		}
	}()

	for gnnEdgeObservation := range gec {
		srcIndex, ok := hostID2Index[gnnEdgeObservation.SrcHostID]
		if !ok {
			continue
		}

		destIndex, ok := hostID2Index[gnnEdgeObservation.DestHostID]
		if !ok {
			continue
		}

		// update gnnEdgeBandwidthFeatures and gnnEdgeRTTFeatures
		gnnEdgeBandwidth[srcIndex][destIndex] = gnnEdgeObservation.MaxBandwidth
		gnnEdgeRTT[srcIndex][destIndex] = gnnEdgeObservation.AverageRTT

		// update neighbor.
		_, ok = rawNeighbors[srcIndex]
		if ok {
			rawNeighbors[srcIndex] = append(rawNeighbors[srcIndex], destIndex)
			continue
		}

		rawNeighbors[srcIndex] = []int{destIndex}
	}

	for i := 0; i < len(rawNeighbors); i++ {
		neighbors = append(neighbors, rawNeighbors[i])
	}

	return neighbors, gnnEdgeBandwidth, gnnEdgeRTT, err
}

func minibatch(baseDir, ip, hostname string, sampleSizes int) error {
	var hostID = idgen.HostIDV2(ip, hostname)
	// Get raw vertex features and host id index.
	// hostID2Index, gnnVertexFeatures, err := gnnVertexFeature(baseDir, hostID)
	// if err != nil {
	// 	return err
	// }
	hostID2Index, _, err := gnnVertexFeature(baseDir, hostID)
	if err != nil {
		return err
	}

	// Get each vertex neighbors and the bandwidth and rtt of each edge.
	// neighbors, gnnEdgeBandwidth, gnnEdgeRTT, err := gnnNeighbor(baseDir, hostID, hostID2Index)
	// if err != nil {
	// 	return err
	// }
	neighbors, _, _, err := gnnNeighbor(baseDir, hostID, hostID2Index)
	if err != nil {
		return err
	}

	// Construct universal host.
	var (
		universalHosts = set.NewSafeSet[int]()
		N              = len(hostID2Index)
		minibatchChan  = make(chan int)
	)

	for i := 0; i < N; i++ {
		universalHosts.Add(i)
	}

	// sampling.
	// loop training step.
	go func() {
		var count int
		for {
			if count >= defaultGNNTrainingStep {
				break
			}

			// randomly select defaultGNNBatchSize edges.
			var (
				flag        = make([][]bool, N)
				sampleCount int
			)

			for i := 0; i < N; i++ {
				flag[i] = make([]bool, N)
			}

			for {
				var (
					src  = rand.Intn(N)
					dest = rand.Intn(N)
				)

				// keep non-duplicate, not self edge and src probe dest.
				// TODO: need to optimize, because it's too complicated.
				if flag[src][dest] || src != dest || !slices.Contains(neighbors[src], dest) {
					continue
				}

				flag[src][dest] = true
				sampleCount++
				if sampleCount >= defaultGNNBatchSize {
					break
				}
			}

			var (
				A  = set.NewSafeSet[int]()
				AN = set.NewSafeSet[int]()
				B  = set.NewSafeSet[int]()
				BN = set.NewSafeSet[int]()
				C  = set.NewSafeSet[int]()
				CR = set.NewSafeSet[int]()
				D  = set.NewSafeSet[int]()
			)

			for i := 0; i < N; i++ {
				for j := 0; j < N; j++ {
					if flag[i][j] {
						// add source host to set A.
						A.Add(i)
						// add destination host to set B.
						B.Add(j)

						// add source neighbor host to set AN.
						for _, srcNeighbor := range neighbors[i] {
							AN.Add(srcNeighbor)
						}

						// add destination neighbor host to set BN.
						for _, destNeighbor := range neighbors[j] {
							BN.Add(destNeighbor)
						}

						break
					}
				}
			}

			// set C = universal set - set A - set AN - set B - set BN.
			for i := 0; i < N; i++ {
				if !A.Contains(i) && !AN.Contains(i) && !B.Contains(i) && !BN.Contains(i) {
					C.Add(i)
				}
			}

			// set CR is randomly get elements from set C.
			for _, negativeSample := range C.Values()[:defaultNegativeSampleSize] {
				CR.Add(negativeSample)
			}

			// set D is set A + set B + set CR
			for _, host := range A.Values() {
				D.Add(host)
			}

			for _, host := range B.Values() {
				D.Add(host)
			}

			for _, host := range CR.Values() {
				D.Add(host)
			}

			var (
				firstOrders  = make(map[int][]int)
				secondOrders = make(map[int][]int)
			)

			// get first order hosts of each host in set D.
			for _, host := range D.Values() {
				var firstOrder = make([]int, 0)
				// if the number of neighbors is equal or greater than default first order sample size, randomly choose neighbor samples.
				// Else complete the sampling number.
				for _, neighbor := range neighbors[host] {
					firstOrder = append(firstOrder, neighbor)
				}

				if len(firstOrder) < defaultFirstOrderSampleSize {
					for len(firstOrder) < defaultFirstOrderSampleSize {
						firstOrder = append(firstOrder, firstOrder[rand.Intn(len(firstOrder))])
					}
				}

				firstOrders[host] = firstOrder[:defaultFirstOrderSampleSize]
			}

			// get second order hosts of each host in set D.aa
			for _, host := range D.Values() {
				var secondOrder = make([]int, 0)
				// if the number of neighbors is equal or greater than default second order sample size, randomly choose neighbor samples.
				// Else complete the sampling number.
				for _, neighbor := range firstOrders[host] {
					for _, nneighbor := range neighbors[neighbor] {
						secondOrder = append(secondOrder, nneighbor)
					}
				}

				if len(secondOrder) < defaultSecondOrderSampleSize {
					for len(secondOrder) < defaultFirstOrderSampleSize {
						secondOrder = append(secondOrder, secondOrder[rand.Intn(len(secondOrder))])
					}
				}

				secondOrders[host] = secondOrder[:defaultSecondOrderSampleSize]
			}

			minibatchChan <- 1
		}
	}()

	// for minibatch := range minibatchChan {
	// 	// TODO: training loop by chan.
	// }

	return nil
}

func buildBatchFromEdges(edges [][]bool, N int, neighbors [][]int) {
	var (
		batchA          = make([]int, 0)
		batchB          = make([]int, 0)
		batchANeighbors = set.NewSafeSet[int]()
		batchBNeighbors = set.NewSafeSet[int]()
		possibleNegs    = set.NewSafeSet[int]()
		universalHosts  = set.NewSafeSet[int]()
		batchN          = set.NewSafeSet[int]()
		batchAll        = set.NewSafeSet[int]()
		dst2batchA      = make([]int, 0)
		dst2batchB      = make([]int, 0)
	)

	// Init universal hosts.
	for i := 0; i < N; i++ {
		universalHosts.Add(i)
	}

	for i := 0; i < N; i++ {
		for j := 0; j < N; j++ {
			if edges[i][j] {
				// add source host to batch A.
				batchA = append(batchA, i)
				// add destination host to batch B.
				batchB = append(batchB, j)

				// add source neighbor host to set AN.
				for _, srcNeighbor := range neighbors[i] {
					batchANeighbors.Add(srcNeighbor)
				}

				// add destination neighbor host to set BN.
				for _, destNeighbor := range neighbors[j] {
					batchBNeighbors.Add(destNeighbor)
				}

				break
			}
		}
	}

	// Set possibleNegs = universal set - set A - set AN - set B - set BN.
	for i := 0; i < N; i++ {
		if !slices.Contains(batchA, i) && !batchANeighbors.Contains(i) &&
			!slices.Contains(batchB, i) && !batchBNeighbors.Contains(i) {
			possibleNegs.Add(i)
		}
	}

	// batchN is the random non-repeating elements from possibleNegs.
	for _, negativeSample := range possibleNegs.Values()[:math.Min(defaultNegativeSampleSize, len(possibleNegs.Values()))] {
		batchN.Add(negativeSample)
	}

	// batchAll consists of the non-repeating elements of batchA, batchB and batchN.
	for _, index := range batchA {
		batchAll.Add(index)
	}

	for _, index := range batchB {
		batchAll.Add(index)
	}

	for _, index := range batchN.Values() {
		batchAll.Add(index)
	}

	batchAllS := batchAll.Values()
	// Find batchA index from batchAll.
	for _, e := range batchA {
		dst2batchA = append(dst2batchA, slices.Find(batchAllS, e))
	}

	// Find batchB index from batchAll.
	for _, e := range batchB {
		dst2batchB = append(dst2batchB, slices.Find(batchAllS, e))
	}

	// Construct bool dst2batchN true if elements in batchAll and batchN.
	var dst2batchN = make([]bool, batchAll.Len())
	for i, e := range batchAllS {
		if batchN.Contains(e) {
			dst2batchN[i] = true
			continue
		}

		dst2batchN[i] = false
	}

	buildBatchFromVetexes(batchAllS, neighbors)
}

// TODO
func buildBatchFromVetexes(batchAllS []int, neighbors [][]int) {
	// var (
	// 	dstsrc2dsts = make([]int, 0)
	// 	dstsrc2srcs = make([]int, 0)
	// 	difMatrix   = make([][]int, 0)
	// )

}
