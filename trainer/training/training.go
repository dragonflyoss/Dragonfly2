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
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"

	tf "github.com/galeone/tensorflow/tensorflow/go"
	"github.com/gocarina/gocsv"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"d7y.io/api/v2/pkg/apis/manager/v2"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/idgen"
	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	schedulerstorage "d7y.io/dragonfly/v2/scheduler/storage"
	"d7y.io/dragonfly/v2/trainer/config"
	"d7y.io/dragonfly/v2/trainer/storage"
)

//go:generate mockgen -destination mocks/training_mock.go -source training.go -package mocks

const (
	// PartitionRatio is the partition ratio between the training set and the test set.
	PartitionRatio = 0.8

	// Batch size for each optimization step for MLP model.
	defaultMLPBatchSize int = 32

	// The number of epochs for training MLP model.
	defaultMLPEpochCount int = 10

	// The dimension of score Tensor for MLP model's inputs.
	defaultMLPScoreDims int = 5
)

const (
	// Batch size for each optimization step for GNN model.
	defaultGNNBatchSize int = 500

	// The number of training step of training GNN model.
	defaultGNNTrainingStep = 1000

	// The number of negative sample size of GNN model.
	defaultNegativeSampleSize = 25
)

// Training defines the interface to train GNN and MLP model.
type Training interface {
	// Train begins training GNN and MLP model.
	Train(context.Context, string, string) error
}

// training implements Training interface.
type training struct {
	// Trainer service config.
	config *config.Config

	// Base directory.
	baseDir string

	// observation interface.
	obs observation

	// Storage interface.
	storage storage.Storage

	// Manager service client.
	managerClient managerclient.V2
}

// New returns a new Training.
func New(cfg *config.Config, baseDir string, managerClient managerclient.V2, storage storage.Storage) Training {
	return &training{
		config:        cfg,
		baseDir:       baseDir,
		obs:           *NewObservation(baseDir),
		storage:       storage,
		managerClient: managerClient,
	}
}

// Train begins training GNN and MLP model.
func (t *training) Train(ctx context.Context, ip, hostname string) error {
	// Preprocess training data.
	if err := t.preprocess(ip, hostname); err != nil {
		logger.Errorf("preprocess failed: %s", err.Error())
		return err
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return t.trainGNN(ctx, ip, hostname)
	})

	eg.Go(func() error {
		return t.trainMLP(ctx, ip, hostname)
	})

	// Wait for all train tasks to complete.
	if err := eg.Wait(); err != nil {
		logger.Errorf("training failed: %s", err.Error())
		return err
	}

	// Clean up training data.
	// var hostID = idgen.HostIDV2(ip, hostname)
	// if err := t.storage.ClearDownload(hostID); err != nil {
	// 	logger.Errorf("remove download file failed: %v", err)
	// 	return err
	// }

	// if err := t.storage.ClearNetworkTopology(hostID); err != nil {
	// 	logger.Errorf("remove network topology file failed: %v", err)
	// 	return err
	// }

	// if err := t.clearMLPObservation(hostID); err != nil {
	// 	logger.Errorf("remove mlp observation file failed: %v", err)
	// 	return err
	// }

	// if err := t.clearGNNVertexObservation(hostID); err != nil {
	// 	logger.Errorf("remove gnn vertex observation file failed: %v", err)
	// 	return err
	// }

	// if err := t.clearGNNEdgeObservation(hostID); err != nil {
	// 	logger.Errorf("remove gnn edge observation file failed: %v", err)
	// 	return err
	// }

	return nil
}

// preprocess constructs mlp observation, gnn vertex observation and edge observation before training.
func (t *training) preprocess(ip, hostname string) error {
	var hostID = idgen.HostIDV2(ip, hostname)
	downloadFile, err := t.storage.OpenDownload(hostID)
	if err != nil {
		msg := fmt.Sprintf("open download failed: %s", err.Error())
		logger.Error(msg)
		return status.Error(codes.Internal, msg)
	}
	defer downloadFile.Close()

	networkTopologyFile, err := t.storage.OpenNetworkTopology(hostID)
	if err != nil {
		msg := fmt.Sprintf("open network topology failed: %s", err.Error())
		logger.Error(msg)
		return status.Error(codes.Internal, msg)
	}
	defer networkTopologyFile.Close()

	var bandwidths = make(map[string]float64)
	// Preprocess download training data.
	dc := make(chan schedulerstorage.Download)
	go func() {
		if gocsv.UnmarshalToChanWithoutHeaders(downloadFile, dc) != nil {
			logger.Errorf("prase download file filed: %s", err.Error())
		}
	}()

	for download := range dc {
		for _, parent := range download.Parents {
			if parent.ID != "" {
				// get maxBandwidth locally from pieces.
				var localMaxBandwidth float64
				for _, piece := range parent.Pieces {
					if piece.Cost > 0 && float64(piece.Length/piece.Cost) > localMaxBandwidth {
						localMaxBandwidth = float64(piece.Length / piece.Cost)
					}
				}

				// updata maxBandwidth globally.
				key := pkgredis.MakeBandwidthKeyInTrainer(download.Host.ID, parent.Host.ID)
				if value, ok := bandwidths[key]; ok {
					if localMaxBandwidth > value {
						bandwidths[key] = localMaxBandwidth
					}
				} else {
					bandwidths[key] = localMaxBandwidth
				}

				mlpObservation := MLPObservation{
					FinishedPieceScore:    calculatePieceScore(parent.FinishedPieceCount, download.FinishedPieceCount, download.Task.TotalPieceCount),
					FreeUploadScore:       calculateFreeUploadScore(parent.Host.ConcurrentUploadCount, parent.Host.ConcurrentUploadLimit),
					UploadSuccessScore:    calculateParentHostUploadSuccessScore(parent.Host.UploadCount, parent.Host.UploadFailedCount),
					IDCAffinityScore:      calculateIDCAffinityScore(parent.Host.Network.IDC, download.Host.Network.IDC),
					LocationAffinityScore: calculateMultiElementAffinityScore(parent.Host.Network.Location, download.Host.Network.Location),
					MaxBandwidth:          bandwidths[key],
				}

				// Divide the data into train set or test set.
				if rand.Float32() < PartitionRatio {
					if err := t.obs.createMLPObservationTrain(hostID, mlpObservation); err != nil {
						logger.Errorf("create MLP observation for training failed: %s", err.Error())
					}
				} else {
					if err := t.obs.createMLPObservationTest(hostID, mlpObservation); err != nil {
						logger.Errorf("create MLP observation for testing failed: %s", err.Error())
					}
				}

			}
		}
	}

	// Preprocess network topology training data.
	ntc := make(chan schedulerstorage.NetworkTopology)
	go func() {
		if gocsv.UnmarshalToChanWithoutHeaders(networkTopologyFile, ntc) != nil {
			logger.Errorf("prase network topology file filed: %s", err.Error())
		}
	}()

	for networkTopology := range ntc {
		for _, destHost := range networkTopology.DestHosts {
			if destHost.ID != "" {
				key := pkgredis.MakeBandwidthKeyInTrainer(networkTopology.Host.ID, destHost.ID)
				maxBandwidth, ok := bandwidths[key]
				if !ok {
					maxBandwidth = 0
				}

				if err := t.obs.createGNNEdgeObservation(hostID, GNNEdgeObservation{
					SrcHostID:    networkTopology.Host.ID,
					DestHostID:   destHost.ID,
					AverageRTT:   destHost.Probes.AverageRTT,
					MaxBandwidth: maxBandwidth,
				}); err != nil {
					logger.Errorf("create GNN Edge observation failed: %s", err.Error())
					continue
				}
			}
		}

		ipFeature, err := ipFeature(networkTopology.Host.IP)
		if err != nil {
			logger.Errorf("convert IP to feature failed: %s", err.Error())
			continue
		}

		if err := t.obs.createGNNVertexObservation(hostID, GNNVertexObservation{
			HostID:   networkTopology.Host.ID,
			IP:       ipFeature,
			Location: locationFeature(networkTopology.Host.Network.Location),
			IDC:      idcFeature(networkTopology.Host.Network.IDC),
		}); err != nil {
			logger.Errorf("create GNN vertex observation failed: %s", err.Error())
			continue
		}
	}

	return nil
}

// trainGNN trains GNN model.
func (t *training) trainGNN(ctx context.Context, ip, hostname string) error {
	// TODO: Train GNN model.

	// TODO: Upload MLP model to manager.
	// if err := t.managerClient.CreateModel(ctx, &manager.CreateModelRequest{
	// 	Hostname: hostname,
	// 	Ip:       ip,
	// 	Request: &manager.CreateModelRequest_CreateGnnRequest{
	// 		CreateGnnRequest: &manager.CreateGNNRequest{
	// 			Data:      []byte{},
	// 			Recall:    0,
	// 			Precision: 0,
	// 			F1Score:   0,
	// 		},
	// 	},
	// }); err != nil {
	// 	logger.Error("upload GNN model to manager error: %v", err.Error())
	// 	return err
	// }

	return nil
}

// trainMLP trains MLP model.
func (t *training) trainMLP(ctx context.Context, ip, hostname string) error {
	hostID := idgen.HostIDV2(ip, hostname)
	model, err := tf.LoadSavedModel(MLPInitialModelPath, []string{"serve"}, nil)
	if err != nil {
		return err
	}

	// Training phase.
	// Open MLP observation training file.
	mlpObservationTrainFile, err := t.obs.openMLPObservationTrainFile(hostID)
	if err != nil {
		return err
	}
	defer mlpObservationTrainFile.Close()

	trainReader := csv.NewReader(mlpObservationTrainFile)
	for i := 0; i < defaultMLPEpochCount; i++ {
		for {
			// Read MLP observation training file by line.
			record, err := trainReader.Read()
			if err == io.EOF {
				// Reset MLP observation training file in line.
				mlpObservationTrainFile.Seek(0, 0)
				break
			}

			if err != nil {
				return err
			}

			var MLPBatchSize = defaultMLPBatchSize
			if len(record) < defaultMLPBatchSize {
				MLPBatchSize = len(record)
			}

			var (
				// Initialize score batch and target batch for training.
				scoresBatch = make([][]float64, 0, MLPBatchSize)
				targetBatch = make([]float64, 0, MLPBatchSize)
				scores      = make([]float64, MLPBatchSize-1)
			)

			// Add scores and target of each MLP observation to score batch and target batch.
			for j, v := range record[:MLPBatchSize-1] {
				if scores[j], err = strconv.ParseFloat(v, 64); err != nil {
					return err
				}
			}
			scoresBatch = append(scoresBatch, scores)

			target, err := strconv.ParseFloat(record[MLPBatchSize-1], 64)
			if err != nil {
				return err
			}
			targetBatch = append(targetBatch, target)

			if len(scoresBatch) == MLPBatchSize {
				// Convert MLP observations to Tensorflow tensor.
				scoreTensor, err := tf.NewTensor(scoresBatch)
				if err != nil {
					return err
				}

				targetTensor, err := tf.NewTensor(targetBatch)
				if err != nil {
					return err
				}

				// Run an optimization step on the model using batch of values from observation file.
				if err := mlpLearning(model, scoreTensor, targetTensor); err != nil {
					return err
				}

				// Reset score batch and target batch for further training phase.
				scoresBatch = make([][]float64, 0, MLPBatchSize)
				targetBatch = make([]float64, 0, MLPBatchSize)
			}
		}
	}

	// Testing phase.
	// Initialize accumulated MSE, MAE and test observation count.
	var (
		accMSE    float64
		accMAE    float64
		testCount int
	)

	// Open MLP observation testing file.
	mlpObservationTestFile, err := t.obs.openMLPObservationTestFile(hostID)
	if err != nil {
		return err
	}
	defer mlpObservationTestFile.Close()

	testReader := csv.NewReader(mlpObservationTestFile)
	for {
		// Read preprocessed file in line.
		record, err := testReader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		// Initialize score and target for testing.
		var (
			scores       = make([][]float64, 1)
			target       = make([]float64, 1)
			recordLength = len(record)
		)
		scores[0] = make([]float64, recordLength-1)

		for j, v := range record[:recordLength-1] {
			val, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return err
			}
			scores[0][j] = val
		}

		target[0], err = strconv.ParseFloat(record[recordLength-1], 64)
		if err != nil {
			return err
		}

		// Convert MLP observation to Tensorflow tensor.
		scoreTensor, err := tf.NewTensor(scores)
		if err != nil {
			return err
		}

		targetTensor, err := tf.NewTensor(target)
		if err != nil {
			return err
		}

		// Run test and receive avarage model metrics.
		MSE, MAE, err := mlpValidation(model, scoreTensor, targetTensor)
		if err != nil {
			return err
		}
		accMSE += MSE[0]
		accMAE += MAE[0]
		testCount++
	}

	MAE := accMAE / float64(testCount)
	MSE := accMSE / float64(testCount)
	logger.Infof("Metric-MAE: %f", MAE)
	logger.Infof("Metric-MSE: %f", MSE)

	// Create MLP trained model directory file for updating and uploading.
	if err := os.MkdirAll(t.obs.mlpModelDirFile(hostID), os.ModePerm); err != nil {
		return err
	}

	// Create trained MLP model variables directory file.
	if err := os.MkdirAll(t.obs.mlpModelVariablesFile(hostID), os.ModePerm); err != nil {
		return err
	}

	mlpInitialSavedModelFile, err := t.obs.openMLPInitialSavedModelFile()
	if err != nil {
		return err
	}
	defer mlpInitialSavedModelFile.Close()

	mlpSavedModelFile, err := t.obs.openMLPSavedModelFile(hostID)
	if err != nil {
		return err
	}
	defer mlpSavedModelFile.Close()

	// Copy MLP initial saved model file to MLP saved model file.
	if _, err := io.Copy(mlpSavedModelFile, mlpInitialSavedModelFile); err != nil {
		return err
	}

	// Update variables file.
	variableTensor, err := tf.NewTensor(t.obs.mlpModelVariablesFile(hostID))
	if err != nil {
		return err
	}

	if err := mlpSaving(model, variableTensor); err != nil {
		return err
	}

	// Compress MLP trained model file and convert to bytes.
	d, err := compressMLPFile(t.obs.mlpModelDirFile(hostID))
	if err != nil {
		return err
	}

	// Upload MLP model to manager.
	if err := t.managerClient.CreateModel(ctx, &manager.CreateModelRequest{
		Hostname: hostname,
		Ip:       ip,
		Request: &manager.CreateModelRequest_CreateMlpRequest{
			CreateMlpRequest: &manager.CreateMLPRequest{
				Data: d,
				Mse:  MSE,
				Mae:  MAE,
			},
		},
	}); err != nil {
		logger.Error("upload MLP model to manager error: %v", err.Error())
		return err
	}

	return nil
}

func (t *training) minibatch(ip, hostname string, sampleSizes int) error {
	// 1.
	// 先遍历vertex.csv 提取feature 向量 (ip,idc,location), 定位每个节点。
	// map [hostID]int hostID 对应一个int，不重复，用来存索引
	// 然后需要将特征存成matrix[[feature1],[feature1],[feature1]]

	// 2.
	// 遍历edge.csv 提取feature 向量 (rtt,bandwidth),
	// 存所有邻居 [][]int , 存index 下对应的host 的邻居。[[1,2,3][2,3]] 0:1,2,3 1:2,3

	// 3. 采样
	// for 循环 trainingStep = 1000
	// {
	// 所有点的列表 边的列表。
	// 随机1个batch_size的边 暂定超参数500边。
	// 随机选一个500条边的一条的两个节点作为 初始节点 和末节点，根据这些节点生成子图：500 （1,2）
	// 500条边的节点，把他们的一阶邻居包括本身放到集合A里。集合点全集-集合A=集合B，在 集合B random 元素作为集合C。然后集合A ∪ 集合C = 集合D
	// 负样本取值超参数25> 二阶参数。

	// 根据集合D 的点进行采样。
	//  集合D 记录其一阶 ，二阶的点，存起来。
	// for 循环集合D 的每个点
	// 	{记录 [[一阶][二阶]]
	//   }
	// 扔给 trainloop [][]int
	// }

	// construct GNN vertex feature matrix.
	var (
		hostID            = idgen.HostIDV2(ip, hostname)
		hostID2Index      = make(map[string]int)
		Index2HostID      = make(map[int]string)
		index             int
		gnnVertexFeatures = make([][]uint32, 0)
		universalHosts    = set.NewSafeSet[int]()
	)

	GNNVertexObservationFile, err := t.obs.openGNNVertexObservationFile(hostID)
	if err != nil {
		msg := fmt.Sprintf("open gnn vertex observation failed: %s", err.Error())
		logger.Error(msg)
		return status.Error(codes.Internal, msg)
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
			Index2HostID[index] = gnnVertexObservation.HostID

			universalHosts.Add(index)
			var gnnVertexFeature = make([]uint32, 0)
			gnnVertexFeature = append(gnnVertexFeature, gnnVertexObservation.IP...)
			gnnVertexFeature = append(gnnVertexFeature, gnnVertexObservation.Location...)
			gnnVertexFeature = append(gnnVertexFeature, gnnVertexObservation.IDC)
			gnnVertexFeatures = append(gnnVertexFeatures, gnnVertexFeature)
			index++
		}
	}

	// construct GNN edge neighbor matrix.
	var (
		rawNeighbors             = make(map[int][]int, 0)
		neighbors                = make([][]int, 0)
		N                        = len(hostID2Index)
		gnnEdgeBandwidthFeatures = make([][]float64, N)
		gnnEdgeRTTFeatures       = make([][]int64, N)
	)

	for i := 0; i < N; i++ {
		gnnEdgeBandwidthFeatures[i] = make([]float64, N)
		gnnEdgeRTTFeatures[i] = make([]int64, N)
	}

	GNNEdgeObservationFile, err := t.obs.openGNNEdgeObservationFile(hostID)
	if err != nil {
		msg := fmt.Sprintf("open gnn edge observation failed: %s", err.Error())
		logger.Error(msg)
		return status.Error(codes.Internal, msg)
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
		gnnEdgeBandwidthFeatures[srcIndex][destIndex] = gnnEdgeObservation.MaxBandwidth
		gnnEdgeRTTFeatures[srcIndex][destIndex] = gnnEdgeObservation.AverageRTT

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

	// sampling.
	// loop training step.
	for j := 0; j < defaultGNNTrainingStep; j++ {
		// random select edges, default batch size = 500 .
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

			if !flag[src][dest] {
				flag[src][dest] = true
				sampleCount++
			}

			if sampleCount >= defaultGNNBatchSize {
				break
			}
		}

		var (
			A        = set.NewSafeSet[int]()
			AN       = set.NewSafeSet[int]()
			B        = set.NewSafeSet[int]()
			BN       = set.NewSafeSet[int]()
			C        = set.NewSafeSet[int]()
			CR       = set.NewSafeSet[int]()
			D        = set.NewSafeSet[int]()
			subgraph = make([][]int, 0)
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
			firstOrder  = set.NewSafeSet[int]()
			secondOrder = set.NewSafeSet[int]()
		)

		// get first order hosts of each host in set D.aa
		for _, host := range D.Values() {
			for _, neighbor := range neighbors[host] {
				firstOrder.Add(neighbor)
			}

		}
		subgraph = append(subgraph, firstOrder.Values())

		// get second order hosts of each host in set D.aa
		for _, neighbor := range firstOrder.Values() {
			for _, nneighbor := range neighbors[neighbor] {
				secondOrder.Add(nneighbor)
			}
		}
		subgraph = append(subgraph, secondOrder.Values())

		// TODO: training loop by chan.
	}

	return nil
}
