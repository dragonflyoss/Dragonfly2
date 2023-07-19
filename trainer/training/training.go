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
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gocarina/gocsv"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/math"
	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/pkg/types"
	schedulerstorage "d7y.io/dragonfly/v2/scheduler/storage"
	"d7y.io/dragonfly/v2/trainer/config"
	"d7y.io/dragonfly/v2/trainer/storage"
)

//go:generate mockgen -destination mocks/training_mock.go -source training.go -package mocks

const (
	// MLPObservationFilePrefix is prefix of mlp observation file name.
	MLPObservationFilePrefix = "mlp_data"

	// GNNVertexObservationFilePrefix is gnn vertex observation of file name.
	GNNVertexObservationFilePrefix = "gnn_vertex_data"

	// GNNEdgeObservationFilePrefix is prefix of gnn edge observation file name.
	GNNEdgeObservationFilePrefix = "gnn_edge_data"

	// CSVFileExt is extension of file name.
	CSVFileExt = "csv"

	// locationSeparator is location separator.
	locationSeparator = "|"
)

const (
	// Maximum score.
	maxScore float64 = 1

	// Minimum score.
	minScore = 0
)

const (
	// Maximum number of elements.
	maxElementLen = 3
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

	// Storage interface.
	storage storage.Storage

	// Manager service clent.
	managerClient managerclient.V2
}

// New returns a new Training.
func New(cfg *config.Config, baseDir string, managerClient managerclient.V2, storage storage.Storage) Training {
	return &training{
		config:        cfg,
		baseDir:       baseDir,
		storage:       storage,
		managerClient: managerClient,
	}
}

// Train begins training GNN and MLP model.
func (t *training) Train(ctx context.Context, ip, hostname string) error {
	// Preprocess training data.
	_, _, _, err := t.preprocess(ip, hostname)
	if err != nil {
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
func (t *training) preprocess(ip, hostname string) (int, int, int, error) {
	var (
		mlpObservationRecordCount       int
		gnnVertexObservationRecordCount int
		gnnEdgeObservationRecordCount   int
		bandwidths                      = make(map[string]float64)
		hostID                          = idgen.HostIDV2(ip, hostname)
	)

	logger.Info(t.mlpObservationFilename(hostID))
	logger.Info(t.gnnVertexObservationFilename(hostID))
	logger.Info(t.gnnEdgeObservationFilename(hostID))

	downloadFile, err := t.storage.OpenDownload(hostID)
	if err != nil {
		msg := fmt.Sprintf("open download failed: %s", err.Error())
		logger.Error(msg)
		return 0, 0, 0, status.Error(codes.Internal, msg)
	}
	defer downloadFile.Close()

	networkTopologyFile, err := t.storage.OpenNetworkTopology(hostID)
	if err != nil {
		msg := fmt.Sprintf("open network topology failed: %s", err.Error())
		logger.Error(msg)
		return 0, 0, 0, status.Error(codes.Internal, msg)
	}
	defer networkTopologyFile.Close()

	// Get the maximum bandwidth of each edge.
	dc := make(chan schedulerstorage.Download)
	go func() {
		if gocsv.UnmarshalToChanWithoutHeaders(downloadFile, dc) != nil {
			logger.Errorf("prase download file filed: %s", err.Error())
		}
	}()

	for download := range dc {
		for _, parent := range download.Parents {
			logger.Info(parent)
			var maxBandwidth float64
			key := pkgredis.MakeBandwidthKeyInTrainer(download.Host.ID, parent.ID)
			value, ok := bandwidths[key]
			if ok {
				maxBandwidth = value
			}

			for _, piece := range parent.Pieces {
				var bandwidth float64
				if piece.Cost > 0 {
					bandwidth = float64(piece.Length / piece.Cost)
				}

				if bandwidth > maxBandwidth {
					maxBandwidth = bandwidth
				}
			}

			bandwidths[key] = maxBandwidth
		}
	}

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		// Preprocess download training data.
		dc := make(chan schedulerstorage.Download)
		go func() {
			if gocsv.UnmarshalToChanWithoutHeaders(downloadFile, dc) != nil {
				logger.Errorf("prase download file filed: %s", err.Error())
			}
		}()

		for download := range dc {
			for _, parent := range download.Parents {
				key := pkgredis.MakeBandwidthKeyInTrainer(download.Host.ID, parent.ID)
				value, ok := bandwidths[key]
				if !ok {
					value = 0
				}

				logger.Info(parent)
				if err := t.createMLPObservation(hostID, MLPObservation{
					FinishedPieceScore:    calculatePieceScore(parent.FinishedPieceCount, download.FinishedPieceCount, download.Task.TotalPieceCount),
					FreeUploadScore:       calculateFreeUploadScore(parent.Host.ConcurrentUploadCount, parent.Host.ConcurrentUploadLimit),
					UploadSuccessScore:    calculateParentHostUploadSuccessScore(parent.Host.UploadCount, parent.Host.UploadFailedCount),
					IDCAffinityScore:      calculateIDCAffinityScore(parent.Host.Network.IDC, download.Host.Network.IDC),
					LocationAffinityScore: calculateMultiElementAffinityScore(parent.Host.Network.Location, download.Host.Network.Location),
					MaxBandwidth:          value,
				}); err != nil {
					logger.Errorf("create MLP Edge observation failed: %s", err.Error())
				}

				mlpObservationRecordCount++
			}
		}

		wg.Done()
	}()

	go func() {
		// Preprocess network topology training data.
		ntc := make(chan schedulerstorage.NetworkTopology)
		for networkTopology := range ntc {
			for _, destHost := range networkTopology.DestHosts {
				key := pkgredis.MakeBandwidthKeyInTrainer(networkTopology.Host.ID, destHost.ID)
				value, ok := bandwidths[key]
				if !ok {
					value = 0
				}

				if err := t.createGNNEdgeObservation(hostID, GNNEdgeObservation{
					SrcHostID:    networkTopology.Host.ID,
					DestHostID:   destHost.ID,
					AverageRTT:   destHost.Probes.AverageRTT,
					MaxBandwidth: value,
				}); err != nil {
					logger.Errorf("create GNN Edge observation failed: %s", err.Error())
					continue
				}

				gnnEdgeObservationRecordCount++
			}

			ipFeature, err := ipFeature(networkTopology.Host.IP)
			if err != nil {
				logger.Errorf("convert IP to feature failed: %s", err.Error())
				continue
			}

			if err := t.createGNNVertexObservation(hostID, GNNVertexObservation{
				HostID:   networkTopology.Host.ID,
				IP:       ipFeature,
				Location: locationFeature(networkTopology.Host.Network.Location),
				IDC:      idcFeature(networkTopology.Host.Network.IDC),
			}); err != nil {
				logger.Errorf("create GNN vertex observation failed: %s", err.Error())
				continue
			}

			gnnVertexObservationRecordCount++
		}

		wg.Done()
	}()

	wg.Wait()
	return mlpObservationRecordCount, gnnVertexObservationRecordCount, gnnEdgeObservationRecordCount, nil
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
	// TODO: Train MLP model.

	// TODO: Upload MLP model to manager.
	// if err := t.managerClient.CreateModel(ctx, &manager.CreateModelRequest{
	// 	Hostname: hostname,
	// 	Ip:       ip,
	// 	Request: &manager.CreateModelRequest_CreateMlpRequest{
	// 		CreateMlpRequest: &manager.CreateMLPRequest{
	// 			Data: []byte{},
	// 			Mse:  0,
	// 			Mae:  0,
	// 		},
	// 	},
	// }); err != nil {
	// 	logger.Error("upload MLP model to manager error: %v", err.Error())
	// 	return err
	// }

	return nil
}

// createMLPObservation inserts the MLP observations into csv file.
func (t *training) createMLPObservation(key string, observations ...MLPObservation) error {
	file, err := os.OpenFile(t.mlpObservationFilename(key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := gocsv.MarshalWithoutHeaders(observations, file); err != nil {
		return err
	}

	return nil
}

// createGNNVertexObservation inserts the GNN vertex observations into csv file.
func (t *training) createGNNVertexObservation(key string, observations ...GNNVertexObservation) error {
	file, err := os.OpenFile(t.gnnVertexObservationFilename(key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := gocsv.MarshalWithoutHeaders(observations, file); err != nil {
		return err
	}

	return nil
}

// createGNNEdgeObservation inserts the GNN edge observations into csv file.
func (t *training) createGNNEdgeObservation(key string, observations ...GNNEdgeObservation) error {
	file, err := os.OpenFile(t.gnnEdgeObservationFilename(key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := gocsv.MarshalWithoutHeaders(observations, file); err != nil {
		return err
	}

	return nil
}

// clearMLPObservation removes mlp observation file.
func (t *training) clearMLPObservation(key string) error {
	return os.Remove(t.mlpObservationFilename(key))
}

// clearGNNVertexObservation removes gnn vertex observation file.
func (t *training) clearGNNVertexObservation(key string) error {
	return os.Remove(t.gnnVertexObservationFilename(key))
}

// clearGNNEdgeObservation removes gnn edge observation file.
func (t *training) clearGNNEdgeObservation(key string) error {
	return os.Remove(t.gnnEdgeObservationFilename(key))
}

// mlpObservationFilename generates mlp observation file name based on the given key.
func (t *training) mlpObservationFilename(key string) string {
	return filepath.Join(t.baseDir, fmt.Sprintf("%s_%s.%s", MLPObservationFilePrefix, key, CSVFileExt))
}

// gnnVertexObservationFilename generates gnn vertex observation file name based on the given key.
func (t *training) gnnVertexObservationFilename(key string) string {
	return filepath.Join(t.baseDir, fmt.Sprintf("%s_%s.%s", GNNVertexObservationFilePrefix, key, CSVFileExt))
}

// gnnEdgeObservationFilename generates gnn edge observation file name based on the given key.
func (t *training) gnnEdgeObservationFilename(key string) string {
	return filepath.Join(t.baseDir, fmt.Sprintf("%s_%s.%s", GNNEdgeObservationFilePrefix, key, CSVFileExt))
}

// networkTopologyFilename generates network topology file name based on the given model key.
func calculatePieceScore(parentFinishedPieceCount, childFinishedPieceCount, totalPieceCount int32) float64 {
	// If the total piece is determined, normalize the number of
	// pieces downloaded by the parent node.
	if totalPieceCount > 0 {
		return float64(parentFinishedPieceCount) / float64(totalPieceCount)
	}

	// Use the difference between the parent node and the child node to
	// download the piece to roughly represent the piece score.
	return float64(parentFinishedPieceCount) - float64(childFinishedPieceCount)
}

// calculateParentHostUploadSuccessScore 0.0~unlimited larger and better.
func calculateParentHostUploadSuccessScore(uploadCount, uploadFailedCount int64) float64 {
	if uploadCount < uploadFailedCount {
		return minScore
	}

	// Host has not been scheduled, then it is scheduled first.
	if uploadCount == 0 && uploadFailedCount == 0 {
		return maxScore
	}

	return float64(uploadCount-uploadFailedCount) / float64(uploadCount)
}

// calculateFreeUploadScore 0.0~1.0 larger and better.
func calculateFreeUploadScore(concurrentUploadLimit, concurrentUploadCount int32) float64 {
	freeUploadCount := concurrentUploadLimit - concurrentUploadCount
	if concurrentUploadLimit > 0 && freeUploadCount > 0 {
		return float64(freeUploadCount) / float64(concurrentUploadLimit)
	}

	return minScore
}

// calculateIDCAffinityScore 0.0~1.0 larger and better.
func calculateIDCAffinityScore(dst, src string) float64 {
	if dst != "" && src != "" && dst == src {
		return maxScore
	}

	return minScore
}

// calculateMultiElementAffinityScore 0.0~1.0 larger and better.
func calculateMultiElementAffinityScore(dst, src string) float64 {
	if dst == "" || src == "" {
		return minScore
	}

	if dst == src {
		return maxScore
	}

	// Calculate the number of multi-element matches divided by "|".
	var score, elementLen int
	dstElements := strings.Split(dst, types.AffinitySeparator)
	srcElements := strings.Split(src, types.AffinitySeparator)
	elementLen = math.Min(len(dstElements), len(srcElements))

	// Maximum element length is 3.
	if elementLen > maxElementLen {
		elementLen = maxElementLen
	}

	for i := 0; i < elementLen; i++ {
		if dstElements[i] != srcElements[i] {
			break
		}
		score++
	}

	return float64(score) / float64(maxElementLen)
}

// ipFeature convert an ip address to a feature vector.
func ipFeature(data string) ([]uint32, error) {
	ip := net.IP(data)
	var features = make([]uint32, net.IPv6len)
	if l := len(ip); l != net.IPv4len && l != net.IPv6len {
		msg := fmt.Sprintf("invalid IP address: %s", ip)
		logger.Error(msg)
		return features, errors.New(msg)
	}

	if ip.To4() != nil {
		for i := 0; i < net.IPv4len; i++ {
			features[i] = uint32(ip[i])
		}
	}

	if ip.To16() != nil {
		for i := 0; i < net.IPv6len; i++ {
			features[i] = uint32(ip[i])
		}
	}

	return features, nil
}

// locationFeature converts location to a feature vector.
func locationFeature(location string) []uint32 {
	locs := strings.Split(location, locationSeparator)
	var features = make([]uint32, maxElementLen)
	for i, part := range locs {
		features[i] = hash(part)
	}

	return features
}

// idcFeature converts idc to a feature vector.
func idcFeature(idc string) []uint32 {
	return []uint32{hash(idc)}
}

// hash convert string to uint64.
func hash(s string) uint32 {
	h := sha1.New()
	h.Write([]byte(s))

	return binary.LittleEndian.Uint32(h.Sum(nil))
}
