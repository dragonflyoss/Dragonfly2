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

	"github.com/gocarina/gocsv"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"d7y.io/api/pkg/apis/manager/v2"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/math"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/pkg/types"
	schedulerstorage "d7y.io/dragonfly/v2/scheduler/storage"
	"d7y.io/dragonfly/v2/trainer/config"
	"d7y.io/dragonfly/v2/trainer/storage"
)

//go:generate mockgen -destination mocks/training_mock.go -source training.go -package mocks

const (
	MLPObservationFilename = "mlp-data.csv"

	GNNVertexObservationFilename = "gnn-vertex-data.csv"

	GNNEdgeObservationFilename = "gnn-edge-data.csv"

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
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return t.trainGNN(ctx, ip, hostname)
	})

	eg.Go(func() error {
		return t.trainMLP(ctx, ip, hostname)
	})

	// Wait for all train tasks to complete.
	if err := eg.Wait(); err != nil {
		logger.Errorf("training failed: %v", err)
		return err
	}

	// Clean up training data.
	if err := t.storage.Clear(); err != nil {
		logger.Errorf("remove network topology and download file failed: %v", err)
		return err
	}

	return nil
}

// trainGNN trains GNN model.
func (t *training) trainGNN(ctx context.Context, ip, hostname string) error {
	var hostID = idgen.HostIDV2(ip, hostname)
	// Get training data from storage.
	downloadFile, err := t.storage.OpenDownload(hostID)
	if err != nil {
		msg := fmt.Sprintf("open download failed: %s", err.Error())
		logger.Error(msg)
		return status.Error(codes.Internal, msg)
	}
	defer downloadFile.Close()

	// Preprocess training data.
	dc := make(chan schedulerstorage.Download)
	go func() {
		// start parsing download file.
		if err := gocsv.UnmarshalToChanWithoutHeaders(downloadFile, dc); err != nil {
			logger.Error("parse download file error: %s", err.Error())
		}
	}()

	// Traverse download file to get the maximum bandwidth of the edge.
	// Store the maximum bandwidth of each edge in the map as a feature of the GNN training edge.
	var bandwidths = make(map[string]float64)
	for downloadRecord := range dc {
		for _, parent := range downloadRecord.Parents {
			var maxBandwidth float64
			key := fmt.Sprint(downloadRecord.Host.ID + ":" + parent.ID)
			value, ok := bandwidths[key]
			if ok {
				maxBandwidth = value
			}

			for _, piece := range parent.Pieces {
				bandwidth := float64(piece.Length / piece.Cost)
				if bandwidth > maxBandwidth {
					maxBandwidth = bandwidth
				}
			}

			bandwidths[key] = maxBandwidth
		}
	}

	networkTopologyFile, err := t.storage.OpenNetworkTopology(hostID)
	if err != nil {
		msg := fmt.Sprintf("open network topology failed: %s", err.Error())
		logger.Error(msg)
		return status.Error(codes.Internal, msg)
	}
	defer networkTopologyFile.Close()

	ntc := make(chan schedulerstorage.NetworkTopology)
	go func() {
		// Start parsing download file.
		if err := gocsv.UnmarshalToChanWithoutHeaders(networkTopologyFile, ntc); err != nil {
			logger.Error("parse network topology file error: %s", err.Error())
		}
	}()

	for networkTopologyRecord := range ntc {
		ipFeature, err := ipFeature(networkTopologyRecord.Host.IP)
		if err != nil {
			logger.Error("convert IP to feature error: %s", err.Error())
			continue
		}

		if err := t.createGNNVertexObservation(GNNVertexObservation{
			HostID:   networkTopologyRecord.Host.ID,
			IP:       ipFeature,
			Location: locationFeature(networkTopologyRecord.Host.Network.Location),
			IDC:      idcFeature(networkTopologyRecord.Host.Network.IDC),
		}); err != nil {
			logger.Error("create GNN vertex observation error: %s", err.Error())
		}

		for _, destHost := range networkTopologyRecord.DestHosts {
			key := fmt.Sprint(networkTopologyRecord.Host.ID + ":" + destHost.ID)
			value, ok := bandwidths[key]
			if !ok {
				value = 0
			}

			if err := t.createGNNEdgeObservation(GNNEdgeObservation{
				SrcHostID:    networkTopologyRecord.Host.ID,
				DestHostID:   destHost.ID,
				AverageRTT:   destHost.Probes.AverageRTT,
				MaxBandwidth: value,
			}); err != nil {
				logger.Error("create GNN Edge observation error: %s", err.Error())
			}
		}
	}

	// TODO: Train GNN model.
	// Upload MLP model to manager service.
	if err := t.managerClient.CreateModel(ctx, &manager.CreateModelRequest{
		Hostname: hostname,
		Ip:       ip,
		Request: &manager.CreateModelRequest_CreateGnnRequest{
			CreateGnnRequest: &manager.CreateGNNRequest{
				Data:      []byte{},
				Recall:    0,
				Precision: 0,
				F1Score:   0,
			},
		},
	}); err != nil {
		logger.Error("upload GNN model to manager error: %s", err.Error())
		return err
	}

	return nil
}

// trainMLP trains MLP model.
func (t *training) trainMLP(ctx context.Context, ip, hostname string) error {
	var hostID = idgen.HostIDV2(ip, hostname)

	// Get training data from storage.
	downloadFile, err := t.storage.OpenDownload(hostID)
	if err != nil {
		msg := fmt.Sprintf("open download failed: %s", err.Error())
		logger.Error(msg)
		return status.Error(codes.Internal, msg)
	}
	defer downloadFile.Close()

	// Preprocess training data.
	dc := make(chan schedulerstorage.Download)
	go func() {
		// start parsing download file.
		if err := gocsv.UnmarshalToChanWithoutHeaders(downloadFile, dc); err != nil {
			logger.Error("parse download file error: %s", err.Error())
		}
	}()

	for downloadRecord := range dc {
		for _, parent := range downloadRecord.Parents {
			var maxBandwidth float64
			for _, piece := range parent.Pieces {
				bandwidth := float64(piece.Length / piece.Cost)
				if bandwidth > maxBandwidth {
					maxBandwidth = bandwidth
				}
			}

			if err := t.createMLPObservation(MLPObservation{
				FinishedPieceScore:    calculatePieceScore(parent.UploadPieceCount, downloadRecord.Task.TotalPieceCount),
				FreeUploadScore:       calculateFreeUploadScore(parent.Host.ConcurrentUploadCount, parent.Host.ConcurrentUploadLimit),
				UploadSuccessScore:    calculateParentHostUploadSuccessScore(parent.Host.UploadCount, parent.Host.UploadFailedCount),
				IDCAffinityScore:      calculateIDCAffinityScore(parent.Host.Network.IDC, downloadRecord.Host.Network.IDC),
				LocationAffinityScore: calculateMultiElementAffinityScore(parent.Host.Network.Location, downloadRecord.Host.Network.Location),
				MaxBandwidth:          maxBandwidth,
			}); err != nil {
				logger.Error("create MLP Edge observation error: %s", err.Error())
			}
		}
	}

	// TODO: Train MLP model.
	// Upload MLP model to manager service.
	if err := t.managerClient.CreateModel(ctx, &manager.CreateModelRequest{
		Hostname: hostname,
		Ip:       ip,
		Request: &manager.CreateModelRequest_CreateMlpRequest{
			CreateMlpRequest: &manager.CreateMLPRequest{
				Data: []byte{},
				Mse:  0,
				Mae:  0,
			},
		},
	}); err != nil {
		logger.Error("upload MLP model to manager error: %s", err.Error())
		return err
	}

	return nil
}

// createMLPObservation inserts the MLP observations into csv file.
func (t *training) createMLPObservation(observations ...MLPObservation) error {
	file, err := os.OpenFile(filepath.Join(t.baseDir, MLPObservationFilename), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
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
func (t *training) createGNNVertexObservation(observations ...GNNVertexObservation) error {
	file, err := os.OpenFile(filepath.Join(t.baseDir, GNNVertexObservationFilename), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
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
func (t *training) createGNNEdgeObservation(observations ...GNNEdgeObservation) error {
	file, err := os.OpenFile(filepath.Join(t.baseDir, GNNEdgeObservationFilename), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := gocsv.MarshalWithoutHeaders(observations, file); err != nil {
		return err
	}

	return nil
}

// cleanMLPObservation removes mlp observation file.
func (t *training) cleanMLPObservation() error {
	return os.Remove(MLPObservationFilename)
}

// cleanGNNVertexObservation removes gnn vertex observation file.
func (t *training) cleanGNNVertexObservation() error {
	return os.Remove(GNNVertexObservationFilename)
}

// cleanGNNEdgeObservation removes gnn edge observation file.
func (t *training) cleanGNNEdgeObservation() error {
	return os.Remove(GNNEdgeObservationFilename)
}

// calculatePieceScore 0.0~unlimited larger and better.
func calculatePieceScore(uploadPieceCount, totalPieceCount int32) float64 {
	// If the total piece is determined, normalize the number of
	// pieces downloaded by the parent node.
	if totalPieceCount > 0 {
		return float64(uploadPieceCount) / float64(totalPieceCount)
	}

	return minScore
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

	// Maximum element length is 5.
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
	loc := strings.Split(location, locationSeparator)
	features := make([]uint32, maxElementLen)
	for i, part := range loc {
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
