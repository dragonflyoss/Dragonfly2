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
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"

	tf "github.com/galeone/tensorflow/tensorflow/go"
	"github.com/gocarina/gocsv"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"d7y.io/api/v2/pkg/apis/manager/v2"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/idgen"
	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	schedulerstorage "d7y.io/dragonfly/v2/scheduler/storage"
	"d7y.io/dragonfly/v2/trainer/config"
	"d7y.io/dragonfly/v2/trainer/storage"
)

//go:generate mockgen -destination mocks/training_mock.go -source training.go -package mocks

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

	// Manager service client.
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

				// Divide the data into training set or test set.
				if rand.Float32() < PartitionRatio {
					if err := createMLPObservationTrain(t.baseDir, hostID, mlpObservation); err != nil {
						logger.Errorf("create MLP observation for training failed: %s", err.Error())
					}
				} else {
					if err := createMLPObservationTest(t.baseDir, hostID, mlpObservation); err != nil {
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

				if err := createGNNEdgeObservation(t.baseDir, hostID, GNNEdgeObservation{
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

		if err := createGNNVertexObservation(t.baseDir, hostID, GNNVertexObservation{
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
	if err = mlpTrainingPhase(t.baseDir, hostID, model); err != nil {
		return err
	}

	// Testing phase.
	MAE, MSE, err := mlpTestingPhase(t.baseDir, hostID, model)
	if err != nil {
		return err
	}

	// Save and compress MLP model.
	if err = saveMLPModel(t.baseDir, hostID, model); err != nil {
		return err
	}

	// Compress MLP trained model file and convert to bytes.
	data, err := compressFile(mlpModelDirFile(t.baseDir, hostID))
	if err != nil {
		return err
	}

	// Upload MLP model to manager.
	if err := t.managerClient.CreateModel(ctx, &manager.CreateModelRequest{
		Hostname: hostname,
		Ip:       ip,
		Request: &manager.CreateModelRequest_CreateMlpRequest{
			CreateMlpRequest: &manager.CreateMLPRequest{
				Data: data,
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

// TODO: optimize.
// compressFile compresses trained model directory and convert to bytes.
func compressFile(dir string) ([]byte, error) {
	// create a gzip compressor.
	buf := new(bytes.Buffer)
	gzWriter := gzip.NewWriter(buf)
	defer gzWriter.Close()

	// create a zip compressor.
	zipWriter := zip.NewWriter(gzWriter)
	defer zipWriter.Close()

	// iterate through all the files and subdirectories in the directory and add them to the zip compressor.
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// create a zip file header.
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}

		// set the zip file header name.
		header.Name = path

		// If the current file is a directory, add the zip file header directly.
		if info.IsDir() {
			_, err = zipWriter.CreateHeader(header)
			return err
		}

		// If the current file is a normal file, open it and add its contents to the zip compressor.
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		entry, err := zipWriter.CreateHeader(header)
		if err != nil {
			return err
		}

		_, err = io.Copy(entry, file)
		return err
	}); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
