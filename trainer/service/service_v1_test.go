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

package service

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/emptypb"

	trainerv1 "d7y.io/api/pkg/apis/trainer/v1"
	trainerv1mocks "d7y.io/api/pkg/apis/trainer/v1/mocks"

	"d7y.io/dragonfly/v2/trainer/config"
	storagemocks "d7y.io/dragonfly/v2/trainer/storage/mocks"
	trainingmocks "d7y.io/dragonfly/v2/trainer/training/mocks"
)

var (
	mockHostName  = "localhost"
	mockIP        = "127.0.0.1"
	mockClusterID = uint64(1)
	mockDataset   = []byte("foo")
)

func TestService_NewV1(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, s any)
	}{
		{
			name: "new service",
			expect: func(t *testing.T, s any) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "V1")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			storage := storagemocks.NewMockStorage(ctl)
			training := trainingmocks.NewMockTraining(ctl)
			tc.expect(t, NewV1(&config.Config{}, storage, training))
		})
	}
}

func TestServiceV1_Train(t *testing.T) {
	tests := []struct {
		name string
		mock func(
			mt *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
			storage *storagemocks.MockStorageMockRecorder,
			training *trainingmocks.MockTrainingMockRecorder,
			downloads *os.File, networktopologies *os.File, mockModelKey string)
		expect func(t *testing.T, err error)
	}{
		{
			name: "receive error at the time of beginning",
			mock: func(
				mt *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				storage *storagemocks.MockStorageMockRecorder,
				training *trainingmocks.MockTrainingMockRecorder,
				downloads *os.File, networktopologies *os.File, mockModelKey string,
			) {
				gomock.InOrder(
					mt.Recv().Return(nil, errors.New("receive error at the time of beginning")).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "receive error at the time of beginning")
			},
		},
		{
			name: "receive GNN train requests successfully",
			mock: func(
				mt *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				storage *storagemocks.MockStorageMockRecorder,
				training *trainingmocks.MockTrainingMockRecorder,
				downloads *os.File, networktopologies *os.File, mockModelKey string,
			) {
				gomock.InOrder(
					mt.Recv().Return(&trainerv1.TrainRequest{
						Hostname:  mockHostName,
						Ip:        mockIP,
						ClusterId: mockClusterID,
						Request: &trainerv1.TrainRequest_TrainGnnRequest{
							TrainGnnRequest: &trainerv1.TrainGNNRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),
					storage.OpenNetworkTopology(mockModelKey).Return(networktopologies, nil).Times(1),
					mt.Recv().Return(nil, io.EOF).Times(1),
					mt.SendAndClose(new(emptypb.Empty)).Return(nil).Times(1),
					training.GNNTrain().Return(nil).Times(1),
					storage.ClearNetworkTopology(mockModelKey).Return(nil),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "receive GNN train requests successfully but send and close failed",
			mock: func(
				mt *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				storage *storagemocks.MockStorageMockRecorder,
				training *trainingmocks.MockTrainingMockRecorder,
				downloads *os.File, networktopologies *os.File, mockModelKey string,
			) {
				gomock.InOrder(
					mt.Recv().Return(&trainerv1.TrainRequest{
						Hostname:  mockHostName,
						Ip:        mockIP,
						ClusterId: mockClusterID,
						Request: &trainerv1.TrainRequest_TrainGnnRequest{
							TrainGnnRequest: &trainerv1.TrainGNNRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),
					storage.OpenNetworkTopology(mockModelKey).Return(networktopologies, nil).Times(1),
					mt.Recv().Return(nil, io.EOF).Times(1),
					mt.SendAndClose(new(emptypb.Empty)).Return(errors.New("send and close error")).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "send and close error")
			},
		},
		{
			name: "receive GNN train requests successfully but train GNN failed",
			mock: func(
				mt *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				storage *storagemocks.MockStorageMockRecorder,
				training *trainingmocks.MockTrainingMockRecorder,
				downloads *os.File, networktopologies *os.File, mockModelKey string,
			) {
				gomock.InOrder(
					mt.Recv().Return(&trainerv1.TrainRequest{
						Hostname:  mockHostName,
						Ip:        mockIP,
						ClusterId: mockClusterID,
						Request: &trainerv1.TrainRequest_TrainGnnRequest{
							TrainGnnRequest: &trainerv1.TrainGNNRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),
					storage.OpenNetworkTopology(mockModelKey).Return(networktopologies, nil).Times(1),
					mt.Recv().Return(nil, io.EOF).Times(1),
					mt.SendAndClose(new(emptypb.Empty)).Return(nil).Times(1),
					training.GNNTrain().Return(errors.New("train GNN failed")))
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "train GNN failed")
			},
		},
		{
			name: "receive GNN train requests successfully but clear the file of network topologies failed",
			mock: func(
				mt *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				storage *storagemocks.MockStorageMockRecorder,
				training *trainingmocks.MockTrainingMockRecorder,
				downloads *os.File, networktopologies *os.File, mockModelKey string,
			) {
				gomock.InOrder(
					mt.Recv().Return(&trainerv1.TrainRequest{
						Hostname:  mockHostName,
						Ip:        mockIP,
						ClusterId: mockClusterID,
						Request: &trainerv1.TrainRequest_TrainGnnRequest{
							TrainGnnRequest: &trainerv1.TrainGNNRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),
					storage.OpenNetworkTopology(mockModelKey).Return(networktopologies, nil).Times(1),
					mt.Recv().Return(nil, io.EOF).Times(1),
					mt.SendAndClose(new(emptypb.Empty)).Return(nil).Times(1),
					training.GNNTrain().Return(nil).Times(1),
					storage.ClearNetworkTopology(mockModelKey).Return(errors.New("clear the file of network topologies error")),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "clear the file of network topologies error")
			},
		},
		{
			name: "open the file of network topologies failed when receive GNN train requests",
			mock: func(
				mt *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				storage *storagemocks.MockStorageMockRecorder,
				training *trainingmocks.MockTrainingMockRecorder,
				downloads *os.File, networktopologies *os.File, mockModelKey string,
			) {
				gomock.InOrder(
					mt.Recv().Return(&trainerv1.TrainRequest{
						Hostname:  mockHostName,
						Ip:        mockIP,
						ClusterId: mockClusterID,
						Request: &trainerv1.TrainRequest_TrainGnnRequest{
							TrainGnnRequest: &trainerv1.TrainGNNRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),
					storage.OpenNetworkTopology(mockModelKey).Return(nil, errors.New("open the file of network topologies error")).Times(1),
					storage.ClearNetworkTopology(mockModelKey).Return(nil),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "open the file of network topologies error")
			},
		},
		{
			name: "open and clear the file of network topologies failed when receive GNN train requests",
			mock: func(
				mt *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				storage *storagemocks.MockStorageMockRecorder,
				training *trainingmocks.MockTrainingMockRecorder,
				downloads *os.File, networktopologies *os.File, mockModelKey string,
			) {
				gomock.InOrder(
					mt.Recv().Return(&trainerv1.TrainRequest{
						Hostname:  mockHostName,
						Ip:        mockIP,
						ClusterId: mockClusterID,
						Request: &trainerv1.TrainRequest_TrainGnnRequest{
							TrainGnnRequest: &trainerv1.TrainGNNRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),
					storage.OpenNetworkTopology(mockModelKey).Return(nil, errors.New("open the file of network topologies error")).Times(1),
					storage.ClearNetworkTopology(mockModelKey).Return(errors.New("clear the file of network topologies error")),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "clear the file of network topologies error")
			},
		},
		{
			name: "receive MLP train requests successfully",
			mock: func(
				mt *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				storage *storagemocks.MockStorageMockRecorder,
				training *trainingmocks.MockTrainingMockRecorder,
				downloads *os.File, networktopologies *os.File, mockModelKey string,
			) {
				gomock.InOrder(
					mt.Recv().Return(&trainerv1.TrainRequest{
						Hostname:  mockHostName,
						Ip:        mockIP,
						ClusterId: mockClusterID,
						Request: &trainerv1.TrainRequest_TrainMlpRequest{
							TrainMlpRequest: &trainerv1.TrainMLPRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),
					storage.OpenDownload(mockModelKey).Return(downloads, nil).Times(1),
					mt.Recv().Return(nil, io.EOF).Times(1),
					mt.SendAndClose(new(emptypb.Empty)).Return(nil).Times(1),
					training.MLPTrain().Return(nil).Times(1),
					storage.ClearDownload(mockModelKey).Return(nil),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "receive MLP train requests successfully but send and close failed",
			mock: func(
				mt *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				storage *storagemocks.MockStorageMockRecorder,
				training *trainingmocks.MockTrainingMockRecorder,
				downloads *os.File, networktopologies *os.File, mockModelKey string,
			) {
				gomock.InOrder(
					mt.Recv().Return(&trainerv1.TrainRequest{
						Hostname:  mockHostName,
						Ip:        mockIP,
						ClusterId: mockClusterID,
						Request: &trainerv1.TrainRequest_TrainMlpRequest{
							TrainMlpRequest: &trainerv1.TrainMLPRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),
					storage.OpenDownload(mockModelKey).Return(downloads, nil).Times(1),
					mt.Recv().Return(nil, io.EOF).Times(1),
					mt.SendAndClose(new(emptypb.Empty)).Return(errors.New("send and close error")).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "send and close error")
			},
		},
		{
			name: "receive MLP train requests successfully but train MLP failed",
			mock: func(
				mt *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				storage *storagemocks.MockStorageMockRecorder,
				training *trainingmocks.MockTrainingMockRecorder,
				downloads *os.File, networktopologies *os.File, mockModelKey string,
			) {
				gomock.InOrder(
					mt.Recv().Return(&trainerv1.TrainRequest{
						Hostname:  mockHostName,
						Ip:        mockIP,
						ClusterId: mockClusterID,
						Request: &trainerv1.TrainRequest_TrainMlpRequest{
							TrainMlpRequest: &trainerv1.TrainMLPRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),
					storage.OpenDownload(mockModelKey).Return(downloads, nil).Times(1),
					mt.Recv().Return(nil, io.EOF).Times(1),
					mt.SendAndClose(new(emptypb.Empty)).Return(nil).Times(1),
					training.MLPTrain().Return(errors.New("train MLP failed")),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "train MLP failed")
			},
		},
		{
			name: "receive MLP train requests successfully but clear the file of downloads failed",
			mock: func(
				mt *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				storage *storagemocks.MockStorageMockRecorder,
				training *trainingmocks.MockTrainingMockRecorder,
				downloads *os.File, networktopologies *os.File, mockModelKey string,
			) {
				gomock.InOrder(
					mt.Recv().Return(&trainerv1.TrainRequest{
						Hostname:  mockHostName,
						Ip:        mockIP,
						ClusterId: mockClusterID,
						Request: &trainerv1.TrainRequest_TrainMlpRequest{
							TrainMlpRequest: &trainerv1.TrainMLPRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),
					storage.OpenDownload(mockModelKey).Return(downloads, nil).Times(1),
					mt.Recv().Return(nil, io.EOF).Times(1),
					mt.SendAndClose(new(emptypb.Empty)).Return(nil).Times(1),
					training.MLPTrain().Return(nil).Times(1),
					storage.ClearDownload(mockModelKey).Return(errors.New("clear the file of downloads error")).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "clear the file of downloads error")
			},
		},
		{
			name: "open the file of downloads failed when handle MLP train requests",
			mock: func(
				mt *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				storage *storagemocks.MockStorageMockRecorder,
				training *trainingmocks.MockTrainingMockRecorder,
				downloads *os.File, networktopologies *os.File, mockModelKey string,
			) {
				gomock.InOrder(
					mt.Recv().Return(&trainerv1.TrainRequest{
						Hostname:  mockHostName,
						Ip:        mockIP,
						ClusterId: mockClusterID,
						Request: &trainerv1.TrainRequest_TrainMlpRequest{
							TrainMlpRequest: &trainerv1.TrainMLPRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),
					storage.OpenDownload(mockModelKey).Return(nil, errors.New("open the file of downloads error")).Times(1),
					storage.ClearDownload(mockModelKey).Return(nil),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "open the file of downloads error")
			},
		},
		{
			name: "open and clear the file of downloads failed when handle MLP train requests",
			mock: func(
				mt *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				storage *storagemocks.MockStorageMockRecorder,
				training *trainingmocks.MockTrainingMockRecorder,
				downloads *os.File, networktopologies *os.File, mockModelKey string,
			) {
				gomock.InOrder(
					mt.Recv().Return(&trainerv1.TrainRequest{
						Hostname:  mockHostName,
						Ip:        mockIP,
						ClusterId: mockClusterID,
						Request: &trainerv1.TrainRequest_TrainMlpRequest{
							TrainMlpRequest: &trainerv1.TrainMLPRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),
					storage.OpenDownload(mockModelKey).Return(nil, errors.New("open the file of downloads error")).Times(1),
					storage.ClearDownload(mockModelKey).Return(errors.New("clear the file of downloads error")),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "clear the file of downloads error")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			storage := storagemocks.NewMockStorage(ctl)
			stream := trainerv1mocks.NewMockTrainer_TrainServer(ctl)
			training := trainingmocks.NewMockTraining(ctl)

			svc := NewV1(&config.Config{}, storage, training)
			mockModelKey, err := svc.createModelKey(mockHostName, mockIP, uint(mockClusterID), DefaultHashAlgorithm)
			if err != nil {
				t.Fatal(err)
			}

			downloads, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "download", mockModelKey, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
			if err != nil {
				t.Fatal(err)
			}
			defer downloads.Close()

			networktopologies, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "networktopology", mockModelKey, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
			if err != nil {
				t.Fatal(err)
			}
			defer networktopologies.Close()

			tc.mock(stream.EXPECT(), storage.EXPECT(), training.EXPECT(), downloads, networktopologies, mockModelKey)
			tc.expect(t, svc.Train(stream))

			if err = os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "download", mockModelKey, "csv"))); err != nil {
				t.Fatal(err)
			}

			if err = os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "networktopology", mockModelKey, "csv"))); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestServiceV1_clearFile(t *testing.T) {
	tests := []struct {
		name    string
		reqType RequestType
		mock    func(storage *storagemocks.MockStorageMockRecorder, modelKey string)
		expect  func(t *testing.T, err error, reqType RequestType, modelKey string)
	}{
		{
			name:    "Clear download files",
			reqType: TrainMLPRequest,
			mock: func(storage *storagemocks.MockStorageMockRecorder, modelKey string) {
				_, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "download", modelKey, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}

				gomock.InOrder(
					storage.ClearDownload(modelKey).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, err error, reqType RequestType, modelKey string) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name:    "Clear download files failed",
			reqType: TrainMLPRequest,
			mock: func(storage *storagemocks.MockStorageMockRecorder, modelKey string) {
				gomock.InOrder(
					storage.ClearDownload(modelKey).Return(errors.New("clear download files failed")).Times(1),
				)
			},
			expect: func(t *testing.T, err error, reqType RequestType, modelKey string) {
				assert := assert.New(t)
				assert.EqualError(err, "clear download files failed")
			},
		},
		{
			name:    "Clear network topology files",
			reqType: TrainGNNRequest,
			mock: func(storage *storagemocks.MockStorageMockRecorder, modelKey string) {
				_, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "networktopology", modelKey, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}

				gomock.InOrder(
					storage.ClearNetworkTopology(modelKey).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, err error, reqType RequestType, modelKey string) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name:    "Clear network topology files failed",
			reqType: TrainGNNRequest,
			mock: func(storage *storagemocks.MockStorageMockRecorder, modelKey string) {
				gomock.InOrder(
					storage.ClearNetworkTopology(modelKey).Return(errors.New("clear network topology files failed")).Times(1),
				)
			},
			expect: func(t *testing.T, err error, reqType RequestType, modelKey string) {
				assert := assert.New(t)
				assert.EqualError(err, "clear network topology files failed")
			},
		},
		{
			name:    "receive unknown request",
			reqType: 3,
			mock:    func(storage *storagemocks.MockStorageMockRecorder, modelKey string) {},
			expect: func(t *testing.T, err error, reqType RequestType, modelKey string) {
				assert := assert.New(t)
				assert.EqualError(err, "rpc error: code = FailedPrecondition desc = receive unknown request: 0x3")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			storage := storagemocks.NewMockStorage(ctl)
			training := trainingmocks.NewMockTraining(ctl)
			svc := NewV1(&config.Config{}, storage, training)
			mockModelKey, _ := svc.createModelKey(mockHostName, mockIP, uint(mockClusterID), DefaultHashAlgorithm)

			tc.mock(storage.EXPECT(), mockModelKey)
			tc.expect(t, svc.clearFile(mockModelKey, tc.reqType), tc.reqType, mockModelKey)
		})
	}
}

func TestServiceV1_handleTrainGNNRequest(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(storage *storagemocks.MockStorageMockRecorder, modelKey string)
		expect func(t *testing.T, err error, modelKey string)
	}{
		{
			name: "Handle GNN train request",
			mock: func(storage *storagemocks.MockStorageMockRecorder, modelKey string) {
				file, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "networktopology", modelKey, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}

				gomock.InOrder(
					storage.OpenNetworkTopology(modelKey).Return(file, nil).Times(1),
				)
			},
			expect: func(t *testing.T, err error, modelKey string) {
				assert := assert.New(t)
				assert.NoError(err)
				if err = os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "networktopology", modelKey, "csv"))); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "Open file failed",
			mock: func(storage *storagemocks.MockStorageMockRecorder, modelKey string) {
				gomock.InOrder(
					storage.OpenNetworkTopology(modelKey).Return(nil, errors.New("open file failed")).Times(1),
				)
			},
			expect: func(t *testing.T, err error, modelKey string) {
				assert := assert.New(t)
				assert.EqualError(err, "open file failed")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			storage := storagemocks.NewMockStorage(ctl)
			training := trainingmocks.NewMockTraining(ctl)
			svc := NewV1(&config.Config{}, storage, training)
			mockModelKey, _ := svc.createModelKey(mockHostName, mockIP, uint(mockClusterID), DefaultHashAlgorithm)

			tc.mock(storage.EXPECT(), mockModelKey)
			tc.expect(t, svc.handleTrainGNNRequest(mockModelKey, mockDataset), mockModelKey)
		})
	}
}

func TestServiceV1_handleTrainMLPRequest(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(storage *storagemocks.MockStorageMockRecorder, modelKey string)
		expect func(t *testing.T, err error, modelKey string)
	}{
		{
			name: "Handle MLP train request",
			mock: func(storage *storagemocks.MockStorageMockRecorder, modelKey string) {
				file, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "download", modelKey, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}

				gomock.InOrder(
					storage.OpenDownload(modelKey).Return(file, nil).Times(1),
				)
			},
			expect: func(t *testing.T, err error, modelKey string) {
				assert := assert.New(t)
				assert.NoError(err)
				if err = os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "download", modelKey, "csv"))); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "Open file failed",
			mock: func(storage *storagemocks.MockStorageMockRecorder, modelKey string) {
				gomock.InOrder(
					storage.OpenDownload(modelKey).Return(nil, errors.New("open file failed")).Times(1),
				)
			},
			expect: func(t *testing.T, err error, modelKey string) {
				assert := assert.New(t)
				assert.EqualError(err, "open file failed")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			storage := storagemocks.NewMockStorage(ctl)
			training := trainingmocks.NewMockTraining(ctl)
			svc := NewV1(&config.Config{}, storage, training)
			mockModelKey, _ := svc.createModelKey(mockHostName, mockIP, uint(mockClusterID), DefaultHashAlgorithm)

			tc.mock(storage.EXPECT(), mockModelKey)
			tc.expect(t, svc.handleTrainMLPRequest(mockModelKey, mockDataset), mockModelKey)
		})
	}
}

func TestServiceV1_CreateModelKey(t *testing.T) {
	tests := []struct {
		name          string
		hashAlgorithm string
		expect        func(t *testing.T, svc *V1, hashAlgorithm string)
	}{
		{
			name:          "create model key using sha1",
			hashAlgorithm: "sha1",
			expect: func(t *testing.T, svc *V1, hashAlgorithm string) {
				assert := assert.New(t)
				modelKey, err := svc.createModelKey(mockHostName, mockIP, uint(mockClusterID), hashAlgorithm)
				assert.NoError(err)
				assert.Equal("c00f0b440d1e75e45eec5ba505480e0d41bfca62", modelKey)
			},
		},
		{
			name:          "create model key using sha512",
			hashAlgorithm: "sha512",
			expect: func(t *testing.T, svc *V1, hashAlgorithm string) {
				assert := assert.New(t)
				modelKey, err := svc.createModelKey(mockHostName, mockIP, uint(mockClusterID), hashAlgorithm)
				assert.NoError(err)
				assert.Equal("25dec121f82b545bdfe0d7476a2fce7ace8f8d98c3d8207b5a9fe89e359a0f56af55d1d343a4cf5c00a1bb3252cc51fc528f5c7a97f757533022f5243244f4fa", modelKey)
			},
		},
		{
			name:          "create model key using sha256",
			hashAlgorithm: "sha256",
			expect: func(t *testing.T, svc *V1, hashAlgorithm string) {
				assert := assert.New(t)
				modelKey, err := svc.createModelKey(mockHostName, mockIP, uint(mockClusterID), hashAlgorithm)
				assert.NoError(err)
				assert.Equal("29ceb2b30ad4b010f64408df8950cced152b43ae20d5c2c202e08679e097314a", modelKey)
			},
		},
		{
			name:          "create model key using md5",
			hashAlgorithm: "md5",
			expect: func(t *testing.T, svc *V1, hashAlgorithm string) {
				assert := assert.New(t)
				modelKey, err := svc.createModelKey(mockHostName, mockIP, uint(mockClusterID), hashAlgorithm)
				assert.NoError(err)
				assert.Equal("321287159ebf8006811a88dd40f25cf3", modelKey)
			},
		},
		{
			name:          "create model key using unsupported hash algorithm",
			hashAlgorithm: "baz",
			expect: func(t *testing.T, svc *V1, hashAlgorithm string) {
				assert := assert.New(t)
				modelKey, err := svc.createModelKey(mockHostName, mockIP, uint(mockClusterID), hashAlgorithm)
				assert.EqualError(err, "unsupport hash method: baz")
				assert.Equal("", modelKey)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			storage := storagemocks.NewMockStorage(ctl)
			training := trainingmocks.NewMockTraining(ctl)

			svc := NewV1(&config.Config{}, storage, training)
			tc.expect(t, svc, tc.hashAlgorithm)
		})
	}
}
