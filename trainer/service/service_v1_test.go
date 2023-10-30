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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/emptypb"

	trainerv1 "d7y.io/api/v2/pkg/apis/trainer/v1"
	trainerv1mocks "d7y.io/api/v2/pkg/apis/trainer/v1/mocks"

	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/trainer/config"
	storagemocks "d7y.io/dragonfly/v2/trainer/storage/mocks"
	trainingmocks "d7y.io/dragonfly/v2/trainer/training/mocks"
)

var (
	mockHostName = "localhost"
	mockIP       = "127.0.0.1"
	mockHostID   = idgen.HostIDV2(mockIP, mockHostName)
	mockDataset  = []byte("foo")
)

func TestService_NewV1(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, s any)
	}{
		{
			name: "new service",
			run: func(t *testing.T, s any) {
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
			tc.run(t, NewV1(config.New(), storage, training))
		})
	}
}

func TestV1_Train(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, svc *V1, stream trainerv1.Trainer_TrainServer, mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
			ms *storagemocks.MockStorageMockRecorder, mt *trainingmocks.MockTrainingMockRecorder)
	}{
		{
			name: "receive GNN and MLP train requests success",
			run: func(t *testing.T, svc *V1, stream trainerv1.Trainer_TrainServer, mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				ms *storagemocks.MockStorageMockRecorder, mt *trainingmocks.MockTrainingMockRecorder) {
				networktopologyFile, err := os.OpenFile(filepath.Join(os.TempDir(),
					fmt.Sprintf("%s-%s.%s", "networktopology", mockHostID, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}

				downloadFile, err := os.OpenFile(filepath.Join(os.TempDir(),
					fmt.Sprintf("%s-%s.%s", "download", mockHostID, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}

				var wg sync.WaitGroup
				wg.Add(1)
				defer wg.Wait()
				gomock.InOrder(
					mtts.Recv().Return(&trainerv1.TrainRequest{
						Hostname: mockHostName,
						Ip:       mockIP,
						Request: &trainerv1.TrainRequest_TrainGnnRequest{
							TrainGnnRequest: &trainerv1.TrainGNNRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),

					ms.OpenNetworkTopology(mockHostID).Return(networktopologyFile, nil).Times(1),
					ms.OpenDownload(mockHostID).Return(downloadFile, nil).Times(1),
					mtts.Recv().Return(&trainerv1.TrainRequest{
						Hostname: mockHostName,
						Ip:       mockIP,
						Request: &trainerv1.TrainRequest_TrainMlpRequest{
							TrainMlpRequest: &trainerv1.TrainMLPRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),
					mtts.Recv().Return(nil, io.EOF).Times(1),
					mtts.SendAndClose(new(emptypb.Empty)).Return(nil).Times(1),
					mt.Train(context.Background(), mockIP, mockHostName).DoAndReturn(func(ctx context.Context, ip, hostName string) error {
						wg.Done()
						return nil
					}).Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.Train(stream))
			},
		},
		{
			name: "receive error",
			run: func(t *testing.T, svc *V1, stream trainerv1.Trainer_TrainServer, mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				ms *storagemocks.MockStorageMockRecorder, mt *trainingmocks.MockTrainingMockRecorder) {
				gomock.InOrder(
					mtts.Recv().Return(nil, errors.New("receive error")).Times(1),
				)

				assert := assert.New(t)
				assert.EqualError(svc.Train(stream), "receive error")
			},
		},
		{
			name: "open network topology file error",
			run: func(t *testing.T, svc *V1, stream trainerv1.Trainer_TrainServer, mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				ms *storagemocks.MockStorageMockRecorder, mt *trainingmocks.MockTrainingMockRecorder) {
				gomock.InOrder(
					mtts.Recv().Return(&trainerv1.TrainRequest{
						Hostname: mockHostName,
						Ip:       mockIP,
						Request: &trainerv1.TrainRequest_TrainGnnRequest{
							TrainGnnRequest: &trainerv1.TrainGNNRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),

					ms.OpenNetworkTopology(mockHostID).Return(nil, errors.New("open network topology file error")).Times(1),
				)

				assert := assert.New(t)
				assert.EqualError(svc.Train(stream),
					"rpc error: code = Internal desc = open network topology failed: open network topology file error")
			},
		},
		{
			name: "open download file error",
			run: func(t *testing.T, svc *V1, stream trainerv1.Trainer_TrainServer, mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				ms *storagemocks.MockStorageMockRecorder, mt *trainingmocks.MockTrainingMockRecorder) {
				networktopologyFile, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "networktopology", mockHostID, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}

				gomock.InOrder(
					mtts.Recv().Return(&trainerv1.TrainRequest{
						Hostname: mockHostName,
						Ip:       mockIP,
						Request: &trainerv1.TrainRequest_TrainGnnRequest{
							TrainGnnRequest: &trainerv1.TrainGNNRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),

					ms.OpenNetworkTopology(mockHostID).Return(networktopologyFile, nil).Times(1),
					ms.OpenDownload(mockHostID).Return(nil, errors.New("open download file error")).Times(1),
					ms.ClearNetworkTopology(mockHostID).Do(func(id string) {
						networktopologyFile.Close()
						if err := os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "networktopology", id, "csv"))); err != nil {
							t.Fatal(err)
						}
					}).Return(nil).Times(1),
				)

				assert := assert.New(t)
				assert.EqualError(svc.Train(stream),
					"rpc error: code = Internal desc = open download failed: open download file error")
			},
		},
		{
			name: "clear network topology file error",
			run: func(t *testing.T, svc *V1, stream trainerv1.Trainer_TrainServer, mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				ms *storagemocks.MockStorageMockRecorder, mt *trainingmocks.MockTrainingMockRecorder) {
				networktopologyFile, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "networktopology", mockHostID, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}

				downloadFile, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "download", mockHostID, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}

				gomock.InOrder(
					mtts.Recv().Return(&trainerv1.TrainRequest{
						Hostname: mockHostName,
						Ip:       mockIP,
						Request: &trainerv1.TrainRequest_TrainGnnRequest{
							TrainGnnRequest: &trainerv1.TrainGNNRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),

					ms.OpenNetworkTopology(mockHostID).Return(networktopologyFile, nil).Times(1),
					ms.OpenDownload(mockHostID).Return(downloadFile, nil).Times(1),
					mtts.Recv().Return(nil, errors.New("receive error")).Times(1),
					ms.ClearDownload(mockHostID).Do(func(id string) {
						downloadFile.Close()
						if err := os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "download", id, "csv"))); err != nil {
							t.Fatal(err)
						}
					}).Return(nil).Times(1),

					ms.ClearNetworkTopology(mockHostID).Do(func(id string) {
						networktopologyFile.Close()
						if err := os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "networktopology", id, "csv"))); err != nil {
							t.Fatal(err)
						}
					}).Return(errors.New("clear network topology file error")).Times(1),
				)

				assert := assert.New(t)
				assert.EqualError(svc.Train(stream), "receive error")
			},
		},
		{
			name: "clear download file error",
			run: func(t *testing.T, svc *V1, stream trainerv1.Trainer_TrainServer, mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				ms *storagemocks.MockStorageMockRecorder, mt *trainingmocks.MockTrainingMockRecorder) {
				networktopologyFile, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "networktopology", mockHostID, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}

				downloadFile, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "download", mockHostID, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}

				gomock.InOrder(
					mtts.Recv().Return(&trainerv1.TrainRequest{
						Hostname: mockHostName,
						Ip:       mockIP,
						Request: &trainerv1.TrainRequest_TrainGnnRequest{
							TrainGnnRequest: &trainerv1.TrainGNNRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),

					ms.OpenNetworkTopology(mockHostID).Return(networktopologyFile, nil).Times(1),
					ms.OpenDownload(mockHostID).Return(downloadFile, nil).Times(1),
					mtts.Recv().Return(nil, errors.New("receive error")).Times(1),
					ms.ClearDownload(mockHostID).Do(func(id string) {
						downloadFile.Close()
						if err := os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "download", id, "csv"))); err != nil {
							t.Fatal(err)
						}
					}).Return(errors.New("clear download file error")).Times(1),

					ms.ClearNetworkTopology(mockHostID).Do(func(id string) {
						networktopologyFile.Close()
						if err := os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "networktopology", id, "csv"))); err != nil {
							t.Fatal(err)
						}
					}).Return(nil).Times(1),
				)

				assert := assert.New(t)
				assert.EqualError(svc.Train(stream), "receive error")
			},
		},
		{
			name: "store network topology error",
			run: func(t *testing.T, svc *V1, stream trainerv1.Trainer_TrainServer, mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				ms *storagemocks.MockStorageMockRecorder, mt *trainingmocks.MockTrainingMockRecorder) {
				networktopologyFile, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "networktopology", mockHostID, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}

				downloadFile, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "download", mockHostID, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}

				gomock.InOrder(
					mtts.Recv().Return(&trainerv1.TrainRequest{
						Hostname: mockHostName,
						Ip:       mockIP,
						Request: &trainerv1.TrainRequest_TrainGnnRequest{
							TrainGnnRequest: &trainerv1.TrainGNNRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),

					ms.OpenNetworkTopology(mockHostID).Return(networktopologyFile, nil).Times(1),
					ms.OpenDownload(mockHostID).Return(downloadFile, nil).Times(1),
				)

				networktopologyFile.Close()
				assert := assert.New(t)
				assert.EqualError(svc.Train(stream),
					"rpc error: code = Internal desc = write network topology failed: write /tmp/networktopology-52fa2eb710c71cc3e6ba7be6ca82453fcfe59e1c5da358ab3df8b72fd4d2a2cf.csv: file already closed")
			},
		},
		{
			name: "store download error",
			run: func(t *testing.T, svc *V1, stream trainerv1.Trainer_TrainServer, mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				ms *storagemocks.MockStorageMockRecorder, mt *trainingmocks.MockTrainingMockRecorder) {
				networktopologyFile, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "networktopology", mockHostID, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}

				downloadFile, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "download", mockHostID, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}

				gomock.InOrder(
					mtts.Recv().Return(&trainerv1.TrainRequest{
						Hostname: mockHostName,
						Ip:       mockIP,
						Request: &trainerv1.TrainRequest_TrainMlpRequest{
							TrainMlpRequest: &trainerv1.TrainMLPRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),

					ms.OpenNetworkTopology(mockHostID).Return(networktopologyFile, nil).Times(1),
					ms.OpenDownload(mockHostID).Return(downloadFile, nil).Times(1),
				)

				downloadFile.Close()
				assert := assert.New(t)
				assert.EqualError(svc.Train(stream),
					"rpc error: code = Internal desc = write download failed: write /tmp/download-52fa2eb710c71cc3e6ba7be6ca82453fcfe59e1c5da358ab3df8b72fd4d2a2cf.csv: file already closed")
			},
		},
		{
			name: "receive unknown request",
			run: func(t *testing.T, svc *V1, stream trainerv1.Trainer_TrainServer, mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				ms *storagemocks.MockStorageMockRecorder, mt *trainingmocks.MockTrainingMockRecorder) {
				networktopologyFile, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "networktopology", mockHostID, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}

				downloadFile, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "download", mockHostID, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}

				gomock.InOrder(
					mtts.Recv().Return(&trainerv1.TrainRequest{
						Hostname: mockHostName,
						Ip:       mockIP,
						Request:  nil,
					}, nil).Times(1),

					ms.OpenNetworkTopology(mockHostID).Return(networktopologyFile, nil).Times(1),
					ms.OpenDownload(mockHostID).Return(downloadFile, nil).Times(1),
				)

				assert := assert.New(t)
				assert.EqualError(svc.Train(stream), "rpc error: code = FailedPrecondition desc = receive unknown request: <nil>")
			},
		},
		{
			name: "send and close error",
			run: func(t *testing.T, svc *V1, stream trainerv1.Trainer_TrainServer, mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				ms *storagemocks.MockStorageMockRecorder, mt *trainingmocks.MockTrainingMockRecorder) {
				networktopologyFile, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "networktopology", mockHostID, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}

				downloadFile, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "download", mockHostID, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}

				gomock.InOrder(
					mtts.Recv().Return(&trainerv1.TrainRequest{
						Hostname: mockHostName,
						Ip:       mockIP,
						Request: &trainerv1.TrainRequest_TrainGnnRequest{
							TrainGnnRequest: &trainerv1.TrainGNNRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),

					ms.OpenNetworkTopology(mockHostID).Return(networktopologyFile, nil).Times(1),
					ms.OpenDownload(mockHostID).Return(downloadFile, nil).Times(1),
					mtts.Recv().Return(&trainerv1.TrainRequest{
						Hostname: mockHostName,
						Ip:       mockIP,
						Request: &trainerv1.TrainRequest_TrainMlpRequest{
							TrainMlpRequest: &trainerv1.TrainMLPRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),
					mtts.Recv().Return(nil, io.EOF).Times(1),
					mtts.SendAndClose(new(emptypb.Empty)).Return(errors.New("send and close error")).Times(1),
				)

				assert := assert.New(t)
				assert.EqualError(svc.Train(stream), "send and close error")
			},
		},
		{
			name: "training error",
			run: func(t *testing.T, svc *V1, stream trainerv1.Trainer_TrainServer, mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				ms *storagemocks.MockStorageMockRecorder, mt *trainingmocks.MockTrainingMockRecorder) {
				networktopologyFile, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "networktopology", mockHostID, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}

				downloadFile, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "download", mockHostID, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}

				var wg sync.WaitGroup
				wg.Add(1)
				defer wg.Wait()
				gomock.InOrder(
					mtts.Recv().Return(&trainerv1.TrainRequest{
						Hostname: mockHostName,
						Ip:       mockIP,
						Request: &trainerv1.TrainRequest_TrainGnnRequest{
							TrainGnnRequest: &trainerv1.TrainGNNRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),

					ms.OpenNetworkTopology(mockHostID).Return(networktopologyFile, nil).Times(1),
					ms.OpenDownload(mockHostID).Return(downloadFile, nil).Times(1),
					mtts.Recv().Return(&trainerv1.TrainRequest{
						Hostname: mockHostName,
						Ip:       mockIP,
						Request: &trainerv1.TrainRequest_TrainMlpRequest{
							TrainMlpRequest: &trainerv1.TrainMLPRequest{
								Dataset: mockDataset,
							},
						},
					}, nil).Times(1),
					mtts.Recv().Return(nil, io.EOF).Times(1),
					mtts.SendAndClose(new(emptypb.Empty)).Return(nil).Times(1),
					mt.Train(context.Background(), mockIP, mockHostName).DoAndReturn(func(ctx context.Context, ip, hostName string) error {
						wg.Done()
						return errors.New("training error")
					}).Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.Train(stream))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			storage := storagemocks.NewMockStorage(ctl)
			training := trainingmocks.NewMockTraining(ctl)
			stream := trainerv1mocks.NewMockTrainer_TrainServer(ctl)

			svc := NewV1(config.New(), storage, training)
			tc.run(t, svc, stream, stream.EXPECT(), storage.EXPECT(), training.EXPECT())
		})
	}
}
