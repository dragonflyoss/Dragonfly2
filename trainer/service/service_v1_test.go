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
			tc.expect(t, NewV1(config.New(), storage, training))
		})
	}
}

func TestV1_Train(t *testing.T) {
	tests := []struct {
		name string
		mock func(
			mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder, ms *storagemocks.MockStorageMockRecorder,
			mt *trainingmocks.MockTrainingMockRecorder)
		expect func(t *testing.T, err error)
	}{
		// {
		// 	name: "receive GNN and MLP train requests success",
		// 	mock: func(
		// 		mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder, ms *storagemocks.MockStorageMockRecorder,
		// 		mt *trainingmocks.MockTrainingMockRecorder) {
		// 		networktopologyFile, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "networktopology", mockHostID, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		// 		if err != nil {
		// 			t.Fatal(err)
		// 		}

		// 		downloadFile, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "download", mockHostID, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		// 		if err != nil {
		// 			t.Fatal(err)
		// 		}

		// 		var wg sync.WaitGroup
		// 		wg.Add(1)
		// 		gomock.InOrder(
		// 			mtts.Recv().Return(&trainerv1.TrainRequest{
		// 				Hostname: mockHostName,
		// 				Ip:       mockIP,
		// 				Request: &trainerv1.TrainRequest_TrainGnnRequest{
		// 					TrainGnnRequest: &trainerv1.TrainGNNRequest{
		// 						Dataset: mockDataset,
		// 					},
		// 				},
		// 			}, nil).Times(1),

		// 			ms.OpenNetworkTopology(mockHostID).Return(networktopologyFile, nil).Times(1),
		// 			ms.OpenDownload(mockHostID).Return(downloadFile, nil).Times(1),
		// 			mtts.Recv().Return(&trainerv1.TrainRequest{
		// 				Hostname: mockHostName,
		// 				Ip:       mockIP,
		// 				Request: &trainerv1.TrainRequest_TrainMlpRequest{
		// 					TrainMlpRequest: &trainerv1.TrainMLPRequest{
		// 						Dataset: mockDataset,
		// 					},
		// 				},
		// 			}, nil).Times(1),
		// 			mtts.Recv().Return(nil, io.EOF).Times(1),
		// 			mtts.SendAndClose(new(emptypb.Empty)).Return(nil).Times(1),
		// 			mt.Train(context.Background(), mockIP, mockHostName).DoAndReturn(func(ctx context.Context, ip, hostName string) error {
		// 				wg.Done()
		// 				return nil
		// 			}).Times(1),
		// 		)
		// 	},
		// 	expect: func(t *testing.T, err error) {
		// 		assert := assert.New(t)
		// 		assert.NoError(err)
		// 	},
		// },
		{
			name: "receive error",
			mock: func(
				mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder, ms *storagemocks.MockStorageMockRecorder,
				mt *trainingmocks.MockTrainingMockRecorder) {
				gomock.InOrder(
					mtts.Recv().Return(nil, errors.New("receive error")).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "receive error")
			},
		},
		{
			name: "open network topology file error",
			mock: func(
				mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder, ms *storagemocks.MockStorageMockRecorder,
				mt *trainingmocks.MockTrainingMockRecorder) {
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
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "rpc error: code = Internal desc = open network topology failed: open network topology file error")
			},
		},
		{
			name: "open download file error",
			mock: func(
				mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder, ms *storagemocks.MockStorageMockRecorder,
				mt *trainingmocks.MockTrainingMockRecorder) {
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
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "rpc error: code = Internal desc = open download failed: open download file error")
			},
		},
		{
			name: "clear network topology file error",
			mock: func(
				mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder, ms *storagemocks.MockStorageMockRecorder,
				mt *trainingmocks.MockTrainingMockRecorder) {
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
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "receive error")
			},
		},
		{
			name: "clear download file error",
			mock: func(
				mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder, ms *storagemocks.MockStorageMockRecorder,
				mt *trainingmocks.MockTrainingMockRecorder) {
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
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "receive error")
			},
		},
		{
			name: "store network topology error",
			mock: func(
				mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder, ms *storagemocks.MockStorageMockRecorder,
				mt *trainingmocks.MockTrainingMockRecorder) {
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
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "rpc error: code = Internal desc = write network topology failed: write /tmp/networktopology-52fa2eb710c71cc3e6ba7be6ca82453fcfe59e1c5da358ab3df8b72fd4d2a2cf.csv: file already closed")
			},
		},
		{
			name: "store download error",
			mock: func(
				mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder, ms *storagemocks.MockStorageMockRecorder,
				mt *trainingmocks.MockTrainingMockRecorder) {
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
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "rpc error: code = Internal desc = write download failed: write /tmp/download-52fa2eb710c71cc3e6ba7be6ca82453fcfe59e1c5da358ab3df8b72fd4d2a2cf.csv: file already closed")
			},
		},
		{
			name: "receive unknown request",
			mock: func(
				mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder, ms *storagemocks.MockStorageMockRecorder,
				mt *trainingmocks.MockTrainingMockRecorder) {
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
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "rpc error: code = FailedPrecondition desc = receive unknown request: <nil>")
			},
		},
		{
			name: "send and close error",
			mock: func(
				mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder, ms *storagemocks.MockStorageMockRecorder,
				mt *trainingmocks.MockTrainingMockRecorder) {
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
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "send and close error")
			},
		},
		// {
		// 	name: "training error",
		// 	mock: func(
		// 		mtts *trainerv1mocks.MockTrainer_TrainServerMockRecorder, ms *storagemocks.MockStorageMockRecorder,
		// 		mt *trainingmocks.MockTrainingMockRecorder) {
		// 		networktopologyFile, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "networktopology", mockHostID, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		// 		if err != nil {
		// 			t.Fatal(err)
		// 		}

		// 		downloadFile, err := os.OpenFile(filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.%s", "download", mockHostID, "csv")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		// 		if err != nil {
		// 			t.Fatal(err)
		// 		}

		// 		var wg sync.WaitGroup
		// 		wg.Add(1)
		// 		gomock.InOrder(
		// 			mtts.Recv().Return(&trainerv1.TrainRequest{
		// 				Hostname: mockHostName,
		// 				Ip:       mockIP,
		// 				Request: &trainerv1.TrainRequest_TrainGnnRequest{
		// 					TrainGnnRequest: &trainerv1.TrainGNNRequest{
		// 						Dataset: mockDataset,
		// 					},
		// 				},
		// 			}, nil).Times(1),

		// 			ms.OpenNetworkTopology(mockHostID).Return(networktopologyFile, nil).Times(1),
		// 			ms.OpenDownload(mockHostID).Return(downloadFile, nil).Times(1),
		// 			mtts.Recv().Return(&trainerv1.TrainRequest{
		// 				Hostname: mockHostName,
		// 				Ip:       mockIP,
		// 				Request: &trainerv1.TrainRequest_TrainMlpRequest{
		// 					TrainMlpRequest: &trainerv1.TrainMLPRequest{
		// 						Dataset: mockDataset,
		// 					},
		// 				},
		// 			}, nil).Times(1),
		// 			mtts.Recv().Return(nil, io.EOF).Times(1),
		// 			mtts.SendAndClose(new(emptypb.Empty)).Return(nil).Times(1),
		// 			mt.Train(context.Background(), mockIP, mockHostName).DoAndReturn(func(ctx context.Context, ip, hostName string) error {
		// 				wg.Done()
		// 				return nil
		// 			}).Times(1),
		// 		)
		// 	},
		// 	expect: func(t *testing.T, err error) {
		// 		assert := assert.New(t)
		// 		assert.NoError(err)
		// 	},
		// },
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			storage := storagemocks.NewMockStorage(ctl)
			training := trainingmocks.NewMockTraining(ctl)
			stream := trainerv1mocks.NewMockTrainer_TrainServer(ctl)

			tc.mock(stream.EXPECT(), storage.EXPECT(), training.EXPECT())
			svc := NewV1(config.New(), storage, training)
			tc.expect(t, svc.Train(stream))
		})
	}
}
