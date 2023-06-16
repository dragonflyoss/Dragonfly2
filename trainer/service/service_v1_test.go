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
			tc.expect(t, NewV1(&config.Config{}, storage))
		})
	}
}

func TestServiceV1_Train(t *testing.T) {
	tests := []struct {
		name string
		mock func(
			mt *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
			storage *storagemocks.MockStorageMockRecorder,
		)
		expect func(t *testing.T, err error)
	}{
		{
			name: "receive error at the time of beginning",
			mock: func(
				mt *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				storage *storagemocks.MockStorageMockRecorder,
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
			name: "receive GNN train request",
			mock: func(
				mt *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				storage *storagemocks.MockStorageMockRecorder,
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
					}, nil).Times(3),
					mt.SendAndClose(new(emptypb.Empty)).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "receive MLP train request",
			mock: func(
				mt *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				storage *storagemocks.MockStorageMockRecorder,
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
					}, nil).Times(3),
					mt.SendAndClose(new(emptypb.Empty)).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "receive EOF",
			mock: func(
				mt *trainerv1mocks.MockTrainer_TrainServerMockRecorder,
				storage *storagemocks.MockStorageMockRecorder,
			) {
				gomock.InOrder(
					mt.Recv().Return(mt.SendAndClose, io.EOF).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			storage := storagemocks.NewMockStorage(ctl)

			stream := trainerv1mocks.NewMockTrainer_TrainServer(ctl)
			svc := NewV1(&config.Config{}, storage)
			tc.mock(stream.EXPECT(), storage.EXPECT())
			tc.expect(t, svc.Train(stream))
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
				defer file.Close()

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
			svc := NewV1(&config.Config{}, storage)
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
				defer file.Close()

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
			svc := NewV1(&config.Config{}, storage)
			mockModelKey, _ := svc.createModelKey(mockHostName, mockIP, uint(mockClusterID), DefaultHashAlgorithm)

			tc.mock(storage.EXPECT(), mockModelKey)
			tc.expect(t, svc.handleTrainMLPRequest(mockModelKey, mockDataset), mockModelKey)
		})
	}
}
