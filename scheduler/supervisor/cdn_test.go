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

package supervisor_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/client"
	"d7y.io/dragonfly/v2/scheduler/supervisor"
	"d7y.io/dragonfly/v2/scheduler/supervisor/mocks"
)

var (
	mockPieceSeedStream = &client.PieceSeedStream{}
	mockPieceSeed       = &cdnsystem.PieceSeed{}
	mockHost            = &supervisor.Host{}
	mockTask            = &supervisor.Task{}
	mockPeer            = &supervisor.Peer{}
	mockLogger          = &logger.SugaredLoggerOnWith{}
)

func TestCDN_Nil(t *testing.T) {
	tests := []struct {
		name   string
		status supervisor.TaskStatus
		mock   func(t *testing.T) (supervisor.PeerManager, supervisor.HostManager, *gomonkey.Patches)
		expect func(t *testing.T, cdn supervisor.CDN, peer *supervisor.Peer, err error)
	}{
		{
			name:   "nil client",
			status: supervisor.TaskStatusWaiting,
			mock: func(t *testing.T) (supervisor.PeerManager, supervisor.HostManager, *gomonkey.Patches) {
				ctl := gomock.NewController(t)
				defer ctl.Finish()

				mockPeerManager := mocks.NewMockPeerManager(ctl)
				mockHostManager := mocks.NewMockHostManager(ctl)
				mockTask.ID = "mocktask"

				patch := &gomonkey.Patches{}
				return mockPeerManager, mockHostManager, patch
			},
			expect: func(t *testing.T, cdn supervisor.CDN, peer *supervisor.Peer, err error) {
				assert := assert.New(t)
				assert.NotNil(cdn)
				assert.Nil(cdn.GetClient())
				assert.Nil(peer)
				assert.Equal(supervisor.ErrCDNClientUninitialized, err)

			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockPeerManager, mockHostManager, patch := tc.mock(t)
			cdn := supervisor.NewCDN(nil, mockPeerManager, mockHostManager)
			mockTask.SetStatus(tc.status)
			peer, err := cdn.StartSeedTask(context.Background(), mockTask)
			tc.expect(t, cdn, peer, err)
			patch.Reset()
		})
	}
}

func TestCDN_Initial(t *testing.T) {
	tests := []struct {
		name   string
		task   *supervisor.Task
		status supervisor.TaskStatus
		mock   func(t *testing.T) (supervisor.CDNDynmaicClient, supervisor.PeerManager, supervisor.HostManager, *gomonkey.Patches)
		expect func(t *testing.T, cdn supervisor.CDN, peer *supervisor.Peer, err error)
	}{
		{
			name:   "ObtainSeeds cause CDNTaskRegistryFail",
			status: supervisor.TaskStatusWaiting,
			mock: func(t *testing.T) (supervisor.CDNDynmaicClient, supervisor.PeerManager, supervisor.HostManager, *gomonkey.Patches) {
				ctl := gomock.NewController(t)
				defer ctl.Finish()

				err := dferrors.New(base.Code_CDNTaskRegistryFail, "mockError")
				mockCDNDynmaicClient := mocks.NewMockCDNDynmaicClient(ctl)
				mockPeerManager := mocks.NewMockPeerManager(ctl)
				mockHostManager := mocks.NewMockHostManager(ctl)
				mockCDNDynmaicClient.EXPECT().ObtainSeeds(gomock.Any(), gomock.Any()).Return(nil, err).AnyTimes()

				patch := &gomonkey.Patches{}
				return mockCDNDynmaicClient, mockPeerManager, mockHostManager, patch
			},
			expect: func(t *testing.T, cdn supervisor.CDN, peer *supervisor.Peer, err error) {
				assert := assert.New(t)
				assert.Nil(peer)
				assert.Error(supervisor.ErrCDNRegisterFail, errors.Cause(err))
			},
		},
		{
			name:   "ObtainSeeds cause CDNTaskDownloadFail",
			status: supervisor.TaskStatusWaiting,
			mock: func(t *testing.T) (supervisor.CDNDynmaicClient, supervisor.PeerManager, supervisor.HostManager, *gomonkey.Patches) {
				ctl := gomock.NewController(t)
				defer ctl.Finish()

				err := dferrors.New(base.Code_CDNTaskDownloadFail, "mockError")
				mockCDNDynmaicClient := mocks.NewMockCDNDynmaicClient(ctl)
				mockPeerManager := mocks.NewMockPeerManager(ctl)
				mockHostManager := mocks.NewMockHostManager(ctl)
				mockCDNDynmaicClient.EXPECT().ObtainSeeds(gomock.Any(), gomock.Any()).Return(nil, err).AnyTimes()

				patch := &gomonkey.Patches{}
				return mockCDNDynmaicClient, mockPeerManager, mockHostManager, patch
			},
			expect: func(t *testing.T, cdn supervisor.CDN, peer *supervisor.Peer, err error) {
				assert := assert.New(t)
				assert.Nil(peer)
				assert.Error(supervisor.ErrCDNDownloadFail, errors.Cause(err))
			},
		},
		{
			name:   "ObtainSeeds cause other errors",
			status: supervisor.TaskStatusWaiting,
			mock: func(t *testing.T) (supervisor.CDNDynmaicClient, supervisor.PeerManager, supervisor.HostManager, *gomonkey.Patches) {
				ctl := gomock.NewController(t)
				defer ctl.Finish()

				err := dferrors.New(114514, "mockError")
				mockCDNDynmaicClient := mocks.NewMockCDNDynmaicClient(ctl)
				mockPeerManager := mocks.NewMockPeerManager(ctl)
				mockHostManager := mocks.NewMockHostManager(ctl)
				mockCDNDynmaicClient.EXPECT().ObtainSeeds(gomock.Any(), gomock.Any()).Return(nil, err).AnyTimes()

				patch := &gomonkey.Patches{}
				return mockCDNDynmaicClient, mockPeerManager, mockHostManager, patch
			},
			expect: func(t *testing.T, cdn supervisor.CDN, peer *supervisor.Peer, err error) {
				assert := assert.New(t)
				assert.Nil(peer)
				assert.Error(supervisor.ErrCDNUnknown, errors.Cause(err))
			},
		},
		{
			name:   "ObtainSeeds cause invoke client failed",
			status: supervisor.TaskStatusWaiting,
			mock: func(t *testing.T) (supervisor.CDNDynmaicClient, supervisor.PeerManager, supervisor.HostManager, *gomonkey.Patches) {
				ctl := gomock.NewController(t)
				defer ctl.Finish()

				err := fmt.Errorf("invoke error")
				mockCDNDynmaicClient := mocks.NewMockCDNDynmaicClient(ctl)
				mockPeerManager := mocks.NewMockPeerManager(ctl)
				mockHostManager := mocks.NewMockHostManager(ctl)
				mockCDNDynmaicClient.EXPECT().ObtainSeeds(gomock.Any(), gomock.Any()).Return(nil, err).AnyTimes()

				patch := &gomonkey.Patches{}
				return mockCDNDynmaicClient, mockPeerManager, mockHostManager, patch
			},
			expect: func(t *testing.T, cdn supervisor.CDN, peer *supervisor.Peer, err error) {
				assert := assert.New(t)
				assert.Nil(peer)
				assert.Equal(supervisor.ErrCDNInvokeFail, errors.Cause(err))
			},
		},
		{
			name:   "failed for EOF and TaskStatusWaiting",
			status: supervisor.TaskStatusWaiting,
			mock: func(t *testing.T) (supervisor.CDNDynmaicClient, supervisor.PeerManager, supervisor.HostManager, *gomonkey.Patches) {
				ctl := gomock.NewController(t)
				defer ctl.Finish()

				mockCDNDynmaicClient := mocks.NewMockCDNDynmaicClient(ctl)
				mockPeerManager := mocks.NewMockPeerManager(ctl)
				mockHostManager := mocks.NewMockHostManager(ctl)
				mockCDNDynmaicClient.EXPECT().ObtainSeeds(gomock.Any(), gomock.Any()).Return(mockPieceSeedStream, nil).AnyTimes()

				streamRet := []gomonkey.OutputCell{
					{Values: gomonkey.Params{nil, io.EOF}},
				}
				patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(mockPieceSeedStream), "Recv", streamRet)

				return mockCDNDynmaicClient, mockPeerManager, mockHostManager, patch
			},
			expect: func(t *testing.T, cdn supervisor.CDN, peer *supervisor.Peer, err error) {
				assert := assert.New(t)
				assert.Nil(peer)
				assert.Error(err)
			},
		},
		{
			name:   "success for EOF and TaskStatusSuccess",
			status: supervisor.TaskStatusSuccess,
			mock: func(t *testing.T) (supervisor.CDNDynmaicClient, supervisor.PeerManager, supervisor.HostManager, *gomonkey.Patches) {
				ctl := gomock.NewController(t)
				defer ctl.Finish()

				mockCDNDynmaicClient := mocks.NewMockCDNDynmaicClient(ctl)
				mockPeerManager := mocks.NewMockPeerManager(ctl)
				mockHostManager := mocks.NewMockHostManager(ctl)
				mockCDNDynmaicClient.EXPECT().ObtainSeeds(gomock.Any(), gomock.Any()).Return(mockPieceSeedStream, nil).AnyTimes()
				mockPeerManager.EXPECT().Get(gomock.Any()).Return(nil, false).AnyTimes()
				mockPeerManager.EXPECT().Add(gomock.Any()).Return().AnyTimes()
				mockHostManager.EXPECT().Get(gomock.Any()).Return(mockHost, true).AnyTimes()

				patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(mockLogger), "Debugf",
					[]gomonkey.OutputCell{{Values: gomonkey.Params{}}})

				patch.ApplyMethodSeq(reflect.TypeOf(mockTask), "GetOrAddPiece",
					[]gomonkey.OutputCell{{Values: gomonkey.Params{nil, true}}})
				patch.ApplyMethodSeq(reflect.TypeOf(mockTask), "Log",
					[]gomonkey.OutputCell{{Values: gomonkey.Params{mockLogger}}})

				patch.ApplyMethodSeq(reflect.TypeOf(mockPeer), "Touch",
					[]gomonkey.OutputCell{{Values: gomonkey.Params{}}})
				patch.ApplyMethodSeq(reflect.TypeOf(mockPeer), "UpdateProgress",
					[]gomonkey.OutputCell{{Values: gomonkey.Params{}}})

				newPeerRet := []gomonkey.OutputCell{
					{Values: gomonkey.Params{mockPeer}},
				}
				patch.ApplyFuncSeq(supervisor.NewPeer, newPeerRet)

				streamRet := []gomonkey.OutputCell{
					{Values: gomonkey.Params{mockPieceSeed, nil}, Times: 1},
					{Values: gomonkey.Params{nil, io.EOF}, Times: 1},
				}
				patch.ApplyMethodSeq(reflect.TypeOf(mockPieceSeedStream), "Recv", streamRet)

				mockPieceSeed.PieceInfo = &base.PieceInfo{PieceNum: 0}
				mockPeer.Task = mockTask
				mockPeer.ID = "114514"
				return mockCDNDynmaicClient, mockPeerManager, mockHostManager, patch
			},
			expect: func(t *testing.T, cdn supervisor.CDN, peer *supervisor.Peer, err error) {
				assert := assert.New(t)
				assert.Equal(mockPeer, peer)
				assert.Nil(err)
			},
		},
		{
			name:   "receivePiece cause CDNTaskRegistryFail",
			status: supervisor.TaskStatusWaiting,
			mock: func(t *testing.T) (supervisor.CDNDynmaicClient, supervisor.PeerManager, supervisor.HostManager, *gomonkey.Patches) {
				ctl := gomock.NewController(t)
				defer ctl.Finish()

				mockCDNDynmaicClient := mocks.NewMockCDNDynmaicClient(ctl)
				mockPeerManager := mocks.NewMockPeerManager(ctl)
				mockHostManager := mocks.NewMockHostManager(ctl)
				mockCDNDynmaicClient.EXPECT().ObtainSeeds(gomock.Any(), gomock.Any()).Return(mockPieceSeedStream, nil).AnyTimes()

				err := dferrors.New(base.Code_CDNTaskRegistryFail, "mockError")
				streamRet := []gomonkey.OutputCell{
					{Values: gomonkey.Params{nil, err}},
				}
				patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(mockPieceSeedStream), "Recv", streamRet)
				return mockCDNDynmaicClient, mockPeerManager, mockHostManager, patch
			},
			expect: func(t *testing.T, cdn supervisor.CDN, peer *supervisor.Peer, err error) {
				assert := assert.New(t)
				assert.Nil(peer)
				assert.Error(supervisor.ErrCDNRegisterFail, errors.Cause(err))
			},
		},
		{
			name:   "receivePiece cause CDNTaskDownloadFail",
			status: supervisor.TaskStatusWaiting,
			mock: func(t *testing.T) (supervisor.CDNDynmaicClient, supervisor.PeerManager, supervisor.HostManager, *gomonkey.Patches) {
				ctl := gomock.NewController(t)
				defer ctl.Finish()

				mockCDNDynmaicClient := mocks.NewMockCDNDynmaicClient(ctl)
				mockPeerManager := mocks.NewMockPeerManager(ctl)
				mockHostManager := mocks.NewMockHostManager(ctl)
				mockCDNDynmaicClient.EXPECT().ObtainSeeds(gomock.Any(), gomock.Any()).Return(mockPieceSeedStream, nil).AnyTimes()

				err := dferrors.New(base.Code_CDNTaskDownloadFail, "mockError")
				streamRet := []gomonkey.OutputCell{
					{Values: gomonkey.Params{nil, err}},
				}
				patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(mockPieceSeedStream), "Recv", streamRet)
				return mockCDNDynmaicClient, mockPeerManager, mockHostManager, patch
			},
			expect: func(t *testing.T, cdn supervisor.CDN, peer *supervisor.Peer, err error) {
				assert := assert.New(t)
				assert.Nil(peer)
				assert.Error(supervisor.ErrCDNDownloadFail, errors.Cause(err))
			},
		},
		{
			name:   "receivePiece cause other errors",
			status: supervisor.TaskStatusWaiting,
			mock: func(t *testing.T) (supervisor.CDNDynmaicClient, supervisor.PeerManager, supervisor.HostManager, *gomonkey.Patches) {
				ctl := gomock.NewController(t)
				defer ctl.Finish()

				mockCDNDynmaicClient := mocks.NewMockCDNDynmaicClient(ctl)
				mockPeerManager := mocks.NewMockPeerManager(ctl)
				mockHostManager := mocks.NewMockHostManager(ctl)
				mockCDNDynmaicClient.EXPECT().ObtainSeeds(gomock.Any(), gomock.Any()).Return(mockPieceSeedStream, nil).AnyTimes()

				err := dferrors.New(114514, "mockError")
				streamRet := []gomonkey.OutputCell{
					{Values: gomonkey.Params{nil, err}},
				}
				patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(mockPieceSeedStream), "Recv", streamRet)
				return mockCDNDynmaicClient, mockPeerManager, mockHostManager, patch
			},
			expect: func(t *testing.T, cdn supervisor.CDN, peer *supervisor.Peer, err error) {
				assert := assert.New(t)
				assert.Nil(peer)
				assert.Error(supervisor.ErrCDNUnknown, errors.Cause(err))
			},
		},
		{
			name: "receivePiece cause invoke client failed",

			status: supervisor.TaskStatusWaiting,
			mock: func(t *testing.T) (supervisor.CDNDynmaicClient, supervisor.PeerManager, supervisor.HostManager, *gomonkey.Patches) {
				ctl := gomock.NewController(t)
				defer ctl.Finish()

				mockCDNDynmaicClient := mocks.NewMockCDNDynmaicClient(ctl)
				mockPeerManager := mocks.NewMockPeerManager(ctl)
				mockHostManager := mocks.NewMockHostManager(ctl)
				mockCDNDynmaicClient.EXPECT().ObtainSeeds(gomock.Any(), gomock.Any()).Return(mockPieceSeedStream, nil).AnyTimes()

				err := fmt.Errorf("invoke error")
				streamRet := []gomonkey.OutputCell{
					{Values: gomonkey.Params{nil, err}},
				}
				patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(mockPieceSeedStream), "Recv", streamRet)
				return mockCDNDynmaicClient, mockPeerManager, mockHostManager, patch
			},
			expect: func(t *testing.T, cdn supervisor.CDN, peer *supervisor.Peer, err error) {
				assert := assert.New(t)
				assert.Nil(peer)
				assert.Equal(supervisor.ErrCDNInvokeFail, errors.Cause(err))
			},
		},
		{
			name:   "initCDNPeer peer is nil",
			status: supervisor.TaskStatusWaiting,
			mock: func(t *testing.T) (supervisor.CDNDynmaicClient, supervisor.PeerManager, supervisor.HostManager, *gomonkey.Patches) {
				ctl := gomock.NewController(t)
				defer ctl.Finish()

				mockCDNDynmaicClient := mocks.NewMockCDNDynmaicClient(ctl)
				mockPeerManager := mocks.NewMockPeerManager(ctl)
				mockHostManager := mocks.NewMockHostManager(ctl)
				mockCDNDynmaicClient.EXPECT().ObtainSeeds(gomock.Any(), gomock.Any()).Return(mockPieceSeedStream, nil).AnyTimes()
				mockCDNDynmaicClient.EXPECT().GetHost(gomock.Any()).Return(nil, false).AnyTimes()
				mockPeerManager.EXPECT().Get(gomock.Any()).Return(nil, false).AnyTimes()
				mockHostManager.EXPECT().Get(gomock.Any()).Return(nil, false).AnyTimes()

				streamRet := []gomonkey.OutputCell{
					{Values: gomonkey.Params{mockPieceSeed, nil}},
				}
				patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(mockPieceSeedStream), "Recv", streamRet)
				return mockCDNDynmaicClient, mockPeerManager, mockHostManager, patch
			},
			expect: func(t *testing.T, cdn supervisor.CDN, peer *supervisor.Peer, err error) {
				assert := assert.New(t)
				assert.Nil(peer)
				assert.Equal(supervisor.ErrInitCDNPeerFail, errors.Cause(err))
			},
		},
		{
			name:   "downloadTinyFile http.Get error (restore host from hostManager)",
			status: supervisor.TaskStatusWaiting,
			mock: func(t *testing.T) (supervisor.CDNDynmaicClient, supervisor.PeerManager, supervisor.HostManager, *gomonkey.Patches) {
				ctl := gomock.NewController(t)
				defer ctl.Finish()

				mockCDNDynmaicClient := mocks.NewMockCDNDynmaicClient(ctl)
				mockPeerManager := mocks.NewMockPeerManager(ctl)
				mockHostManager := mocks.NewMockHostManager(ctl)
				mockCDNDynmaicClient.EXPECT().ObtainSeeds(gomock.Any(), gomock.Any()).Return(mockPieceSeedStream, nil).AnyTimes()
				mockCDNDynmaicClient.EXPECT().GetHost(gomock.Any()).Return(nil, false).AnyTimes()
				mockPeerManager.EXPECT().Get(gomock.Any()).Return(nil, false).AnyTimes()
				mockPeerManager.EXPECT().Add(gomock.Any()).Return().AnyTimes()
				mockHostManager.EXPECT().Get(gomock.Any()).Return(mockHost, true).AnyTimes()

				patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(mockLogger), "Debugf",
					[]gomonkey.OutputCell{{Values: gomonkey.Params{}}})

				patch.ApplyMethodSeq(reflect.TypeOf(mockTask), "Log",
					[]gomonkey.OutputCell{{Values: gomonkey.Params{mockLogger}}})

				err := fmt.Errorf("http error")
				httpRet := []gomonkey.OutputCell{
					{Values: gomonkey.Params{nil, err}},
				}

				patch.ApplyFuncSeq(http.Get, httpRet)

				patch.ApplyMethodSeq(reflect.TypeOf(mockPeer), "Touch",
					[]gomonkey.OutputCell{{Values: gomonkey.Params{}}})

				newPeerRet := []gomonkey.OutputCell{
					{Values: gomonkey.Params{mockPeer}},
				}
				patch.ApplyFuncSeq(supervisor.NewPeer, newPeerRet)

				streamRet := []gomonkey.OutputCell{
					{Values: gomonkey.Params{mockPieceSeed, nil}},
				}
				patch.ApplyMethodSeq(reflect.TypeOf(mockPieceSeedStream), "Recv", streamRet)

				mockPieceSeed.Done = true
				mockHost.IP = "0.0.0.0"
				mockHost.DownloadPort = 1919
				mockTask.ID = "1919810"
				mockPeer.Host = mockHost
				return mockCDNDynmaicClient, mockPeerManager, mockHostManager, patch
			},
			expect: func(t *testing.T, cdn supervisor.CDN, peer *supervisor.Peer, err error) {
				assert := assert.New(t)
				assert.Equal(mockPeer, peer)
				assert.Nil(err)
			},
		},
		{
			name:   "downloadTinyFile success (restore host from client)",
			status: supervisor.TaskStatusWaiting,
			mock: func(t *testing.T) (supervisor.CDNDynmaicClient, supervisor.PeerManager, supervisor.HostManager, *gomonkey.Patches) {
				ctl := gomock.NewController(t)
				defer ctl.Finish()

				mockCDNDynmaicClient := mocks.NewMockCDNDynmaicClient(ctl)
				mockPeerManager := mocks.NewMockPeerManager(ctl)
				mockHostManager := mocks.NewMockHostManager(ctl)
				mockCDNDynmaicClient.EXPECT().ObtainSeeds(gomock.Any(), gomock.Any()).Return(mockPieceSeedStream, nil).AnyTimes()
				mockCDNDynmaicClient.EXPECT().GetHost(gomock.Any()).Return(mockHost, true).AnyTimes()
				mockPeerManager.EXPECT().Get(gomock.Any()).Return(nil, false).AnyTimes()
				mockPeerManager.EXPECT().Add(gomock.Any()).Return().AnyTimes()
				mockHostManager.EXPECT().Get(gomock.Any()).Return(nil, false).AnyTimes()
				mockHostManager.EXPECT().Add(gomock.Any()).Return().AnyTimes()

				patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(mockLogger), "Debugf",
					[]gomonkey.OutputCell{{Values: gomonkey.Params{}}})

				patch.ApplyMethodSeq(reflect.TypeOf(mockTask), "Log",
					[]gomonkey.OutputCell{{Values: gomonkey.Params{mockLogger}}})

				const testwords string = "dragonfly-scheduler-test"
				res := &http.Response{
					Body: ioutil.NopCloser(
						bytes.NewBuffer([]byte(testwords))),
				}
				httpRet := []gomonkey.OutputCell{
					{Values: gomonkey.Params{res, nil}},
				}
				patch.ApplyFuncSeq(http.Get, httpRet)

				patch.ApplyMethodSeq(reflect.TypeOf(mockPeer), "Touch",
					[]gomonkey.OutputCell{{Values: gomonkey.Params{}}})

				newPeerRet := []gomonkey.OutputCell{
					{Values: gomonkey.Params{mockPeer}},
				}
				patch.ApplyFuncSeq(supervisor.NewPeer, newPeerRet)

				streamRet := []gomonkey.OutputCell{
					{Values: gomonkey.Params{mockPieceSeed, nil}},
				}
				patch.ApplyMethodSeq(reflect.TypeOf(mockPieceSeedStream), "Recv", streamRet)

				mockPieceSeed.Done = true
				mockPieceSeed.ContentLength = int64(len(testwords))
				mockHost.IP = "0.0.0.0"
				mockHost.DownloadPort = 1919
				mockTask.ID = "1919810"
				mockPeer.Host = mockHost
				return mockCDNDynmaicClient, mockPeerManager, mockHostManager, patch
			},
			expect: func(t *testing.T, cdn supervisor.CDN, peer *supervisor.Peer, err error) {
				assert := assert.New(t)
				assert.Equal(mockPeer, peer)
				assert.Nil(err)
				assert.Equal([]byte("dragonfly-scheduler-test"), mockTask.DirectPiece)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockCDNDynmaicClient, mockPeerManager, mockHostManager, patch := tc.mock(t)
			cdn := supervisor.NewCDN(mockCDNDynmaicClient, mockPeerManager, mockHostManager)
			mockTask.SetStatus(tc.status)
			peer, err := cdn.StartSeedTask(context.Background(), mockTask)
			tc.expect(t, cdn, peer, err)
			patch.Reset()
		})
	}
}
