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

package rpcserver

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/cdn/supervisor/mocks"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	"d7y.io/dragonfly/v2/internal/dferrors"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	cdnRPCServer "d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/server"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"
)

func TestServer_ObtainSeeds(t *testing.T) {
	type args struct {
		ctx context.Context
		req *cdnsystem.SeedRequest
		psc chan *cdnsystem.PieceSeed
	}
	tests := []struct {
		name             string
		createCallArgs   func() args
		createCallObject func(t *testing.T, args args) cdnRPCServer.SeederServer
		wantErr          assert.ErrorAssertionFunc
	}{
		{
			name: "obtain piece seeds success",
			createCallObject: func(t *testing.T, args args) cdnRPCServer.SeederServer {
				ctrl := gomock.NewController(t)
				cdnServiceMock := mocks.NewMockCDNService(ctrl)
				regTask := task.NewSeedTask(args.req.TaskId, args.req.Url, args.req.UrlMeta)
				cdnServiceMock.EXPECT().RegisterSeedTask(gomock.Any(), gomock.Any(), gomock.Eq(regTask)).DoAndReturn(
					func(ctx context.Context, clientAddr string, registerTask *task.SeedTask) (*task.SeedTask, <-chan *task.PieceInfo, error) {
						registerTask.CdnStatus = task.StatusRunning
						registerTask.TotalPieceCount = 5
						registerTask.SourceFileLength = 10000
						pieceChan := make(chan *task.PieceInfo)
						go func() {
							pieceChan <- &task.PieceInfo{
								PieceNum: 0,
								PieceMd5: "xxxxxmd5",
								PieceRange: &rangeutils.Range{
									StartIndex: 0,
									EndIndex:   99,
								},
								OriginRange: &rangeutils.Range{
									StartIndex: 0,
									EndIndex:   99,
								},
								PieceLen:   100,
								PieceStyle: 0,
							}
							close(pieceChan)
						}()
						return registerTask, pieceChan, nil
					}).Times(1)
				cdnServiceMock.EXPECT().GetSeedTask(regTask.ID).DoAndReturn(func(taskID string) (*task.SeedTask, error) {
					regTask.CdnStatus = task.StatusSuccess
					return regTask, nil
				}).Times(1)
				//cdnServiceMock.EXPECT().AddTaskGCSubscriber(regTask.ID, gomock.Any()).Times(1)
				server, _ := New(Config{}, cdnServiceMock)
				return server
			},
			createCallArgs: func() args {
				return args{
					ctx: context.Background(),
					req: &cdnsystem.SeedRequest{
						TaskId:  "task1",
						Url:     "https://www.dragonfly.com",
						UrlMeta: nil,
					},
					psc: make(chan *cdnsystem.PieceSeed),
				}
			},
			wantErr: assert.NoError,
		}, {
			name: "task download fail",
			createCallArgs: func() args {
				return args{
					ctx: context.Background(),
					req: &cdnsystem.SeedRequest{
						TaskId:  "task1",
						Url:     "https://www.dragonfly.com",
						UrlMeta: nil,
					},
					psc: make(chan *cdnsystem.PieceSeed),
				}
			},
			createCallObject: func(t *testing.T, args args) cdnRPCServer.SeederServer {
				ctrl := gomock.NewController(t)
				cdnServiceMock := mocks.NewMockCDNService(ctrl)
				regTask := task.NewSeedTask(args.req.TaskId, args.req.Url, args.req.UrlMeta)
				cdnServiceMock.EXPECT().RegisterSeedTask(gomock.Any(), gomock.Any(), gomock.Eq(regTask)).DoAndReturn(
					func(ctx context.Context, clientAddr string, registerTask *task.SeedTask) (*task.SeedTask, <-chan *task.PieceInfo, error) {
						registerTask.CdnStatus = task.StatusRunning
						registerTask.TotalPieceCount = 5
						registerTask.SourceFileLength = 10000
						pieceChan := make(chan *task.PieceInfo)
						go func() {
							pieceChan <- &task.PieceInfo{
								PieceNum: 0,
								PieceMd5: "xxxxxmd5",
								PieceRange: &rangeutils.Range{
									StartIndex: 0,
									EndIndex:   99,
								},
								OriginRange: &rangeutils.Range{
									StartIndex: 0,
									EndIndex:   99,
								},
								PieceLen:   100,
								PieceStyle: 0,
							}
							close(pieceChan)
						}()
						return registerTask, pieceChan, nil
					}).Times(1)
				cdnServiceMock.EXPECT().GetSeedTask(regTask.ID).DoAndReturn(func(taskID string) (*task.SeedTask, error) {
					regTask.CdnStatus = task.StatusFailed
					return regTask, nil
				}).Times(1)
				//cdnServiceMock.EXPECT().AddTaskGCSubscriber(regTask.ID, gomock.Any()).Times(1)
				server, _ := New(Config{}, cdnServiceMock)
				return server
			},
			wantErr: assert.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := tt.createCallArgs()
			svr := tt.createCallObject(t, args)
			go func() {
				for seed := range args.psc {
					fmt.Println(seed)
				}
			}()
			tt.wantErr(t, svr.ObtainSeeds(args.ctx, args.req, args.psc), fmt.Sprintf("ObtainSeeds(%v, %v, %v)", args.ctx, args.req, args.psc))
		})
	}
}

func TestServer_GetPieceTasks(t *testing.T) {
	type args struct {
		ctx context.Context
		req *base.PieceTaskRequest
	}
	tests := []struct {
		name             string
		createCallObject func(t *testing.T, args args) cdnRPCServer.SeederServer
		createCallArgs   func() args
		wantErr          assert.ErrorAssertionFunc
		wantPiecePacket  *base.PiecePacket
	}{
		{
			name: "task not found",
			createCallObject: func(t *testing.T, args args) cdnRPCServer.SeederServer {
				ctrl := gomock.NewController(t)
				cdnServiceMock := mocks.NewMockCDNService(ctrl)
				cdnServiceMock.EXPECT().GetSeedTask(args.req.TaskId).Return(nil, dferrors.Newf(base.Code_CDNTaskNotFound, "failed to get task(%s)",
					args.req.TaskId)).Times(1)
				cdnServiceMock.EXPECT().GetSeedPieces(gomock.Any()).Times(0)
				server, _ := New(Config{}, cdnServiceMock)
				return server
			},
			createCallArgs: func() args {
				return args{
					ctx: context.Background(),
					req: &base.PieceTaskRequest{
						TaskId:   "task1",
						SrcPid:   "srcPeerID",
						DstPid:   "dstPeerID",
						StartNum: 0,
						Limit:    10,
					},
				}
			},
			wantErr:         assert.Error,
			wantPiecePacket: nil,
		},
		{
			name: "success get pieces",
			createCallObject: func(t *testing.T, args args) cdnRPCServer.SeederServer {
				ctrl := gomock.NewController(t)
				cdnServiceMock := mocks.NewMockCDNService(ctrl)
				testTask := &task.SeedTask{
					ID:               args.req.TaskId,
					RawURL:           "https://www.dragonfly.com",
					TaskURL:          "https://www.dragonfly.com",
					SourceFileLength: 250,
					CdnFileLength:    250,
					PieceSize:        100,
					CdnStatus:        task.StatusSuccess,
					TotalPieceCount:  3,
					SourceRealDigest: "xxxxx111",
					PieceMd5Sign:     "bbbbb222",
					Digest:           "xxxxx111",
					Tag:              "xx",
					Range:            "",
					Filter:           "",
					Header:           nil,
					Pieces:           new(sync.Map),
				}
				testTask.Pieces.Store(0, &task.PieceInfo{
					PieceNum: 0,
					PieceMd5: "xxxx0",
					PieceRange: &rangeutils.Range{
						StartIndex: 0,
						EndIndex:   99,
					},
					OriginRange: &rangeutils.Range{
						StartIndex: 0,
						EndIndex:   99,
					},
					PieceLen:   100,
					PieceStyle: 0,
				})
				testTask.Pieces.Store(1, &task.PieceInfo{
					PieceNum: 1,
					PieceMd5: "xxxx1",
					PieceRange: &rangeutils.Range{
						StartIndex: 100,
						EndIndex:   199,
					},
					OriginRange: &rangeutils.Range{
						StartIndex: 100,
						EndIndex:   199,
					},
					PieceLen:   100,
					PieceStyle: 0,
				})
				testTask.Pieces.Store(2, &task.PieceInfo{
					PieceNum: 2,
					PieceMd5: "xxxx2",
					PieceRange: &rangeutils.Range{
						StartIndex: 200,
						EndIndex:   299,
					},
					OriginRange: &rangeutils.Range{
						StartIndex: 200,
						EndIndex:   249,
					},
					PieceLen:   100,
					PieceStyle: 0,
				})
				cdnServiceMock.EXPECT().GetSeedTask(args.req.TaskId).DoAndReturn(func(taskID string) (seedTask *task.SeedTask, err error) {
					return testTask, nil
				})
				cdnServiceMock.EXPECT().GetSeedPieces(args.req.TaskId).DoAndReturn(func(taskID string) (pieces []*task.PieceInfo, err error) {
					testTask.Pieces.Range(func(key, value interface{}) bool {
						pieces = append(pieces, value.(*task.PieceInfo))
						return true
					})
					sort.Slice(pieces, func(i, j int) bool {
						return pieces[i].PieceNum < pieces[j].PieceNum
					})
					return pieces, nil
				})
				server, _ := New(Config{AdvertiseIP: iputils.IPv4, DownloadPort: DefaultDownloadPort}, cdnServiceMock)
				return server
			},
			createCallArgs: func() args {
				return args{
					ctx: context.Background(),
					req: &base.PieceTaskRequest{
						TaskId:   "task2",
						SrcPid:   "srcPeerID",
						DstPid:   "dstPeerID",
						StartNum: 0,
						Limit:    4,
					},
				}
			},
			wantErr: assert.NoError,
			wantPiecePacket: &base.PiecePacket{
				TaskId:  "task2",
				DstPid:  "dstPeerID",
				DstAddr: fmt.Sprintf("%s:%d", iputils.IPv4, DefaultDownloadPort),
				PieceInfos: []*base.PieceInfo{
					{
						PieceNum:    0,
						RangeStart:  0,
						RangeSize:   100,
						PieceMd5:    "xxxx0",
						PieceOffset: 0,
						PieceStyle:  0,
					}, {
						PieceNum:    1,
						RangeStart:  100,
						RangeSize:   100,
						PieceMd5:    "xxxx1",
						PieceOffset: 100,
						PieceStyle:  0,
					}, {
						PieceNum:    2,
						RangeStart:  200,
						RangeSize:   100,
						PieceMd5:    "xxxx2",
						PieceOffset: 200,
						PieceStyle:  0,
					},
				},
				TotalPiece:    3,
				ContentLength: 250,
				PieceMd5Sign:  "bbbbb222",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := tt.createCallArgs()
			svr := tt.createCallObject(t, args)
			gotPiecePacket, err := svr.GetPieceTasks(args.ctx, args.req)
			if !tt.wantErr(t, err, fmt.Sprintf("GetPieceTasks(%v, %v)", args.ctx, args.req)) {
				return
			}
			assert.True(t, cmp.Equal(tt.wantPiecePacket, gotPiecePacket, cmpopts.IgnoreUnexported(base.PiecePacket{}), cmpopts.IgnoreUnexported(base.PieceInfo{})),
				"want: %v, actual: %v", tt.wantPiecePacket, gotPiecePacket)
		})
	}
}
