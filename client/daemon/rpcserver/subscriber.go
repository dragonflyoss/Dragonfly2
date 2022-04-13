/*
 *     Copyright 2022 The Dragonfly Authors
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
	"io"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"d7y.io/dragonfly/v2/client/daemon/peer"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
)

type subscriber struct {
	sync.Mutex // lock for sent map and grpc Send
	*logger.SugaredLoggerOnWith
	*peer.SubscribeResult
	sync           dfdaemon.Daemon_SyncPieceTasksServer
	request        *base.PieceTaskRequest
	skipPieceCount uint32
	totalPieces    int32
	sentMap        map[int32]struct{}
	done           chan struct{}
	uploadAddr     string
}

func (s *subscriber) getPieces(ctx context.Context, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
	p, err := s.Storage.GetPieces(ctx, request)
	p.DstAddr = s.uploadAddr
	return p, err
}

func sendExistPieces(
	ctx context.Context,
	get func(ctx context.Context, request *base.PieceTaskRequest) (*base.PiecePacket, error),
	request *base.PieceTaskRequest,
	sync dfdaemon.Daemon_SyncPieceTasksServer,
	sendMap map[int32]struct{},
	skipSendZeroPiece bool) (total int32, sent int, err error) {
	if request.Limit <= 0 {
		request.Limit = 16
	}
	for {
		pp, err := get(ctx, request)
		if err != nil {
			return -1, -1, err
		}
		if len(pp.PieceInfos) == 0 && skipSendZeroPiece {
			return pp.TotalPiece, sent, nil
		}
		if err = sync.Send(pp); err != nil {
			return pp.TotalPiece, sent, err
		}
		for _, p := range pp.PieceInfos {
			sendMap[p.PieceNum] = struct{}{}
		}
		sent += len(pp.PieceInfos)
		if uint32(len(pp.PieceInfos)) < request.Limit {
			return pp.TotalPiece, sent, nil
		}
		// the get piece func always return sorted pieces, use last piece num + 1 to get more pieces
		request.StartNum = uint32(pp.PieceInfos[request.Limit-1].PieceNum + 1)
	}
}

// sendExistPieces will send as much as possible pieces
func (s *subscriber) sendExistPieces(startNum uint32) (total int32, sent int, err error) {
	s.request.StartNum = startNum
	return sendExistPieces(s.sync.Context(), s.getPieces, s.request, s.sync, s.sentMap, true)
}

func (s *subscriber) receiveRemainingPieceTaskRequests() {
	defer close(s.done)
	for {
		request, err := s.sync.Recv()
		if err == io.EOF {
			s.Infof("SyncPieceTasks done, exit receiving")
			return
		} else if err != nil {
			if stat, ok := status.FromError(err); !ok {
				s.Errorf("SyncPieceTasks receive error: %s", err)
			} else if stat.Code() == codes.Canceled {
				s.Debugf("SyncPieceTasks canceled, exit receiving")
			} else {
				s.Warnf("SyncPieceTasks receive error code %d/%s", stat.Code(), stat.Message())
			}
			return
		}
		s.Debugf("receive request: %#v", request)
		pp, err := s.getPieces(s.sync.Context(), request)
		if err != nil {
			s.Errorf("GetPieceTasks error: %s", err)
			return
		}

		// TODO if not found, try to send to peer task conductor, then download it first
		s.Lock()
		err = s.sync.Send(pp)
		if err != nil {
			s.Unlock()
			s.Errorf("SyncPieceTasks send error: %s", err)
			return
		}
		for _, p := range pp.PieceInfos {
			s.sentMap[p.PieceNum] = struct{}{}
		}
		s.Unlock()
	}
}

func (s *subscriber) sendRemainingPieceTasks() error {
	// nextPieceNum is the least piece num which did not send to remote peer
	// may great then total piece count, check the total piece count when use it
	var nextPieceNum uint32
	s.Lock()
	for i := int32(s.skipPieceCount); ; i++ {
		if _, ok := s.sentMap[i]; !ok {
			nextPieceNum = uint32(i)
			break
		}
	}
	s.Unlock()
loop:
	for {
		select {
		case <-s.done:
			s.Infof("SyncPieceTasks done, exit sending, local task is running")
			return nil
		case info := <-s.PieceInfoChannel:
			// not desired piece
			s.Debugf("receive piece info, num: %d, %v", info.Num, info.Finished)
			if s.totalPieces > -1 && uint32(info.Num) < nextPieceNum {
				continue
			}
			s.Lock()
			total, _, err := s.sendExistPieces(uint32(info.Num))

			if err != nil {
				if stat, ok := status.FromError(err); !ok {
					// not grpc error
					s.Errorf("sent exist pieces error: %s", err)
				} else if stat.Code() == codes.Canceled {
					err = nil
					s.Debugf("SyncPieceTasks canceled, exit sending")
				} else {
					s.Warnf("SyncPieceTasks send error code %d/%s", stat.Code(), stat.Message())
				}
				s.Unlock()
				return err
			}
			if total > -1 && s.totalPieces == -1 {
				s.totalPieces = total
			}
			if s.totalPieces > -1 && len(s.sentMap)+int(s.skipPieceCount) == int(s.totalPieces) {
				s.Unlock()
				break loop
			}
			if info.Finished {
				s.Unlock()
				break loop
			}
			nextPieceNum = s.searchNextPieceNum(nextPieceNum)
			s.Unlock()
		case <-s.Success:
			s.Lock()
			// all pieces already sent
			if s.totalPieces > -1 && nextPieceNum == uint32(s.totalPieces) {
				s.Unlock()
				break loop
			}
			total, _, err := s.sendExistPieces(nextPieceNum)
			if err != nil {
				if stat, ok := status.FromError(err); !ok {
					// not grpc error
					s.Errorf("sent exist pieces error: %s", err)
				} else if stat.Code() == codes.Canceled {
					err = nil
					s.Debugf("SyncPieceTasks canceled, exit sending")
				} else {
					s.Warnf("SyncPieceTasks send error code %d/%s", stat.Code(), stat.Message())
				}
				s.Unlock()
				return err
			}
			if total > -1 && s.totalPieces == -1 {
				s.totalPieces = total
			}
			if s.totalPieces > -1 && len(s.sentMap)+int(s.skipPieceCount) != int(s.totalPieces) {
				s.Unlock()
				return dferrors.Newf(base.Code_ClientError, "peer task success, but can not send all pieces")
			}
			s.Unlock()
			break loop
		case <-s.Fail:
			return dferrors.Newf(base.Code_ClientError, "peer task failed")
		}
	}
	select {
	case <-s.done:
		s.Infof("SyncPieceTasks done, exit sending, local task is also done")
		return nil
	}
}

func (s *subscriber) searchNextPieceNum(cur uint32) (nextPieceNum uint32) {
	for i := int32(cur); ; i++ {
		if _, ok := s.sentMap[i]; !ok {
			nextPieceNum = uint32(i)
			break
		}
	}
	return nextPieceNum
}
