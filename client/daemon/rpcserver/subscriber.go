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

	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	dfdaemonv1 "d7y.io/api/v2/pkg/apis/dfdaemon/v1"

	"d7y.io/dragonfly/v2/client/daemon/peer"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
)

type subscriber struct {
	sync.Mutex // lock for sent map and grpc Send
	*logger.SugaredLoggerOnWith
	*peer.SubscribeResponse
	sync           dfdaemonv1.Daemon_SyncPieceTasksServer
	request        *commonv1.PieceTaskRequest
	skipPieceCount uint32
	totalPieces    int32
	sentMap        map[int32]struct{}
	done           chan struct{}
	uploadAddr     string
	attributeSent  *atomic.Bool
}

func (s *subscriber) getPieces(ctx context.Context, request *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error) {
	p, err := s.Storage.GetPieces(ctx, request)
	if err != nil {
		return nil, err
	}
	p.DstAddr = s.uploadAddr
	if !s.attributeSent.Load() && len(p.PieceInfos) > 0 {
		exa, err := s.Storage.GetExtendAttribute(ctx, nil)
		if err != nil {
			s.Errorf("get extend attribute error: %s", err.Error())
			return nil, err
		}
		p.ExtendAttribute = exa
		s.attributeSent.Store(true)
	}
	return p, err
}

func sendExistPieces(
	ctx context.Context,
	log *logger.SugaredLoggerOnWith,
	get func(ctx context.Context, request *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error),
	request *commonv1.PieceTaskRequest,
	sync dfdaemonv1.Daemon_SyncPieceTasksServer,
	sentMap map[int32]struct{},
	skipSendZeroPiece bool) (total int32, err error) {
	if request.Limit <= 0 {
		request.Limit = 16
	}
	var pp *commonv1.PiecePacket
	for {
		pp, err = get(ctx, request)
		if err != nil {
			log.Errorf("get piece error: %s", err)
			return -1, err
		}
		// when ContentLength is zero, it's an empty file, need send metadata
		if pp.ContentLength != 0 && len(pp.PieceInfos) == 0 && skipSendZeroPiece {
			return pp.TotalPiece, nil
		}
		if err = sync.Send(pp); err != nil {
			log.Errorf("send pieces error: %s", err)
			return pp.TotalPiece, err
		}
		for _, p := range pp.PieceInfos {
			log.Infof("send ready piece %d", p.PieceNum)
			sentMap[p.PieceNum] = struct{}{}
		}
		if uint32(len(pp.PieceInfos)) < request.Limit {
			log.Infof("sent %d pieces, total: %d", len(pp.PieceInfos), pp.TotalPiece)
			return pp.TotalPiece, nil
		}
		// the get piece func always return sorted pieces, use last piece num + 1 to get more pieces
		request.StartNum = uint32(pp.PieceInfos[request.Limit-1].PieceNum + 1)
	}
}

func searchNextPieceNum(sentMap map[int32]struct{}, cur uint32) (nextPieceNum uint32) {
	for i := int32(cur); ; i++ {
		if _, ok := sentMap[i]; !ok {
			nextPieceNum = uint32(i)
			break
		}
	}
	return nextPieceNum
}

// sendExistPieces will send as much as possible pieces
func (s *subscriber) sendExistPieces(startNum uint32) (total int32, err error) {
	s.request.StartNum = startNum
	return sendExistPieces(s.sync.Context(), s.SugaredLoggerOnWith, s.getPieces, s.request, s.sync, s.sentMap, true)
}

func (s *subscriber) receiveRemainingPieceTaskRequests() {
	defer close(s.done)
	for {
		request, err := s.sync.Recv()
		if err == io.EOF {
			s.Infof("remote SyncPieceTasks done, exit receiving")
			return
		}
		if err != nil {
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
			s.Infof("send ready piece %d", p.PieceNum)
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
			s.Infof("remote SyncPieceTasks done, exit sending, local task is running")
			return nil
		case info := <-s.PieceInfoChannel:
			s.Infof("receive piece info, num: %d, finished: %v", info.Num, info.Finished)
			// not desired piece
			if s.totalPieces > -1 && uint32(info.Num) < nextPieceNum {
				continue
			}

			s.Lock()
			total, err := s.sendExistPieces(uint32(info.Num))
			if err != nil {
				err = s.saveError(err)
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
			s.Infof("peer task is success, send remaining pieces")
			s.Lock()
			// all pieces already sent
			// empty piece task will reach sendExistPieces to sync content length and piece count
			if s.totalPieces > 0 && nextPieceNum == uint32(s.totalPieces) {
				s.Unlock()
				break loop
			}
			total, err := s.sendExistPieces(nextPieceNum)
			if err != nil {
				err = s.saveError(err)
				s.Unlock()
				return err
			}
			if total > -1 && s.totalPieces == -1 {
				s.totalPieces = total
			}
			if s.totalPieces > -1 && len(s.sentMap)+int(s.skipPieceCount) != int(s.totalPieces) {
				s.Unlock()
				msg := "peer task success, but can not send all pieces"
				s.Errorf(msg)
				return dferrors.Newf(commonv1.Code_ClientError, msg)
			}
			s.Unlock()
			break loop
		case <-s.Fail:
			reason := s.FailReason()
			s.Errorf("peer task failed: %s", reason)
			// return underlay status to peer
			if st, ok := status.FromError(reason); ok {
				return st.Err()
			}
			return status.Errorf(codes.Internal, "peer task failed: %s", reason)
		}
	}
	select {
	case <-s.done:
		s.Infof("SyncPieceTasks done, exit sending, local task is also done")
		return nil
	}
}

func (s *subscriber) saveError(err error) error {
	if stat, ok := status.FromError(err); !ok {
		// not grpc error
		s.Errorf("sent exist pieces error: %s", err)
	} else if stat.Code() == codes.Canceled {
		err = nil
		s.Debugf("SyncPieceTasks canceled, exit sending")
	} else {
		s.Warnf("SyncPieceTasks send error code %d/%s", stat.Code(), stat.Message())
	}
	return err
}

func (s *subscriber) searchNextPieceNum(cur uint32) (nextPieceNum uint32) {
	return searchNextPieceNum(s.sentMap, cur)
}
