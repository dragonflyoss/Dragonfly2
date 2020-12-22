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

package seed_server

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/base"
	pb "github.com/dragonflyoss/Dragonfly2/pkg/grpc/cdnsystem"
	"github.com/dragonflyoss/Dragonfly2/pkg/netutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/stringutils"
	"github.com/pkg/errors"
)

// CdnSeedServer is used to implement cdnsystem.Seeder.
type SeedServer struct {
	taskMgr mgr.SeedTaskMgr
	pb.UnimplementedSeederServer
}

// NewManager returns a new Manager Object.
func NewCdnSeedServer(taskMgr mgr.SeedTaskMgr) *SeedServer {
	return &SeedServer{
		taskMgr: taskMgr,
	}
}

// ObtainSeeds seed pieces
func (ss *SeedServer) ObtainSeeds(request *pb.SeedRequest, stream pb.Seeder_ObtainSeedsServer) error {
	if err := validateSeedRequestParams(request); err != nil {
		return err
	}
	headers := constructRequestHeader(request.GetUrlMeta())
	md5, _ := headers["md5"]
	registerRequest := &types.TaskRegisterRequest{
		Headers: headers,
		URL:     request.GetUrl(),
		Md5:     md5,
		TaskID:  request.GetTaskId(),
	}
	ctx := context.Background()
	err := ss.taskMgr.Register(ctx, registerRequest)
	if err != nil {
		return errors.Wrapf(err, "")
	}
	piecePullRequest := &types.PiecePullRequest{
		TaskID: request.GetTaskId(),
		DstPID:          "",
		PieceRange:      "",
		PieceResult:     "",
	}
	ch, err := ss.taskMgr.GetPieces(ctx, piecePullRequest)
	if err != nil {

	}
	for piece := range ch {
		stream.Send(&pb.PieceSeed{
			SeedAddr:      piece.SeedAddr,
			PieceStyle:    base.PieceStyle(piece.PieceStyle),
			PieceNum:      piece.PieceNum,
			PieceMd5:      piece.PieceMd5,
			PieceRange:    piece.PieceRange,
			PieceOffset:   piece.PieceOffset,
			PieceLen:      piece.PieceLen,
			DownloadPath:  piece.DownloadPath,
			Last:          piece.Last,
			ContentLength: piece.ContentLength,
		})
	}
	return nil
}

func constructRequestHeader(meta *base.UrlMeta) map[string]string {
	header := make(map[string]string)
	if meta == nil {
		return header
	}
	if !stringutils.IsEmptyStr(meta.Md5) {
		header["md5"] = meta.Md5
	}

	if !stringutils.IsEmptyStr(meta.Range) {
		header["range"] = meta.Range
	}
	return header
}

// validateSeedRequestParams validates the params of SeedRequest.
func validateSeedRequestParams(req *pb.SeedRequest) error {
	if !netutils.IsValidURL(req.Url) {
		return errors.Wrapf(errortypes.ErrInvalidValue, "resource url: %s", req.Url)
	}
	if stringutils.IsEmptyStr(req.TaskId) {
		return errors.Wrapf(errortypes.ErrEmptyValue, "taskId: %s", req.TaskId)
	}
	return nil
}
