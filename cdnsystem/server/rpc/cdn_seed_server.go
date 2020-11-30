package rpc

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/pkg/errortypes"
	pb "github.com/dragonflyoss/Dragonfly2/pkg/grpc/cdnsystem"
	"github.com/dragonflyoss/Dragonfly2/pkg/netutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/stringutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/syncmap"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// CdnSeedServer is used to implement cdnsystem.Seeder.
type CdnSeedServer struct {
	cfg                     *config.Config
	CDNMgr                  mgr.CDNMgr
	taskURLUnReachableStore *syncmap.SyncMap
	taskMgr                 mgr.TaskMgr
}

func (s *CdnSeedServer) ObtainSeeds(request *pb.SeedRequest, stream pb.Seeder_ObtainSeedsServer) error {
	if err := validateSeedRequestParams(request); err != nil {
		return err
	}
	taskCreateRequest := &types.CdnTaskCreateRequest{
		Headers: nil,
		Md5:     request.GetUrlMeta().GetMd5(),
		URL:     request.GetUrl(),
		TaskID:  request.GetTaskId(),
	}
	// create CdnTask
	cdnTask, err := s.taskMgr.AddOrUpdateTask(context.Background(), taskCreateRequest)
	if err != nil {
		logrus.Errorf("taskId: %s failed to add or update task: %v", request.TaskId, err)
		return err
	}
	pieceChan := make(chan *pb.PieceSeed)
	// trigger CDN
	ctx := context.WithValue(context.Background(), "pieceChan", pieceChan)
	if err := s.taskMgr.TriggerCdnSyncAction(ctx, cdnTask); err != nil {
		return errors.Wrapf(errortypes.ErrSystemError, "taskID: %s failed to trigger cdn: %v", request.TaskId, err)
	}

	for piece := range pieceChan {
		stream.Send(piece)
	}
	stream.Send(&pb.PieceSeed{
		State:         nil,
		SeedAddr:      "",
		PieceStyle:    0,
		PieceNum:      0,
		PieceMd5:      "",
		PieceRange:    "",
		PieceOffset:   0,
		Last:          true,
		ContentLength: 0,
	})
	return nil
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
