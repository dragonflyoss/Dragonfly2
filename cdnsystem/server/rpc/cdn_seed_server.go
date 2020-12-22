package rpc

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/base"
	pb "github.com/dragonflyoss/Dragonfly2/pkg/grpc/cdnsystem"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/netutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/stringutils"
	"github.com/pkg/errors"
)

// CdnSeedServer is used to implement cdnsystem.Seeder.
type CdnSeedServer struct {
	taskMgr mgr.SeedTaskMgr
}

// NewManager returns a new Manager Object.
func NewCdnSeedServer(taskMgr mgr.SeedTaskMgr) (*CdnSeedServer, error) {
	return &CdnSeedServer{
		taskMgr: taskMgr,

	}, nil
}

func (ss *CdnSeedServer) ObtainSeeds(request *pb.SeedRequest, stream pb.Seeder_ObtainSeedsServer) error {
	if err := validateSeedRequestParams(request); err != nil {
		return err
	}
	headers := constructRequestHeader(request.GetUrlMeta())
	registerRequest := &types.TaskRegisterRequest{
		Headers: headers,
		URL:     request.GetUrl(),
		Md5:     request.UrlMeta.Md5,
		TaskID:  request.GetTaskId(),
	}
	err := ss.taskMgr.Register(context.Background(), registerRequest)
	if err != nil {
		return errors.Wrapf(err, "")
	}
	go func() {

	}()
	return nil

}

func constructRequestHeader(meta *base.UrlMeta) map[string]string {
	header := make(map[string]string)
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
