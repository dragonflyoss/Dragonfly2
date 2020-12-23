package server

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/pkg/dferrors"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/cdnsystem"
	"github.com/dragonflyoss/Dragonfly2/pkg/safe"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/netutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/stringutils"
	"github.com/pkg/errors"
	"time"
)

// CdnSeedServer is used to implement cdnsystem.SeederServer.
type CdnSeedServer struct {
	taskMgr mgr.SeedTaskMgr
}

// NewManager returns a new Manager Object.
func NewCdnSeedServer(taskMgr mgr.SeedTaskMgr) (*CdnSeedServer, error) {
	return &CdnSeedServer{
		taskMgr: taskMgr,
	}, nil
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
func validateSeedRequestParams(req *cdnsystem.SeedRequest) error {
	if !netutils.IsValidURL(req.Url) {
		return errors.Wrapf(dferrors.ErrInvalidValue, "resource url: %s", req.Url)
	}
	if stringutils.IsEmptyStr(req.TaskId) {
		return errors.Wrapf(dferrors.ErrEmptyValue, "taskId: %s", req.TaskId)
	}
	return nil
}

func (ss *CdnSeedServer) ObtainSeeds(ctx context.Context, req *cdnsystem.SeedRequest, psc chan<- *cdnsystem.PieceSeed) (err error) {
	safe.Call(func() {
		if err := validateSeedRequestParams(req); err != nil {
			return
		}
		headers := constructRequestHeader(req.GetUrlMeta())
		registerRequest := &types.TaskRegisterRequest{
			Headers: headers,
			URL:     req.GetUrl(),
			Md5:     req.UrlMeta.Md5,
			TaskID:  req.GetTaskId(),
		}
		err = ss.taskMgr.Register(ctx, registerRequest)
		if err != nil {
			return
		}

		var i = 5
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if i < 0 {
					psc <- &cdnsystem.PieceSeed{
						State:         base.NewState(base.Code_SUCCESS, "success"),
						SeedAddr:      "localhost:12345",
						Done:          true,
						ContentLength: 100,
						TotalTraffic:  100,
					}
					return
				}
				psc <- &cdnsystem.PieceSeed{State: base.NewState(base.Code_SUCCESS, "success"), SeedAddr: "localhost:12345"}
				time.Sleep(1 * time.Second)
				i--
			}
		}
	})

	return
}
