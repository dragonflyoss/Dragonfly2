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

package d7y

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"

	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/dferrors"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/client"
	managerRPC "d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
	"github.com/pkg/errors"
)

type manager struct {
	client     client.CdnClient
	cdnServers []*managerRPC.CDN
	lock       sync.RWMutex
}

func NewManager(cdnServers []*managerRPC.CDN) (daemon.CDNMgr, error) {
	// Initialize CDNManager client
	client, err := client.GetClientByAddr(cdnHostsToNetAddrs(cdnServers))
	if err != nil {
		return nil, errors.Wrapf(err, "create cdn client for scheduler")
	}
	mgr := &manager{
		client:     client,
		cdnServers: cdnServers,
	}
	return mgr, nil
}

// cdnHostsToNetAddrs coverts manager.CdnHosts to []dfnet.NetAddr.
func cdnHostsToNetAddrs(hosts []*managerRPC.CDN) []dfnet.NetAddr {
	var netAddrs []dfnet.NetAddr
	for i := range hosts {
		netAddrs = append(netAddrs, dfnet.NetAddr{
			Type: dfnet.TCP,
			Addr: fmt.Sprintf("%s:%d", hosts[i].Ip, hosts[i].Port),
		})
	}
	return netAddrs
}

func (cm *manager) OnNotify(c *managerRPC.Scheduler) {
	cm.lock.Lock()
	defer cm.lock.Unlock()
	// Sync CDNManager servers
	cm.cdnServers = c.Cdns
	// Sync CDNManager client netAddrs
	cm.client.UpdateState(cdnHostsToNetAddrs(cm.cdnServers))
}

func (cm *manager) StartSeedTask(ctx context.Context, task *types.Task, callback func(ps *cdnsystem.PieceSeed)) error {
	if cm.client == nil {
		return errors.New("cdn client is nil")
	}
	stream, err := cm.client.ObtainSeeds(ctx, &cdnsystem.SeedRequest{
		TaskId:  task.TaskID,
		Url:     task.URL,
		Filter:  task.Filter,
		UrlMeta: task.URLMeta,
	})
	if err != nil {
		if cdnErr, ok := err.(*dferrors.DfError); ok {
			switch cdnErr.Code {
			case dfcodes.CdnTaskRegistryFail:
				task.SetStatus(types.TaskStatusRegisterFail)
			case dfcodes.CdnTaskDownloadFail:
				task.SetStatus(types.TaskStatusSourceError)
			}
			task.SetStatus(types.TaskStatusFailed)
		}
		return err
	}
	go cm.Work(task, stream, callback)
	return nil
}

func (cm *manager) Work(task *types.Task, stream *client.PieceSeedStream, callback func(ps *cdnsystem.PieceSeed)) {
	for {
		piece, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			if recvErr, ok := err.(*dferrors.DfError); ok {
				switch recvErr.Code {
				case dfcodes.CdnTaskRegistryFail:
					task.SetStatus(types.TaskStatusRegisterFail)
				case dfcodes.CdnTaskDownloadFail:
					task.SetStatus(types.TaskStatusFailed)
				}
			}
			task.SetStatus(types.TaskStatusFailed)
		}
		if piece != nil {
			cm.processSeedPiece(task, piece, callback)
		}
	}
}

func (cm *manager) processSeedPiece(task *types.Task, ps *cdnsystem.PieceSeed, callback func(ps *cdnsystem.PieceSeed)) error {
	if ps.PieceInfo == nil {
		return errors.New("piece info is nil")
	}
	callback(ps)
	task.AddPiece(&types.PieceInfo{
		PieceNum:    ps.PieceInfo.PieceNum,
		RangeStart:  ps.PieceInfo.RangeStart,
		RangeSize:   ps.PieceInfo.RangeSize,
		PieceMd5:    ps.PieceInfo.PieceMd5,
		PieceOffset: ps.PieceInfo.PieceOffset,
		PieceStyle:  ps.PieceInfo.PieceStyle,
	})
	if ps.Done {
		task.PieceTotal = ps.TotalPieceCount
		task.ContentLength = ps.ContentLength
		task.SetStatus(types.TaskStatusSuccess)
	}
	return nil
}

func (cm *manager) DownloadTinyFileContent(task *types.Task, cdnHost *types.NodeHost) ([]byte, error) {
	// no need to invoke getPieceTasks method
	// TODO download the tiny file
	// http://host:port/download/{taskId 前3位}/{taskId}?peerId={peerId};
	url := fmt.Sprintf("http://%s:%d/download/%s/%s?peerId=scheduler",
		cdnHost.IP, cdnHost.DownloadPort, task.TaskID[:3], task.TaskID)
	response, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	content, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	return content, nil
}
