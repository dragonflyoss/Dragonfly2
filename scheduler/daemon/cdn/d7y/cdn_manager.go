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
	"time"

	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/internal/rpc/base"
	"d7y.io/dragonfly/v2/internal/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/internal/rpc/cdnsystem/client"
	managerRPC "d7y.io/dragonfly/v2/internal/rpc/manager"
	"d7y.io/dragonfly/v2/internal/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/safe"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
	"github.com/pkg/errors"
)

type manager struct {
	client              client.CdnClient
	servers             map[string]*types.NodeHost
	lastTriggerSeedTime map[string]time.Time
	lock                sync.RWMutex
}

func (cm *manager) GetLastTriggerSeedTime(taskID string) time.Time {
	panic("implement me")
}

var _ config.Observer = (*manager)(nil)
var _ daemon.CDNMgr = (*manager)(nil)

func newManager(cdnServers []*managerRPC.ServerInfo) (daemon.CDNMgr, error) {
	// Initialize CDNManager servers
	servers := cdnHostsToServers(cdnServers)
	logger.Debugf("servers map: %+v", servers)

	// Initialize CDNManager client
	client, err := client.GetClientByAddr(cdnHostsToNetAddrs(cdnServers))
	if err != nil {
		return nil, errors.Wrapf(err, "create cdn client for scheduler")
	}
	mgr := &manager{
		servers: servers,
		client:  client,
	}
	return mgr, nil
}

// cdnHostsToServers coverts manager.CdnHosts to map[string]*manager.ServerInfo.
func cdnHostsToServers(cdnServers []*managerRPC.ServerInfo) map[string]*types.NodeHost {
	m := make(map[string]*types.NodeHost)
	for _, server := range cdnServers {
		host := server.HostInfo
		cdnUUID := idgen.CDNUUID(host.HostName, server.RpcPort)
		m[cdnUUID] = &types.NodeHost{
			UUID:              cdnUUID,
			IP:                host.Ip,
			HostName:          host.HostName,
			RPCPort:           server.RpcPort,
			DownloadPort:      server.DownPort,
			HostType:          types.CDNNodeHost,
			SecurityDomain:    host.SecurityDomain,
			Location:          host.Location,
			IDC:               host.Idc,
			NetTopology:       host.NetTopology,
			TotalUploadLoad:   types.CDNHostLoad,
			CurrentUploadLoad: 0,
		}
	}
	return m
}

// cdnHostsToNetAddrs coverts manager.CdnHosts to []dfnet.NetAddr.
func cdnHostsToNetAddrs(hosts []*managerRPC.ServerInfo) []dfnet.NetAddr {
	var netAddrs []dfnet.NetAddr
	for i := range hosts {
		netAddrs = append(netAddrs, dfnet.NetAddr{
			Type: dfnet.TCP,
			Addr: fmt.Sprintf("%s:%d", hosts[i].HostInfo.Ip, hosts[i].RpcPort),
		})
	}
	return netAddrs
}

func (cm *manager) OnNotify(c *managerRPC.SchedulerConfig) {
	cm.lock.Lock()
	defer cm.lock.Unlock()
	// Sync CDNManager servers
	cm.servers = cdnHostsToServers(c.CdnHosts)

	// Sync CDNManager client netAddrs
	cm.client.UpdateState(cdnHostsToNetAddrs(c.CdnHosts))
}

func (cm *manager) SeedTask(ctx context.Context, task *types.Task) (*types.PeerNode, error) {
	if cm.client == nil {
		return nil, errors.New("cdn client is nil")
	}
	stream, err := cm.client.ObtainSeeds(ctx, &cdnsystem.SeedRequest{
		TaskId:  task.TaskID,
		Url:     task.URL,
		Filter:  task.Filter,
		UrlMeta: task.UrlMeta,
	})
	if err != nil {
		logger.Errorf("receive a failure state from cdn: taskId[%s] error:%v", task.TaskID, err)
		// todo deal with error
		return err
	}
	return cm.Work(task, stream)
}

func (cm *manager) Work(task *types.Task, stream *client.PieceSeedStream) error {
	firstPiece, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}

	cdnHost := &types.NodeHost{
		UUID:              "",
		IP:                firstPiece.PieceInfo,
		HostName:          firstPiece.SeederName,
		RPCPort:           firstPiece.Port,
		DownloadPort:      0,
		HostType:          0,
		SecurityDomain:    "",
		Location:          "",
		IDC:               "",
		NetTopology:       "",
		TotalUploadLoad:   0,
		CurrentUploadLoad: 0,
	}

	cdnPeerNode := &types.PeerNode{
		PeerID:         firstPiece.PeerId,
		Task:           task,
		Host:           host,
		FinishedNum:    0,
		StartTime:      time.Now(),
		LastAccessTime: time.Now(),
		Parent:         nil,
		Children:       nil,
		Success:        false,
		Status:         0,
		CostHistory:    nil,
	}
	if firstPiece.Done {
		task.PieceTotal = firstPiece.PieceInfo.PieceNum
	}
	waitCallback := true
	for {

		if err != nil {
			dferr, ok := err.(*dferrors.DfError)
			if !ok {
				dferr = dferrors.New(dfcodes.CdnError, err.Error())
			}
			logger.Warnf("receive a failure state from cdn: taskId[%s] error:%v", task.TaskID, err)
			cm.doCallback(task, dferr)
			return
		}

		if ps == nil {
			logger.Warnf("receive a nil pieceSeed or state from cdn: taskId[%s]", task.TaskID)
		} else {
			pieceNum := int32(-1)
			if ps.PieceInfo != nil {
				pieceNum = ps.PieceInfo.PieceNum
			}
			cm.processSeedPiece(task, ps)
			logger.Debugf("receive a pieceSeed from cdn: taskId[%s]-%d done [%v]", task.TaskID, pieceNum, ps.Done)

			if waitCallback {
				waitCallback = false
				cm.doCallback(task, nil)
			}
		}
	}
}

func (cm *manager) doCallback(task *types.Task, err *dferrors.DfError) {
	cm.lock.Lock()
	fn := cm.callbackFns[task]
	list := cm.callbackList[task]
	delete(cm.callbackFns, task)
	delete(cm.callbackList, task)
	cm.lock.Unlock()

	if list == nil {
		return
	}
	go safe.Call(func() {
		task.CDNError = err
		if list != nil {
			for _, pt := range list {
				fn(pt, err)
			}
		}

		if err != nil {
			time.Sleep(time.Second * 5)
			cm.taskManager.Delete(task.TaskID)
			cm.taskManager.DeleteTask(task)
		}
	})
}

func (cm *manager) getServer(name string) (*types.NodeHost, bool) {
	item, found := cm.servers[name]
	return item, found
}

func (cm *manager) processSeedPiece(task *types.Task, ps *cdnsystem.PieceSeed) (err error) {
	host, ok := cm.hostManager.Get(ps.SeederName)
	if !ok {
		server, found := cm.getServer(ps.SeederName)
		if !found {
			logger.Errorf("get cdn by SeederName[%s] failed", ps.SeederName)
			return fmt.Errorf("cdn %s not found", ps.SeederName)
		}

		host = &types.NodeHost{
			HostType: types.HostTypeCdn,
			PeerHost: scheduler.PeerHost{
				Uuid:     hostID,
				HostName: ps.SeederName,
				Ip:       server.HostInfo.Ip,
				RpcPort:  server.RpcPort,
				DownPort: server.DownPort,
			},
		}
		host = cm.hostManager.Add(host)
	}

	pid := ps.PeerId
	peerTask, _ := cm.taskManager.PeerTask.Get(pid)
	if peerTask == nil {
		peerTask = cm.taskManager.PeerTask.Add(pid, task, host)
	} else if peerTask.Host == nil {
		peerTask.Host = host
	}

	if ps.Done {
		task.PieceTotal = peerTask.GetFinishedNum()
		task.ContentLength = ps.ContentLength
		peerTask.Success = true

		//
		if task.PieceTotal == 1 {
			if task.ContentLength <= TinyFileSize {
				content, er := cm.downloadTinyFileContent(task, host)
				if er == nil && len(content) == int(task.ContentLength) {
					task.SizeScope = base.SizeScope_TINY
					task.DirectPiece = &scheduler.RegisterResult_PieceContent{
						PieceContent: content,
					}
					return
				}
			}
			// other wise scheduler as a small file
			task.SizeScope = base.SizeScope_SMALL
			return
		}

		task.SizeScope = base.SizeScope_NORMAL
		return
	}

	if ps.PieceInfo != nil {
		task.AddPiece(cm.createPiece(task, ps, peerTask))

		finishedCount := ps.PieceInfo.PieceNum + 1
		if finishedCount < peerTask.GetFinishedNum() {
			finishedCount = peerTask.GetFinishedNum()
		}

		peerTask.AddPieceStatus(&scheduler.PieceResult{
			PieceNum: ps.PieceInfo.PieceNum,
			Success:  true,
			// currently completed piece count
			FinishedCount: finishedCount,
		})
	}
	return
}

func (cm *manager) createPiece(task *types.Task, ps *cdnsystem.PieceSeed, pt *types.PeerNode) *types.Piece {
	p := task.GetOrCreatePiece(ps.PieceInfo.PieceNum)
	p.PieceInfo = base.PieceInfo{
		PieceNum:    ps.PieceInfo.PieceNum,
		RangeStart:  ps.PieceInfo.RangeStart,
		RangeSize:   ps.PieceInfo.RangeSize,
		PieceMd5:    ps.PieceInfo.PieceMd5,
		PieceOffset: ps.PieceInfo.PieceOffset,
		PieceStyle:  ps.PieceInfo.PieceStyle,
	}
	return p
}

func (cm *manager) downloadTinyFileContent(task *types.Task, cdnHost *types.NodeHost) ([]byte, error) {
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
