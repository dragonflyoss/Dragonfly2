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

package cdn

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/rpc/base"
	"d7y.io/dragonfly/v2/internal/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/internal/rpc/cdnsystem/client"
	"d7y.io/dragonfly/v2/internal/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/safe"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
	"github.com/pkg/errors"
)

const TinyFileSize = 128

type manager struct {
	client        client.CdnClient
	servers       map[string]*manager.ServerInfo
	dynamicConfig config.DynconfigInterface
	lock          sync.RWMutex
	callbackFns   map[*types.Task]func(*types.PeerTask, *dferrors.DfError)
	callbackList  map[*types.Task][]*types.PeerTask
	taskManager   daemon.TaskMgr
	hostManager   daemon.HostMgr
}

func newManager(taskManager daemon.TaskMgr, hostManager daemon.HostMgr, dynamicConfig config.DynconfigInterface) (daemon.CDNMgr, error) {
	mgr := &manager{
		callbackFns:   make(map[*types.Task]func(*types.PeerTask, *dferrors.DfError)),
		callbackList:  make(map[*types.Task][]*types.PeerTask),
		taskManager:   taskManager,
		hostManager:   hostManager,
		dynamicConfig: dynamicConfig,
	}

	// Registration manager
	dynamicConfig.Register(mgr)

	// Get dynconfig content
	dc, err := mgr.dynamicConfig.Get()
	if err != nil {
		return nil, err
	}

	// Initialize CDNManager servers
	mgr.servers = cdnHostsToServers(dc.CdnHosts)
	logger.Debugf("servers map: %+v\n", mgr.servers)

	// Initialize CDNManager client
	client, err := client.GetClientByAddr(cdnHostsToNetAddrs(dc.CdnHosts))
	if err != nil {
		return nil, err
	}
	mgr.client = client

	return mgr, nil
}

// cdnHostsToServers coverts manager.CdnHosts to map[string]*manager.ServerInfo.
func cdnHostsToServers(hosts []*manager.ServerInfo) map[string]*manager.ServerInfo {
	m := make(map[string]*manager.ServerInfo)
	for i := range hosts {
		m[hosts[i].HostInfo.HostName] = hosts[i]
	}

	return m
}

// cdnHostsToNetAddrs coverts manager.CdnHosts to []dfnet.NetAddr.
func cdnHostsToNetAddrs(hosts []*manager.ServerInfo) []dfnet.NetAddr {
	var netAddrs []dfnet.NetAddr
	for i := range hosts {
		netAddrs = append(netAddrs, dfnet.NetAddr{
			Type: dfnet.TCP,
			Addr: fmt.Sprintf("%s:%d", hosts[i].HostInfo.Ip, hosts[i].RpcPort),
		})
	}

	return netAddrs
}

func (cm *manager) OnNotify(c *manager.SchedulerConfig) {
	// Sync CDNManager servers
	cm.servers = cdnHostsToServers(c.CdnHosts)

	// Sync CDNManager client netAddrs
	cm.client.UpdateState(cdnHostsToNetAddrs(c.CdnHosts))
}

func (cm *manager) SeedTask(task *types.Task, callback func(peerTask *types.PeerTask, e *dferrors.DfError)) (err error) {
	if cm.client == nil {
		err = dferrors.New(dfcodes.SchedNeedBackSource, "empty cdn")
		return
	}
	cm.lock.Lock()
	_, ok := cm.callbackFns[task]
	if !ok {
		cm.callbackFns[task] = callback
	}
	cm.lock.Unlock()
	if ok {
		return
	}

	go safe.Call(func() {
		stream, err := cm.client.ObtainSeeds(context.TODO(), &cdnsystem.SeedRequest{
			TaskId:  task.TaskID,
			Url:     task.URL,
			Filter:  task.Filter,
			UrlMeta: task.URLMata,
		})
		if err != nil {
			logger.Warnf("receive a failure state from cdn: taskId[%s] error:%v", task.TaskID, err)
			e, ok := err.(*dferrors.DfError)
			if !ok {
				e = dferrors.New(dfcodes.CdnError, err.Error())
			}
			cm.doCallback(task, e)
			return
		}

		cm.Work(task, stream)
	})

	return
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
			cm.taskManager.PeerTask.DeleteTask(task)
		}
	})
}

func (cm *manager) AddToCallback(peerTask *types.PeerTask) {
	if peerTask == nil || peerTask.Task == nil {
		return
	}
	cm.lock.RLock()
	if _, ok := cm.callbackFns[peerTask.Task]; !ok {
		cm.lock.RUnlock()
		return
	}
	cm.lock.RUnlock()

	cm.lock.Lock()
	if _, ok := cm.callbackFns[peerTask.Task]; ok {
		cm.callbackList[peerTask.Task] = append(cm.callbackList[peerTask.Task], peerTask)
	}
	cm.lock.Unlock()
}

func (cm *manager) getServer(name string) (*manager.ServerInfo, bool) {
	item, found := cm.servers[name]
	return item, found
}

type CDNClient struct {
	client.CdnClient
	mgr *CDNManager
}

func (cm *CDNManager) Work(task *types.Task, stream *client.PieceSeedStream) {
	waitCallback := true
	for {
		ps, err := stream.Recv()
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

func (cm *manager) processSeedPiece(task *types.Task, ps *cdnsystem.PieceSeed) (err error) {
	hostID := cm.getHostUUID(ps)
	host, ok := cm.hostManager.Load(hostID)
	if !ok {
		server, found := cm.getServer(ps.SeederName)
		if !found {
			logger.Errorf("get cdn by SeederName[%s] failed", ps.SeederName)
			return fmt.Errorf("cdn %s not found", ps.SeederName)
		}

		host = &types.Host{
			Type: types.HostTypeCdn,
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

func (cm *manager) getHostUUID(ps *cdnsystem.PieceSeed) string {
	return fmt.Sprintf("cdn:%s", ps.SeederName)
}

func (cm *manager) createPiece(task *types.Task, ps *cdnsystem.PieceSeed, pt *types.PeerTask) *types.Piece {
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

func (cm *manager) downloadTinyFileContent(task *types.Task, cdnHost *types.Host) ([]byte, error) {
	// no need to invoke getPieceTasks method
	// TODO download the tiny file
	// http://host:port/download/{taskId 前3位}/{taskId}?peerId={peerId};
	url := fmt.Sprintf("http://%s:%d/download/%s/%s?peerId=scheduler",
		cdnHost.IP, cdnHost.DownloadPort, task.TaskID[:3], task.TaskID)
	response, err := http.Get(url)
	if err != nil {
		return nil, errors.Wrapf(err, "get url %s", url)
	}
	defer response.Body.Close()
	content, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	return content, nil
}
