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

package mgr

import (
	"context"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/safe"
	"d7y.io/dragonfly/v2/scheduler/config"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/client"
	"d7y.io/dragonfly/v2/scheduler/types"
)

const TinyFileSize = 128

type CDNManager struct {
	client       client.SeederClient
	cdnInfoMap   map[string]*config.CdnServerConfig
	lock         *sync.RWMutex
	callbackFns  map[*types.Task]func(*types.PeerTask, *dferrors.DfError)
	callbackList map[*types.Task][]*types.PeerTask
}

func createCDNManager() *CDNManager {
	cdnMgr := &CDNManager{
		cdnInfoMap:   make(map[string]*config.CdnServerConfig),
		lock:         new(sync.RWMutex),
		callbackFns:  make(map[*types.Task]func(*types.PeerTask, *dferrors.DfError)),
		callbackList: make(map[*types.Task][]*types.PeerTask),
	}
	return cdnMgr
}

func (cm *CDNManager) InitCDNClient() {
	list := config.GetConfig().CDN.List
	var addrs []dfnet.NetAddr
	for i, cdn := range list {
		addrs = append(addrs, dfnet.NetAddr{
			Type: dfnet.TCP,
			Addr: fmt.Sprintf("%s:%d", cdn.IP, cdn.RpcPort),
		})
		cm.cdnInfoMap[cdn.CdnName] = &list[i]
	}
	seederClient, err := client.GetClientByAddr(addrs)
	if err != nil {
		logger.Errorf("create cdn client failed main addr [%s]", addrs[0])
	}
	cm.client = seederClient
}

func (cm *CDNManager) TriggerTask(task *types.Task, callback func(peerTask *types.PeerTask, e *dferrors.DfError)) (err error) {
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
			TaskId:  task.TaskId,
			Url:     task.Url,
			Filter:  task.Filter,
			UrlMeta: task.UrlMata,
		})
		if err != nil {
			logger.Warnf("receive a failure state from cdn: taskId[%s] error:%v", task.TaskId, err)
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

func (cm *CDNManager) doCallback(task *types.Task, err *dferrors.DfError) {
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
			GetTaskManager().DeleteTask(task.TaskId)
		}
	})
}

func (cm *CDNManager) AddToCallback(peerTask *types.PeerTask) {
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

func (cm *CDNManager) getCdnInfo(seederName string) *config.CdnServerConfig {
	return cm.cdnInfoMap[seederName]
}

type CDNClient struct {
	client.SeederClient
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
			logger.Warnf("receive a failure state from cdn: taskId[%s] error:%v", task.TaskId, err)
			cm.doCallback(task, dferr)
			return
		}

		if ps == nil {
			logger.Warnf("receive a nil pieceSeed or state from cdn: taskId[%s]", task.TaskId)
		} else {
			pieceNum := int32(-1)
			if ps.PieceInfo != nil {
				pieceNum = ps.PieceInfo.PieceNum
			}
			cm.processPieceSeed(task, ps)
			logger.Debugf("receive a pieceSeed from cdn: taskId[%s]-%d done [%v]", task.TaskId, pieceNum, ps.Done)

			if waitCallback {
				waitCallback = false
				cm.doCallback(task, nil)
			}
		}
	}
}

func (cm *CDNManager) processPieceSeed(task *types.Task, ps *cdnsystem.PieceSeed) (err error) {
	hostId := cm.getHostUuid(ps)
	host, ok := GetHostManager().GetHost(hostId)
	if !ok {
		ip, rpcPort, downPort := "", 0, 0
		cdnInfo := cm.getCdnInfo(ps.SeederName)
		if cdnInfo != nil {
			ip, rpcPort, downPort = cdnInfo.IP, cdnInfo.RpcPort, cdnInfo.DownloadPort
		} else {
			logger.Errorf("get cdn by SeederName[%s] failed", ps.SeederName)
		}
		host = &types.Host{
			Type: types.HostTypeCdn,
			PeerHost: scheduler.PeerHost{
				Uuid:     hostId,
				HostName: ps.SeederName,
				Ip:       ip,
				RpcPort:  int32(rpcPort),
				DownPort: int32(downPort),
			},
		}
		host = GetHostManager().AddHost(host)
	}
	pid := ps.PeerId
	peerTask, _ := GetPeerTaskManager().GetPeerTask(pid)
	if peerTask == nil {
		peerTask = GetPeerTaskManager().AddPeerTask(pid, task, host)
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
				content, er := cm.getTinyFileContent(task, host)
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

func (cm *CDNManager) getHostUuid(ps *cdnsystem.PieceSeed) string {
	return fmt.Sprintf("cdn:%s", ps.PeerId)
}

func (cm *CDNManager) createPiece(task *types.Task, ps *cdnsystem.PieceSeed, pt *types.PeerTask) *types.Piece {
	p := task.GetOrCreatePiece(ps.PieceInfo.PieceNum)
	p.PieceInfo = *ps.PieceInfo
	return p
}

func (cm *CDNManager) getTinyFileContent(task *types.Task, cdnHost *types.Host) (content []byte, err error) {
	resp, err := cm.client.GetPieceTasks(context.TODO(), dfnet.NetAddr{Type: dfnet.TCP, Addr: fmt.Sprintf("%s:%d", cdnHost.Ip, cdnHost.RpcPort)}, &base.PieceTaskRequest{
		TaskId:   task.TaskId,
		SrcPid:   "scheduler",
		StartNum: 0,
		Limit:    2,
	})
	if err != nil {
		return
	}
	if resp == nil || len(resp.PieceInfos) != 1 || resp.TotalPiece != 1 || resp.ContentLength > TinyFileSize {
		err = errors.New("not a tiny file")
	}

	// TODO download the tiny file
	// http://host:port/download/{taskId 前3位}/{taskId}?peerId={peerId};
	url := fmt.Sprintf("http://%s:%d/download/%s/%s?peerId=scheduler",
		cdnHost.Ip, cdnHost.DownPort, task.TaskId[:3], task.TaskId)
	client := &http.Client{
		Timeout: time.Second * 5,
	}
	response, err := client.Get(url)
	if err != nil {
		return
	}
	defer response.Body.Close()
	content, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return
	}

	return
}
