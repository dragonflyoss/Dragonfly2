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
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/client"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
	"github.com/pkg/errors"
)

type manager struct {
	client      client.CdnClient
	peerManager daemon.PeerMgr
	hostManager daemon.HostMgr
	lock        sync.RWMutex
}

func NewManager(cdnServers []*config.CDN, peerManager daemon.PeerMgr, hostManager daemon.HostMgr) (daemon.CDNMgr, error) {
	// Initialize CDNManager client
	cdnClient, err := client.GetClientByAddr(cdnHostsToNetAddrs(cdnServers))
	if err != nil {
		return nil, errors.Wrapf(err, "create cdn client for scheduler")
	}
	mgr := &manager{
		client:      cdnClient,
		peerManager: peerManager,
		hostManager: hostManager,
	}
	return mgr, nil
}

// cdnHostsToNetAddrs coverts manager.CdnHosts to []dfnet.NetAddr.
func cdnHostsToNetAddrs(hosts []*config.CDN) []dfnet.NetAddr {
	var netAddrs []dfnet.NetAddr
	for i := range hosts {
		netAddrs = append(netAddrs, dfnet.NetAddr{
			Type: dfnet.TCP,
			Addr: fmt.Sprintf("%s:%d", hosts[i].IP, hosts[i].Port),
		})
	}
	return netAddrs
}

func (cm *manager) OnNotify(c *config.DynconfigData) {
	cm.lock.Lock()
	defer cm.lock.Unlock()
	// Sync CDNManager client netAddrs
	cm.client.UpdateState(cdnHostsToNetAddrs(c.CDNs))
}

func (cm *manager) StartSeedTask(ctx context.Context, task *types.Task, overrideStatus bool) error {
	if cm.client == nil && (overrideStatus || !task.IsSuccess()) {
		task.SetStatus(types.TaskStatusFailed)
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
				task.SetStatus(types.TaskStatusCDNRegisterFail)
			case dfcodes.CdnTaskDownloadFail:
				task.SetStatus(types.TaskStatusSourceError)
			default:
				task.SetStatus(types.TaskStatusFailed)
			}
		} else if overrideStatus {
			task.SetStatus(types.TaskStatusFailed)
		}
		return errors.Wrapf(err, "obtain seeds from cdn")
	}
	return cm.receivePiece(task, stream)
}

func (cm *manager) receivePiece(task *types.Task, stream *client.PieceSeedStream) error {
	var once sync.Once
	var cdnPeer *types.Peer
	var cdnHost *types.PeerHost
	for {
		piece, err := stream.Recv()
		if err == io.EOF {
			if task.GetStatus() == types.TaskStatusSuccess {
				return nil
			}
			return errors.Errorf("cdn stream receive EOF but task status is %d", task.GetStatus())
		}
		if err != nil {
			if recvErr, ok := err.(*dferrors.DfError); ok {
				switch recvErr.Code {
				case dfcodes.CdnTaskRegistryFail:
					task.SetStatus(types.TaskStatusCDNRegisterFail)
				case dfcodes.CdnTaskDownloadFail:
					task.SetStatus(types.TaskStatusFailed)
				default:
					task.SetStatus(types.TaskStatusFailed)
				}
			}
			task.SetStatus(types.TaskStatusFailed)
			return err
		}
		if piece != nil {
			once.Do(func() {
				cm.initCdnPeer(task, piece)
			})
			cdnPeer.AddPieceInfo(piece.PieceInfo.PieceNum+1, 0)
			cdnPeer.Touch()
			if piece.Done {
				cdnPeer.SetStatus(types.PeerStatusSuccess)
				if task.ContentLength <= types.TinyFileSize {
					content, er := cm.DownloadTinyFileContent(task, cdnHost)
					if er == nil && len(content) == int(task.ContentLength) {
						task.DirectPiece = content
					}
				}
			}
			task.AddPiece(&types.PieceInfo{
				PieceNum:    piece.PieceInfo.PieceNum,
				RangeStart:  piece.PieceInfo.RangeStart,
				RangeSize:   piece.PieceInfo.RangeSize,
				PieceMd5:    piece.PieceInfo.PieceMd5,
				PieceOffset: piece.PieceInfo.PieceOffset,
				PieceStyle:  piece.PieceInfo.PieceStyle,
			})
			if piece.Done {
				task.PieceTotal = piece.TotalPieceCount
				task.ContentLength = piece.ContentLength
				task.SetStatus(types.TaskStatusSuccess)
			}
			return nil
		}
	}
}

func (cm *manager) initCdnPeer(task *types.Task, ps *cdnsystem.PieceSeed) {
	var ok bool
	var cdnHost *types.PeerHost
	cdnPeer, ok := cm.peerManager.Get(ps.PeerId)
	if !ok {
		logger.Debugf("first seed cdn task for taskID %s", task.TaskID)
		if cdnHost, ok = cm.hostManager.Get(ps.HostUuid); !ok {
			logger.Errorf("cannot find host %s", ps.HostUuid)
			//cdnHost = types.NewCDNPeerHost(piece.HostUuid)
		}
		cdnPeer = types.NewPeer(ps.PeerId, task, cdnHost)
	}
	cdnPeer.SetStatus(types.PeerStatusRunning)
	cm.peerManager.Add(cdnPeer)
}

func (cm *manager) DownloadTinyFileContent(task *types.Task, cdnHost *types.PeerHost) ([]byte, error) {
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
