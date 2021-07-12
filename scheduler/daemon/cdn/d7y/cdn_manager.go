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
	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/internal/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/internal/rpc/cdnsystem/client"
	managerRPC "d7y.io/dragonfly/v2/internal/rpc/manager"
	"d7y.io/dragonfly/v2/internal/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
	"github.com/pkg/errors"
)

type manager struct {
	client  client.CdnClient
	servers map[string]*types.NodeHost
	lock    sync.RWMutex
}

func NewManager(cdnServers []*managerRPC.ServerInfo) (daemon.CDNMgr, error) {
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

var _ config.Observer = (*manager)(nil)
var _ daemon.CDNMgr = (*manager)(nil)

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

func (cm *manager) SeedTask(ctx context.Context, task *types.Task) error {
	if cm.client == nil {
		return errors.New("cdn client is nil")
	}
	stream, err := cm.client.ObtainSeeds(ctx, &cdnsystem.SeedRequest{
		TaskId:  task.TaskID,
		Url:     task.URL,
		Filter:  task.Filter,
		UrlMeta: task.UrlMeta,
	})
	if err != nil {
		// todo deal with error
		if cdnErr, ok := err.(*dferrors.DfError); ok {
			switch cdnErr.Code {
			case dfcodes.CdnError:
				fmt.Println("cdn error")
			case dfcodes.CdnTaskRegistryFail:
				fmt.Println("cdn task register fail")
			case dfcodes.CdnTaskDownloadFail:
				fmt.Println("cdn task download fail")
			}
		}
		return err
	}
	go cm.Work(task, stream)
	return nil
}

func (cm *manager) Work(task *types.Task, stream *client.PieceSeedStream) {
	for {
		piece, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			if recvErr, ok := err.(*dferrors.DfError); ok {
				switch recvErr.Code {
				case dfcodes.CdnTaskRegistryFail:
					task.Status = types.TaskStatusFailed
				case dfcodes.CdnError:
					task.Status = types.TaskStatusFailed
				}
			}
			task.Status = types.TaskStatusFailed
		}
		if piece != nil {
			cm.processSeedPiece(task, piece)
		}
	}
}

func (cm *manager) getServer(name string) (*types.NodeHost, bool) {
	item, found := cm.servers[name]
	return item, found
}

func (cm *manager) processSeedPiece(task *types.Task, ps *cdnsystem.PieceSeed) (err error) {
	peerID := ps.PeerId
	if ps.Done {
		task.PieceTotal = ps.GetPieceInfo().PieceNum
		task.ContentLength = ps.ContentLength
		peerTask.Success = true

		//
		if task.PieceTotal == 1 {
			if task.ContentLength <= TinyFileSize {
				content, er := cm.downloadTinyFileContent(task, host)
				if er == nil && len(content) == int(task.ContentLength) {
					task.DirectPiece = &scheduler.RegisterResult_PieceContent{
						PieceContent: content,
					}
					return
				}
			}
			// other wise scheduler as a small file
			return
		}

		return
	}

	if ps.PieceInfo != nil {
		task.AddPiece(createPiece(ps))

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

func createPiece(ps *cdnsystem.PieceSeed) *types.PieceInfo {
	return &types.PieceInfo{
		PieceNum:    ps.PieceInfo.PieceNum,
		RangeStart:  ps.PieceInfo.RangeStart,
		RangeSize:   ps.PieceInfo.RangeSize,
		PieceMd5:    ps.PieceInfo.PieceMd5,
		PieceOffset: ps.PieceInfo.PieceOffset,
		PieceStyle:  ps.PieceInfo.PieceStyle,
	}
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
