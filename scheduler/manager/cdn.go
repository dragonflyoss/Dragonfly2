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

package manager

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	cdnclient "d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/client"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/entity"
)

type CDN interface {
	// CetClient get cdn grpc client
	GetClient() CDNClient

	// TriggerTask start to trigger cdn task
	TriggerTask(context.Context, *entity.Task) (*entity.Peer, *rpcscheduler.PeerResult, error)
}

type cdn struct {
	// client is cdn dynamic client
	client CDNClient
	// peerManager is peer manager
	peerManager Peer
	// hostManager is host manager
	hostManager Host
}

func newCDN(peerManager Peer, hostManager Host, dynConfig config.DynconfigInterface, opts []grpc.DialOption) (CDN, error) {
	client, err := newCDNClient(dynConfig, opts)
	if err != nil {
		return nil, err
	}

	return &cdn{
		client:      client,
		peerManager: peerManager,
		hostManager: hostManager,
	}, nil
}

func (c *cdn) GetClient() CDNClient {
	return c.client
}

func (c *cdn) TriggerTask(ctx context.Context, task *entity.Task) (*entity.Peer, *rpcscheduler.PeerResult, error) {
	seedRequest := &cdnsystem.SeedRequest{
		TaskId:  task.ID,
		Url:     task.URL,
		UrlMeta: task.URLMeta,
	}

	stream, err := c.client.ObtainSeeds(ctx, seedRequest)
	if err != nil {
		return nil, nil, err
	}

	return c.receivePiece(ctx, task, stream)
}

func (c *cdn) receivePiece(ctx context.Context, task *entity.Task, stream *cdnclient.PieceSeedStream) (*entity.Peer, *rpcscheduler.PeerResult, error) {
	var (
		initialized bool
		peer        *entity.Peer
	)

	for {
		piece, err := stream.Recv()
		if err != nil {
			return nil, nil, err
		}

		task.Log().Infof("piece info: %#v", piece)

		// Init cdn peer
		if !initialized {
			peer, err = c.initPeer(task, piece)
			if err != nil {
				return nil, nil, err
			}
			initialized = true
		}

		// Trigger peer
		peer.SetStatus(entity.PeerStatusRunning)
		peer.LastAccessAt.Store(time.Now())

		// Get end piece
		if piece.Done {
			peer.Log().Info("receive last piece: %#v", piece)
			if piece.ContentLength <= entity.TinyFileSize {
				peer.Log().Info("peer type is tiny file")
				data, err := downloadTinyFile(ctx, task, peer)
				if err != nil {
					return nil, nil, err
				}

				if len(data) != int(piece.ContentLength) {
					return nil, nil, errors.Errorf(
						"piece actual data length is different from content length, content length is %d, data length is %d",
						piece.ContentLength, len(data),
					)
				}

				if len(data) == int(piece.ContentLength) {
					task.DirectPiece = data
				}
			}

			return peer, &rpcscheduler.PeerResult{
				TotalPieceCount: piece.TotalPieceCount,
				ContentLength:   piece.ContentLength,
			}, nil
		}

		// TODO(244372610) add piece cost by cdn
		// Update piece info
		peer.AddPiece(piece.PieceInfo.PieceNum, 0)
		task.GetOrAddPiece(piece.PieceInfo)
	}
}

func (c *cdn) initPeer(task *entity.Task, ps *cdnsystem.PieceSeed) (*entity.Peer, error) {
	var (
		peer *entity.Peer
		host *entity.Host
		ok   bool
	)

	peer, ok = c.peerManager.Get(ps.PeerId)
	if !ok {
		task.Log().Infof("can not find cdn peer: %s", ps.PeerId)
		if host, ok = c.hostManager.Get(ps.HostUuid); !ok {
			if host, ok = c.client.GetHost(ps.HostUuid); !ok {
				task.Log().Errorf("can not find cdn host uuid: %s", ps.HostUuid)
				return nil, errors.Errorf("can not find host uuid: %s", ps.HostUuid)
			}
			c.hostManager.Add(host)
			task.Log().Infof("new host %s successfully", host.UUID)
		}
		peer = entity.NewPeer(ps.PeerId, task, host)
		peer.Log().Info("new cdn peer succeeded")
	}

	c.peerManager.Add(peer)
	peer.Log().Info("cdn peer has been added")
	return peer, nil
}

func downloadTinyFile(ctx context.Context, task *entity.Task, peer *entity.Peer) ([]byte, error) {
	// download url: http://${host}:${port}/download/${taskIndex}/${taskID}?peerId=scheduler;
	url := url.URL{
		Scheme:   "http",
		Host:     fmt.Sprintf("%s:%d", peer.Host.IP, peer.Host.DownloadPort),
		Path:     fmt.Sprintf("download/%s/%s", task.ID[:3], task.ID),
		RawQuery: "peerId=scheduler",
	}

	peer.Log().Infof("download tiny file url: %s", url)

	resp, err := http.Get(url.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

type CDNClient interface {
	// cdnclient is cdn grpc client
	cdnclient.CdnClient
	// Observer is dynconfig observer
	config.Observer
	// Get cdn host
	GetHost(hostID string) (*entity.Host, bool)
}

type cdnClient struct {
	cdnclient.CdnClient
	data  *config.DynconfigData
	hosts map[string]*entity.Host
	lock  sync.RWMutex
}

func newCDNClient(dynConfig config.DynconfigInterface, opts []grpc.DialOption) (CDNClient, error) {
	config, err := dynConfig.Get()
	if err != nil {
		return nil, err
	}

	client, err := cdnclient.GetClientByAddr(cdnsToNetAddrs(config.CDNs), opts...)
	if err != nil {
		return nil, err
	}

	dc := &cdnClient{
		CdnClient: client,
		data:      config,
		hosts:     cdnsToHosts(config.CDNs),
	}

	dynConfig.Register(dc)
	return dc, nil
}

func (dc *cdnClient) GetHost(id string) (*entity.Host, bool) {
	dc.lock.RLock()
	defer dc.lock.RUnlock()

	host, ok := dc.hosts[id]
	if !ok {
		return nil, false
	}

	return host, true
}

func (dc *cdnClient) OnNotify(data *config.DynconfigData) {
	if reflect.DeepEqual(dc.data, data) {
		return
	}

	dc.lock.Lock()
	defer dc.lock.Unlock()

	dc.data = data
	dc.hosts = cdnsToHosts(data.CDNs)
	dc.UpdateState(cdnsToNetAddrs(data.CDNs))
}

// cdnsToHosts coverts []*config.CDN to map[string]*Host.
func cdnsToHosts(cdns []*config.CDN) map[string]*entity.Host {
	var hosts map[string]*entity.Host
	for _, cdn := range cdns {
		var netTopology string
		options := []entity.HostOption{entity.WithIsCDN(true)}
		if config, ok := cdn.GetCDNClusterConfig(); ok {
			options = append(options, entity.WithTotalUploadLoad(config.LoadLimit))
			netTopology = config.NetTopology
		}

		id := idgen.CDNHostID(cdn.HostName, cdn.Port)
		hosts[id] = entity.NewHost(&rpcscheduler.PeerHost{
			Uuid:           id,
			Ip:             cdn.IP,
			HostName:       cdn.HostName,
			RpcPort:        cdn.Port,
			DownPort:       cdn.DownloadPort,
			SecurityDomain: cdn.SecurityGroup,
			Idc:            cdn.IDC,
			Location:       cdn.Location,
			NetTopology:    netTopology,
		}, options...)
	}
	return hosts
}

// cdnsToNetAddrs coverts []*config.CDN to []dfnet.NetAddr.
func cdnsToNetAddrs(cdns []*config.CDN) []dfnet.NetAddr {
	netAddrs := make([]dfnet.NetAddr, 0, len(cdns))
	for _, cdn := range cdns {
		netAddrs = append(netAddrs, dfnet.NetAddr{
			Type: dfnet.TCP,
			Addr: fmt.Sprintf("%s:%d", cdn.IP, cdn.Port),
		})
	}

	return netAddrs
}
