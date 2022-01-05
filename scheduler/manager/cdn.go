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
	"reflect"
	"sync"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	cdnclient "d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/client"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/entity"
	"d7y.io/dragonfly/v2/scheduler/util"
)

type CDN interface {
	// TriggerTask start to trigger cdn task
	TriggerTask(context.Context, *entity.Task) (*entity.Peer, *rpcscheduler.PeerResult, error)

	// Client is cdn grpc client
	Client() CDNClient
}

type cdn struct {
	// client is cdn dynamic client
	client CDNClient
	// peerManager is peer manager
	peerManager Peer
	// hostManager is host manager
	hostManager Host
}

// New cdn interface
func newCDN(peerManager Peer, hostManager Host, dynConfig config.DynconfigInterface, opts []grpc.DialOption) (CDN, error) {
	client, err := newCDNClient(dynConfig, hostManager, opts)
	if err != nil {
		return nil, err
	}

	return &cdn{
		client:      client,
		peerManager: peerManager,
		hostManager: hostManager,
	}, nil
}

// TriggerTask start to trigger cdn task
func (c *cdn) TriggerTask(ctx context.Context, task *entity.Task) (*entity.Peer, *rpcscheduler.PeerResult, error) {
	stream, err := c.client.ObtainSeeds(ctx, &cdnsystem.SeedRequest{
		TaskId:  task.ID,
		Url:     task.URL,
		UrlMeta: task.URLMeta,
	})
	if err != nil {
		return nil, nil, err
	}

	var (
		initialized bool
		peer        *entity.Peer
	)

	// Receive pieces from cdn
	for {
		piece, err := stream.Recv()
		if err != nil {
			return nil, nil, err
		}

		task.Log.Infof("receive piece: %#v %#v", piece, piece.PieceInfo)

		// Init cdn peer
		if !initialized {
			initialized = true

			peer, err = c.initPeer(task, piece)
			if err != nil {
				return nil, nil, err
			}

			if err := peer.FSM.Event(entity.PeerEventDownload); err != nil {
				return nil, nil, err
			}
		}

		// Get end piece
		if piece.Done {
			peer.Log.Infof("receive end of piece: %#v %#v", piece, piece.PieceInfo)

			// Handle tiny scope size task
			if piece.ContentLength <= entity.TinyFileSize {
				peer.Log.Info("peer type is tiny file")
				data, err := util.DownloadTinyFile(ctx, task, peer)
				if err != nil {
					return nil, nil, err
				}

				// Tiny file downloaded directly from CDN is exception
				if len(data) != int(piece.ContentLength) {
					return nil, nil, errors.Errorf(
						"piece actual data length is different from content length, content length is %d, data length is %d",
						piece.ContentLength, len(data),
					)
				}

				// Tiny file downloaded successfully
				task.DirectPiece = data
			}

			return peer, &rpcscheduler.PeerResult{
				TotalPieceCount: piece.TotalPieceCount,
				ContentLength:   piece.ContentLength,
			}, nil
		}

		// Update piece info
		peer.Pieces.Set(uint(piece.PieceInfo.PieceNum))
		// TODO(244372610) CDN should set piece cost
		peer.PieceCosts.Add(0)
		task.StorePiece(piece.PieceInfo)
	}
}

// Initialize cdn peer
func (c *cdn) initPeer(task *entity.Task, ps *cdnsystem.PieceSeed) (*entity.Peer, error) {
	var (
		peer *entity.Peer
		host *entity.Host
		ok   bool
	)

	// Load peer from manager
	peer, ok = c.peerManager.Load(ps.PeerId)
	if ok {
		return peer, nil
	}

	task.Log.Infof("can not find cdn peer: %s", ps.PeerId)
	if host, ok = c.hostManager.Load(ps.HostUuid); !ok {
		if host, ok = c.client.LoadHost(ps.HostUuid); !ok {
			task.Log.Errorf("can not find cdn host uuid: %s", ps.HostUuid)
			return nil, errors.Errorf("can not find host uuid: %s", ps.HostUuid)
		}

		// Store cdn host
		c.hostManager.Store(host)
		task.Log.Infof("new host %s successfully", host.ID)
	}

	// New cdn peer
	peer = entity.NewPeer(ps.PeerId, task, host)
	peer.Log.Info("new cdn peer successfully")

	// Store cdn peer
	c.peerManager.Store(peer)
	peer.Log.Info("cdn peer has been stored")

	if err := peer.FSM.Event(entity.PeerEventRegisterNormal); err != nil {
		return nil, err
	}

	return peer, nil
}

// Client is cdn grpc client
func (c *cdn) Client() CDNClient {
	return c.client
}

type CDNClient interface {
	// cdnclient is cdn grpc client interface
	cdnclient.CdnClient

	// Observer is dynconfig observer interface
	config.Observer

	// LoadHost return host entity for a key
	LoadHost(string) (*entity.Host, bool)
}

type cdnClient struct {
	// hostManager is host manager
	hostManager Host

	// cdnClient is cdn grpc client instance
	cdnclient.CdnClient

	// data is dynconfig data
	data *config.DynconfigData

	// hosts is host entity map
	hosts map[string]*entity.Host

	// mu is rwmutex
	mu sync.RWMutex
}

// New cdn client interface
func newCDNClient(dynConfig config.DynconfigInterface, hostManager Host, opts []grpc.DialOption) (CDNClient, error) {
	config, err := dynConfig.Get()
	if err != nil {
		return nil, err
	}

	client, err := cdnclient.GetClientByAddr(cdnsToNetAddrs(config.CDNs), opts...)
	if err != nil {
		return nil, err
	}

	dc := &cdnClient{
		hostManager: hostManager,
		CdnClient:   client,
		data:        config,
		hosts:       cdnsToHosts(config.CDNs),
	}

	dynConfig.Register(dc)
	return dc, nil
}

// LoadHost return host entity for a key
func (c *cdnClient) LoadHost(key string) (*entity.Host, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	host, ok := c.hosts[key]
	if !ok {
		return nil, false
	}

	return host, true
}

// Dynamic config notify function
func (c *cdnClient) OnNotify(data *config.DynconfigData) {
	ips := getCDNIPs(data.CDNs)
	if reflect.DeepEqual(c.data, data) {
		logger.Infof("cdn addresses deep equal: %v", ips)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.data = data
	c.hosts = cdnsToHosts(data.CDNs)

	// Update host manager
	for _, host := range c.hosts {
		c.hostManager.Store(host)
	}

	// Update grpc cdn addresses
	c.UpdateState(cdnsToNetAddrs(data.CDNs))
	logger.Infof("cdn addresses have been updated: %v", ips)
}

func (c *cdnClient) updateCDNHost(hosts map[string]*entity.Host) {
}

// cdnsToHosts coverts []*config.CDN to map[string]*Host.
func cdnsToHosts(cdns []*config.CDN) map[string]*entity.Host {
	hosts := map[string]*entity.Host{}
	for _, cdn := range cdns {
		var netTopology string
		options := []entity.HostOption{entity.WithIsCDN(true)}
		if config, ok := cdn.GetCDNClusterConfig(); ok {
			options = append(options, entity.WithUploadLoadLimit(int32(config.LoadLimit)))
			netTopology = config.NetTopology
		}

		id := idgen.CDNHostID(cdn.HostName, cdn.Port)
		hosts[id] = entity.NewHost(&rpcscheduler.PeerHost{
			Uuid:        id,
			Ip:          cdn.IP,
			RpcPort:     cdn.Port,
			DownPort:    cdn.DownloadPort,
			HostName:    cdn.HostName,
			Idc:         cdn.IDC,
			Location:    cdn.Location,
			NetTopology: netTopology,
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

// getCDNIPs get ips by []*config.CDN.
func getCDNIPs(cdns []*config.CDN) []string {
	ips := []string{}
	for _, cdn := range cdns {
		ips = append(ips, cdn.IP)
	}

	return ips
}
