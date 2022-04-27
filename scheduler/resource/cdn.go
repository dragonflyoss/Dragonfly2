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

//go:generate mockgen -destination cdn_mock.go -source cdn.go -package resource

package resource

import (
	"context"
	"fmt"
	"reflect"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	cdnclient "d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/client"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/util/timeutils"
	"d7y.io/dragonfly/v2/scheduler/config"
)

const (
	// Default value of biz tag for cdn peer
	cdnBizTag = "d7y/cdn"
)

type CDN interface {
	// TriggerTask start to trigger cdn task
	TriggerTask(context.Context, *Task) (*Peer, *rpcscheduler.PeerResult, error)

	// Client is cdn grpc client
	Client() CDNClient
}

type cdn struct {
	// client is cdn dynamic client
	client CDNClient
	// peerManager is peer manager
	peerManager PeerManager
	// hostManager is host manager
	hostManager HostManager
}

// New cdn interface
func newCDN(peerManager PeerManager, hostManager HostManager, client CDNClient) CDN {
	return &cdn{
		client:      client,
		peerManager: peerManager,
		hostManager: hostManager,
	}
}

// TriggerTask start to trigger cdn task
func (c *cdn) TriggerTask(ctx context.Context, task *Task) (*Peer, *rpcscheduler.PeerResult, error) {
	stream, err := c.client.ObtainSeeds(ctx, &cdnsystem.SeedRequest{
		TaskId:  task.ID,
		Url:     task.URL,
		UrlMeta: task.URLMeta,
	})
	if err != nil {
		return nil, nil, err
	}

	var (
		peer        *Peer
		initialized bool
	)

	for {
		piece, err := stream.Recv()
		if err != nil {
			// If the peer initialization succeeds and the download fails,
			// set peer status is PeerStateFailed.
			if peer != nil {
				if err := peer.FSM.Event(PeerEventDownloadFailed); err != nil {
					return nil, nil, err
				}
			}

			return nil, nil, err
		}

		if !initialized {
			initialized = true

			// Initialize cdn peer
			peer, err = c.initPeer(task, piece)
			if err != nil {
				return nil, nil, err
			}
		}

		// Handle begin of piece
		if piece.PieceInfo != nil && piece.PieceInfo.PieceNum == common.BeginOfPiece {
			peer.Log.Infof("receive begin of piece from cdn: %#v %#v", piece, piece.PieceInfo)
			if err := peer.FSM.Event(PeerEventDownload); err != nil {
				return nil, nil, err
			}
			continue
		}

		// Handle end of piece
		if piece.Done {
			peer.Log.Infof("receive end of from cdn: %#v %#v", piece, piece.PieceInfo)
			return peer, &rpcscheduler.PeerResult{
				TotalPieceCount: piece.TotalPieceCount,
				ContentLength:   piece.ContentLength,
			}, nil
		}

		// Handle piece download successfully
		peer.Log.Infof("receive piece from cdn: %#v %#v", piece, piece.PieceInfo)
		peer.Pieces.Set(uint(piece.PieceInfo.PieceNum))
		peer.AppendPieceCost(timeutils.SubNano(int64(piece.EndTime), int64(piece.BeginTime)).Milliseconds())
		task.StorePiece(piece.PieceInfo)
	}
}

// Initialize cdn peer
func (c *cdn) initPeer(task *Task, ps *cdnsystem.PieceSeed) (*Peer, error) {
	// Load peer from manager
	peer, ok := c.peerManager.Load(ps.PeerId)
	if ok {
		return peer, nil
	}
	task.Log.Infof("can not find cdn peer: %s", ps.PeerId)

	// Load host from manager
	host, ok := c.hostManager.Load(ps.HostUuid)
	if !ok {
		task.Log.Errorf("can not find cdn host uuid: %s", ps.HostUuid)
		return nil, errors.Errorf("can not find host uuid: %s", ps.HostUuid)
	}

	// New cdn peer
	peer = NewPeer(ps.PeerId, task, host, WithBizTag(cdnBizTag))
	peer.Log.Info("new cdn peer successfully")

	// Store cdn peer
	c.peerManager.Store(peer)
	peer.Log.Info("cdn peer has been stored")

	if err := peer.FSM.Event(PeerEventRegisterNormal); err != nil {
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
}

type cdnClient struct {
	// hostManager is host manager
	hostManager HostManager

	// cdnClient is cdn grpc client instance
	cdnclient.CdnClient

	// data is dynconfig data
	data *config.DynconfigData
}

// New cdn client interface
func newCDNClient(dynconfig config.DynconfigInterface, hostManager HostManager, opts ...grpc.DialOption) (CDNClient, error) {
	config, err := dynconfig.Get()
	if err != nil {
		return nil, err
	}

	// Initialize cdn grpc client
	client, err := cdnclient.GetClientByAddr(cdnsToNetAddrs(config.CDNs), opts...)
	if err != nil {
		return nil, err
	}

	// Initialize cdn hosts
	for _, host := range cdnsToHosts(config.CDNs) {
		hostManager.Store(host)
	}

	dc := &cdnClient{
		hostManager: hostManager,
		CdnClient:   client,
		data:        config,
	}

	dynconfig.Register(dc)
	return dc, nil
}

// Dynamic config notify function
func (c *cdnClient) OnNotify(data *config.DynconfigData) {
	var cdns []config.CDN
	for _, cdn := range data.CDNs {
		cdns = append(cdns, *cdn)
	}

	if reflect.DeepEqual(c.data, data) {
		logger.Infof("cdn addresses deep equal: %#v", cdns)
		return
	}

	// If only the ip of the cdn host is changed,
	// the cdn peer needs to be cleared.
	diff := diffCDNs(c.data.CDNs, data.CDNs)
	for _, v := range diff {
		id := idgen.CDNHostID(v.Hostname, v.Port)
		if host, ok := c.hostManager.Load(id); ok {
			host.LeavePeers()
			c.hostManager.Delete(id)
		}
	}

	// Update host manager
	for _, host := range cdnsToHosts(data.CDNs) {
		c.hostManager.Store(host)
	}

	// Update dynamic data
	c.data = data

	// Update grpc cdn addresses
	c.UpdateState(cdnsToNetAddrs(data.CDNs))
	logger.Infof("cdn addresses have been updated: %#v", cdns)
}

// cdnsToHosts coverts []*config.CDN to map[string]*Host.
func cdnsToHosts(cdns []*config.CDN) map[string]*Host {
	hosts := map[string]*Host{}
	for _, cdn := range cdns {
		var netTopology string
		options := []HostOption{WithIsCDN(true)}
		if config, ok := cdn.GetCDNClusterConfig(); ok && config.LoadLimit > 0 {
			options = append(options, WithUploadLoadLimit(int32(config.LoadLimit)))
			netTopology = config.NetTopology
		}

		id := idgen.CDNHostID(cdn.Hostname, cdn.Port)
		hosts[id] = NewHost(&rpcscheduler.PeerHost{
			Uuid:        id,
			Ip:          cdn.IP,
			RpcPort:     cdn.Port,
			DownPort:    cdn.DownloadPort,
			HostName:    cdn.Hostname,
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

// diffCDNs get cdns with the same HostID but different IP
func diffCDNs(cx []*config.CDN, cy []*config.CDN) []*config.CDN {
	var diff []*config.CDN
	for _, x := range cx {
		for _, y := range cy {
			if x.Hostname != y.Hostname {
				continue
			}

			if x.Port != y.Port {
				continue
			}

			if x.IP == y.IP {
				continue
			}

			diff = append(diff, x)
		}
	}

	for _, x := range cx {
		found := false
		for _, y := range cy {
			if idgen.CDNHostID(x.Hostname, x.Port) == idgen.CDNHostID(y.Hostname, y.Port) {
				found = true
				break
			}
		}

		if !found {
			diff = append(diff, x)
		}
	}

	return diff
}
