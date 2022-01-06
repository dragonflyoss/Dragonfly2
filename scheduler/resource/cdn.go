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

package resource

import (
	"context"
	"fmt"
	"reflect"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	cdnclient "d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/client"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/config"
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
func newCDN(peerManager PeerManager, hostManager HostManager, dynConfig config.DynconfigInterface, opts []grpc.DialOption) (CDN, error) {
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
		initialized bool
		peer        *Peer
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

			if err := peer.FSM.Event(PeerEventDownload); err != nil {
				return nil, nil, err
			}
		}

		// Get end piece
		if piece.Done {
			peer.Log.Infof("receive end of piece: %#v %#v", piece, piece.PieceInfo)

			// Handle tiny scope size task
			if piece.ContentLength <= TinyFileSize {
				peer.Log.Info("peer type is tiny file")
				data, err := peer.DownloadTinyFile(ctx)
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
	peer = NewPeer(ps.PeerId, task, host)
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
func newCDNClient(dynConfig config.DynconfigInterface, hostManager HostManager, opts []grpc.DialOption) (CDNClient, error) {
	config, err := dynConfig.Get()
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

	dynConfig.Register(dc)
	return dc, nil
}

// Dynamic config notify function
func (c *cdnClient) OnNotify(data *config.DynconfigData) {
	ips := getCDNIPs(data.CDNs)
	if reflect.DeepEqual(c.data, data) {
		logger.Infof("cdn addresses deep equal: %v", ips)
		return
	}

	// Update dynamic data
	c.data = data

	// Update host manager
	for _, host := range cdnsToHosts(data.CDNs) {
		c.hostManager.Store(host)
	}

	// Update grpc cdn addresses
	c.UpdateState(cdnsToNetAddrs(data.CDNs))
	logger.Infof("cdn addresses have been updated: %v", ips)
}

// cdnsToHosts coverts []*config.CDN to map[string]*Host.
func cdnsToHosts(cdns []*config.CDN) map[string]*Host {
	hosts := map[string]*Host{}
	for _, cdn := range cdns {
		var netTopology string
		options := []HostOption{WithIsCDN(true)}
		if config, ok := cdn.GetCDNClusterConfig(); ok {
			options = append(options, WithUploadLoadLimit(int32(config.LoadLimit)))
			netTopology = config.NetTopology
		}

		id := idgen.CDNHostID(cdn.HostName, cdn.Port)
		hosts[id] = NewHost(&rpcscheduler.PeerHost{
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
