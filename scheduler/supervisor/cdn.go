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
//go:generate mockgen -destination ./mocks/cdn_mock.go -package mocks d7y.io/dragonfly/v2/scheduler/supervisor CDNDynmaicClient

package supervisor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	cdnclient "d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/client"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/config"
)

var tracer = otel.Tracer("scheduler-cdn")

type CDN interface {
	// CetClient get cdn grpc client
	GetClient() CDNDynmaicClient

	// StartSeedTask start seed cdn task
	StartSeedTask(context.Context, *Task) (*Peer, *rpcscheduler.PeerResult, error)
}

type cdn struct {
	// Client is cdn dynamic client
	client CDNDynmaicClient
	// peerManager is peer manager
	peerManager PeerManager
	// hostManager is host manager
	hostManager HostManager
}

func NewCDN(client CDNDynmaicClient, peerManager PeerManager, hostManager HostManager) CDN {
	return &cdn{
		client:      client,
		peerManager: peerManager,
		hostManager: hostManager,
	}
}

func (c *cdn) GetClient() CDNDynmaicClient {
	return c.client
}

func (c *cdn) StartSeedTask(ctx context.Context, task *Task) (*Peer, *rpcscheduler.PeerResult, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanTriggerCDNSeed)
	defer span.End()

	seedRequest := &cdnsystem.SeedRequest{
		TaskId:  task.ID,
		Url:     task.URL,
		UrlMeta: task.URLMeta,
	}
	span.SetAttributes(config.AttributeCDNSeedRequest.String(seedRequest.String()))

	stream, err := c.client.ObtainSeeds(trace.ContextWithSpan(context.Background(), span), seedRequest)
	if err != nil {
		span.RecordError(err)
		span.SetAttributes(config.AttributePeerDownloadSuccess.Bool(false))
		return nil, nil, err
	}

	return c.receivePiece(ctx, task, stream)
}

func (c *cdn) receivePiece(ctx context.Context, task *Task, stream *cdnclient.PieceSeedStream) (*Peer, *rpcscheduler.PeerResult, error) {
	span := trace.SpanFromContext(ctx)
	var (
		initialized bool
		peer        *Peer
	)

	for {
		piece, err := stream.Recv()
		if err != nil {
			span.RecordError(err)
			span.SetAttributes(config.AttributePeerDownloadSuccess.Bool(false))
			return nil, nil, err
		}

		span.AddEvent(config.EventCDNPieceReceived, trace.WithAttributes(config.AttributePieceReceived.String(piece.String())))
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
		peer.SetStatus(PeerStatusRunning)
		peer.LastAccessAt.Store(time.Now())

		// Get last piece
		if piece.Done {
			peer.Log().Info("receive last piece: %#v", piece)
			if piece.ContentLength <= TinyFileSize {
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

			span.SetAttributes(config.AttributePeerDownloadSuccess.Bool(true))
			span.SetAttributes(config.AttributeContentLength.Int64(task.ContentLength.Load()))
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

func (c *cdn) initPeer(task *Task, ps *cdnsystem.PieceSeed) (*Peer, error) {
	var (
		peer *Peer
		host *Host
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
			task.Log().Infof("find cdn host: %s", host.HostName)
		}
		peer = NewPeer(ps.PeerId, task, host)
		peer.Log().Info("new cdn peer succeeded")
	}

	c.peerManager.Add(peer)
	peer.Log().Info("cdn peer has been added")
	return peer, nil
}

func downloadTinyFile(ctx context.Context, task *Task, peer *Peer) ([]byte, error) {
	// download url: http://${host}:${port}/download/${taskIndex}/${taskID}?peerId=scheduler;
	// taskIndex is the first three characters of taskID
	url := fmt.Sprintf("http://%s:%d/download/%s/%s?peerId=scheduler",
		peer.Host.IP, peer.Host.DownloadPort, task.ID[:3], task.ID)

	span := trace.SpanFromContext(ctx)
	span.AddEvent(config.EventDownloadTinyFile, trace.WithAttributes(config.AttributeDownloadFileURL.String(url)))
	peer.Log().Infof("download tiny file url: %s", url)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

type CDNDynmaicClient interface {
	// cdnclient is cdn grpc client
	cdnclient.CdnClient
	// Observer is dynconfig observer
	config.Observer
	// Get cdn host
	GetHost(hostID string) (*Host, bool)
}

type cdnDynmaicClient struct {
	cdnclient.CdnClient
	data  *config.DynconfigData
	hosts map[string]*Host
	lock  sync.RWMutex
}

func NewCDNDynmaicClient(dynConfig config.DynconfigInterface, opts []grpc.DialOption) (CDNDynmaicClient, error) {
	config, err := dynConfig.Get()
	if err != nil {
		return nil, err
	}

	client, err := cdnclient.GetClientByAddr(cdnsToNetAddrs(config.CDNs), opts...)
	if err != nil {
		return nil, err
	}

	dc := &cdnDynmaicClient{
		CdnClient: client,
		data:      config,
		hosts:     cdnsToHosts(config.CDNs),
	}

	dynConfig.Register(dc)
	return dc, nil
}

func (dc *cdnDynmaicClient) GetHost(id string) (*Host, bool) {
	dc.lock.RLock()
	defer dc.lock.RUnlock()

	host, ok := dc.hosts[id]
	if !ok {
		return nil, false
	}

	return host, true
}

func (dc *cdnDynmaicClient) OnNotify(data *config.DynconfigData) {
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
func cdnsToHosts(cdns []*config.CDN) map[string]*Host {
	hosts := map[string]*Host{}
	for _, cdn := range cdns {
		var options []HostOption
		if config, ok := cdn.GetCDNClusterConfig(); ok {
			options = []HostOption{
				WithNetTopology(config.NetTopology),
				WithTotalUploadLoad(config.LoadLimit),
			}
		}

		id := idgen.CDNHostID(cdn.HostName, cdn.Port)
		hosts[id] = NewCDNHost(id, cdn.IP, cdn.HostName, cdn.Port, cdn.DownloadPort, cdn.SecurityGroup, cdn.Location, cdn.IDC, options...)
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
