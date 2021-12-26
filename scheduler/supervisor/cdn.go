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

	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	cdnclient "d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/client"
	"d7y.io/dragonfly/v2/scheduler/config"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

var (
	ErrCDNClientUninitialized = errors.New("cdn client is not initialized")
	ErrCDNRegisterFail        = errors.New("cdn task register failed")
	ErrCDNDownloadFail        = errors.New("cdn task download failed")
	ErrCDNUnknown             = errors.New("cdn obtain seed encounter unknown err")
	ErrCDNInvokeFail          = errors.New("invoke cdn interface failed")
	ErrInitCDNPeerFail        = errors.New("init cdn peer failed")
)

var tracer = otel.Tracer("scheduler-cdn")

type CDN interface {
	// GetClient get cdn grpc client
	GetClient() CDNDynmaicClient

	// StartSeedTask start seed cdn task
	StartSeedTask(context.Context, *Task) (*Peer, error)
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

func (c *cdn) StartSeedTask(ctx context.Context, task *Task) (*Peer, error) {
	logger.Infof("start seed task %s", task.ID)
	defer func() {
		logger.Infof("finish seed task %s, task status is %s", task.ID, task.GetStatus())
	}()
	var seedSpan trace.Span
	ctx, seedSpan = tracer.Start(ctx, config.SpanTriggerCDNSeed)
	defer seedSpan.End()
	seedRequest := &cdnsystem.SeedRequest{
		TaskId:  task.ID,
		Url:     task.URL,
		UrlMeta: task.URLMeta,
	}
	seedSpan.SetAttributes(config.AttributeCDNSeedRequest.String(seedRequest.String()))

	if c.client == nil {
		err := ErrCDNClientUninitialized
		seedSpan.RecordError(err)
		seedSpan.SetAttributes(config.AttributePeerDownloadSuccess.Bool(false))
		return nil, err
	}

	stream, err := c.client.ObtainSeeds(trace.ContextWithSpan(context.Background(), seedSpan), seedRequest)
	if err != nil {
		seedSpan.RecordError(err)
		seedSpan.SetAttributes(config.AttributePeerDownloadSuccess.Bool(false))
		if cdnErr, ok := err.(*dferrors.DfError); ok {
			logger.Errorf("failed to obtain cdn seed: %v", cdnErr)
			switch cdnErr.Code {
			case base.Code_CDNTaskRegistryFail:
				return nil, errors.Wrap(ErrCDNRegisterFail, "obtain seeds")
			case base.Code_CDNTaskDownloadFail:
				return nil, errors.Wrapf(ErrCDNDownloadFail, "obtain seeds")
			default:
				return nil, errors.Wrapf(ErrCDNUnknown, "obtain seeds")
			}
		}
		return nil, errors.Wrapf(ErrCDNInvokeFail, "obtain seeds from cdn: %v", err)
	}

	return c.receivePiece(ctx, task, stream)
}

func (c *cdn) receivePiece(ctx context.Context, task *Task, stream *cdnclient.PieceSeedStream) (*Peer, error) {
	span := trace.SpanFromContext(ctx)
	var initialized bool
	var cdnPeer *Peer
	for {
		piece, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				logger.Infof("task %s connection closed", task.ID)
				if cdnPeer != nil && task.GetStatus() == TaskStatusSuccess {
					span.SetAttributes(config.AttributePeerDownloadSuccess.Bool(true))
					return cdnPeer, nil
				}
				return nil, errors.Errorf("cdn stream receive EOF but task status is %s", task.GetStatus())
			}

			span.RecordError(err)
			span.SetAttributes(config.AttributePeerDownloadSuccess.Bool(false))
			logger.Errorf("task %s add piece err %v", task.ID, err)
			if recvErr, ok := err.(*dferrors.DfError); ok {
				switch recvErr.Code {
				case base.Code_CDNTaskRegistryFail:
					return nil, errors.Wrapf(ErrCDNRegisterFail, "receive piece")
				case base.Code_CDNTaskDownloadFail:
					return nil, errors.Wrapf(ErrCDNDownloadFail, "receive piece")
				default:
					return nil, errors.Wrapf(ErrCDNUnknown, "recive piece")
				}
			}
			return nil, errors.Wrapf(ErrCDNInvokeFail, "receive piece from cdn: %v", err)
		}

		if piece != nil {
			logger.Infof("task %s add piece %v", task.ID, piece)
			if !initialized {
				cdnPeer, err = c.initCDNPeer(ctx, task, piece)
				if err != nil {
					return nil, err
				}

				logger.Infof("task %s init cdn peer %v", task.ID, cdnPeer)
				if !task.CanSchedule() {
					task.SetStatus(TaskStatusSeeding)
				}
				initialized = true
			}

			span.AddEvent(config.EventCDNPieceReceived, trace.WithAttributes(config.AttributePieceReceived.String(piece.String())))
			cdnPeer.Touch()
			if piece.Done {
				logger.Infof("task %s receive pieces finish", task.ID)
				task.TotalPieceCount.Store(piece.TotalPieceCount)
				task.ContentLength.Store(piece.ContentLength)
				task.SetStatus(TaskStatusSuccess)
				cdnPeer.SetStatus(PeerStatusSuccess)
				if task.ContentLength.Load() <= TinyFileSize {
					data, err := downloadTinyFile(ctx, task, cdnPeer.Host)
					if err == nil && len(data) == int(task.ContentLength.Load()) {
						task.DirectPiece = data
					}
				}
				span.SetAttributes(config.AttributePeerDownloadSuccess.Bool(true))
				span.SetAttributes(config.AttributeContentLength.Int64(task.ContentLength.Load()))
				return cdnPeer, nil
			}

			cdnPeer.UpdateProgress(piece.PieceInfo.PieceNum+1, 0)
			task.GetOrAddPiece(piece.PieceInfo)
		}
	}
}

func (c *cdn) initCDNPeer(ctx context.Context, task *Task, ps *cdnsystem.PieceSeed) (*Peer, error) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent(config.EventCreateCDNPeer)

	var host *Host
	var ok bool
	peer, ok := c.peerManager.Get(ps.PeerId)
	if !ok {
		if host, ok = c.hostManager.Get(ps.HostUuid); !ok {
			if host, ok = c.client.GetHost(ps.HostUuid); !ok {
				logger.Errorf("cannot find cdn host %s", ps.HostUuid)
				return nil, errors.Wrapf(ErrInitCDNPeerFail, "cannot find host %s", ps.HostUuid)
			}
			c.hostManager.Add(host)
		}
		peer = NewPeer(ps.PeerId, task, host)
	}

	peer.SetStatus(PeerStatusRunning)
	c.peerManager.Add(peer)
	peer.Task.Log().Debugf("cdn peer %s has been added", peer.ID)
	return peer, nil
}

func downloadTinyFile(ctx context.Context, task *Task, cdnHost *Host) ([]byte, error) {
	// download url: http://${host}:${port}/download/${taskIndex}/${taskID}?peerId=scheduler;
	// taskIndex is the first three characters of taskID
	url := fmt.Sprintf("http://%s:%d/download/%s/%s?peerId=scheduler",
		cdnHost.IP, cdnHost.DownloadPort, task.ID[:3], task.ID)

	span := trace.SpanFromContext(ctx)
	span.AddEvent(config.EventDownloadTinyFile, trace.WithAttributes(config.AttributeDownloadFileURL.String(url)))

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return data, nil
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
