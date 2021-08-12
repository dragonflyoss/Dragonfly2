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
	"reflect"
	"sync"

	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/client"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/supervise"
	"d7y.io/dragonfly/v2/scheduler/supervise/cdn"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

var tracer trace.Tracer

func init() {
	tracer = otel.Tracer("scheduler-cdn")
}

type manager struct {
	cdnAddrs    []dfnet.NetAddr
	client      client.CdnClient
	peerManager supervise.PeerMgr
	hostManager supervise.HostMgr
	lock        sync.RWMutex
}

func NewManager(cdnServers []*config.CDN, peerManager supervise.PeerMgr, hostManager supervise.HostMgr) (supervise.CDNMgr, error) {
	// Initialize CDNManager client
	cdnAddrs := cdnHostsToNetAddrs(cdnServers)
	cdnClient, err := client.GetClientByAddr(cdnAddrs)
	if err != nil {
		return nil, errors.Wrapf(err, "create cdn client for scheduler")
	}
	mgr := &manager{
		cdnAddrs:    cdnAddrs,
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
	netAddrs := cdnHostsToNetAddrs(c.CDNs)
	if reflect.DeepEqual(netAddrs, c.CDNs) {
		return
	}
	cm.lock.Lock()
	defer cm.lock.Unlock()
	// Sync CDNManager client netAddrs
	cm.cdnAddrs = netAddrs
	cm.client.UpdateState(netAddrs)
}

func (cm *manager) StartSeedTask(ctx context.Context, task *supervise.Task) (*supervise.Peer, error) {
	var seedSpan trace.Span
	ctx, seedSpan = tracer.Start(ctx, config.SpanTriggerCDN, trace.WithSpanKind(trace.SpanKindClient))
	defer seedSpan.End()
	seedRequest := &cdnsystem.SeedRequest{
		TaskId:  task.TaskID,
		Url:     task.URL,
		UrlMeta: task.URLMeta,
	}
	seedSpan.SetAttributes(config.AttributeCDNSeedRequest.String(seedRequest.String()))
	if cm.client == nil {
		err := cdn.ErrCDNRegisterFail
		seedSpan.RecordError(err)
		seedSpan.SetAttributes(config.AttributePeerDownloadSuccess.Bool(false))
		return nil, err
	}
	stream, err := cm.client.ObtainSeeds(context.Background(), seedRequest)
	if err != nil {
		seedSpan.RecordError(err)
		seedSpan.SetAttributes(config.AttributePeerDownloadSuccess.Bool(false))
		if cdnErr, ok := err.(*dferrors.DfError); ok {
			logger.Errorf("failed to obtain cdn seed: %v", cdnErr)
			switch cdnErr.Code {
			case dfcodes.CdnTaskRegistryFail:
				return nil, errors.Wrap(cdn.ErrCDNRegisterFail, "obtain seeds")
			case dfcodes.CdnTaskDownloadFail:
				return nil, errors.Wrapf(cdn.ErrCDNDownloadFail, "obtain seeds")
			default:
				return nil, errors.Wrapf(cdn.ErrCDNUnknown, "obtain seeds")
			}
		}
		return nil, errors.Wrapf(cdn.ErrCDNInvokeFail, "obtain seeds from cdn: %v", err)
	}
	return cm.receivePiece(ctx, task, stream)
}

func (cm *manager) receivePiece(ctx context.Context, task *supervise.Task, stream *client.PieceSeedStream) (*supervise.Peer, error) {
	span := trace.SpanFromContext(ctx)
	var initialized bool
	var cdnPeer *supervise.Peer
	for {
		piece, err := stream.Recv()
		if err == io.EOF {
			if task.GetStatus() == supervise.TaskStatusSuccess {
				span.SetAttributes(config.AttributePeerDownloadSuccess.Bool(true))
				return cdnPeer, nil
			}
			return cdnPeer, errors.Errorf("cdn stream receive EOF but task status is %s", task.GetStatus())
		}
		if err != nil {
			span.RecordError(err)
			span.SetAttributes(config.AttributePeerDownloadSuccess.Bool(false))
			if recvErr, ok := err.(*dferrors.DfError); ok {
				span.RecordError(recvErr)
				switch recvErr.Code {
				case dfcodes.CdnTaskRegistryFail:
					return cdnPeer, errors.Wrapf(cdn.ErrCDNRegisterFail, "receive piece")
				case dfcodes.CdnTaskDownloadFail:
					return cdnPeer, errors.Wrapf(cdn.ErrCDNDownloadFail, "receive piece")
				default:
					return cdnPeer, errors.Wrapf(cdn.ErrCDNUnknown, "recive piece")
				}
			}
			return cdnPeer, errors.Wrapf(cdn.ErrCDNInvokeFail, "receive piece from cdn: %v", err)
		}
		if piece != nil {
			span.AddEvent(config.EventPieceReceived, trace.WithAttributes(config.AttributePieceReceived.String(piece.String())))
			if !initialized {
				cdnPeer, err = cm.initCdnPeer(ctx, task, piece)
				task.SetStatus(supervise.TaskStatusSeeding)
				initialized = true
			}
			if err != nil || cdnPeer == nil {
				return cdnPeer, err
			}
			cdnPeer.Touch()
			if piece.Done {
				task.PieceTotal = piece.TotalPieceCount
				task.ContentLength = piece.ContentLength
				task.SetStatus(supervise.TaskStatusSuccess)
				cdnPeer.SetStatus(supervise.PeerStatusSuccess)
				if task.ContentLength <= supervise.TinyFileSize {
					content, er := cm.DownloadTinyFileContent(ctx, task, cdnPeer.Host)
					if er == nil && len(content) == int(task.ContentLength) {
						task.DirectPiece = content
					}
				}
				span.SetAttributes(config.AttributePeerDownloadSuccess.Bool(true))
				span.SetAttributes(config.AttributeContentLength.Int64(task.ContentLength))
				return cdnPeer, nil
			}
			cdnPeer.AddPieceInfo(piece.PieceInfo.PieceNum+1, 0)
			task.AddPiece(&supervise.PieceInfo{
				PieceNum:    piece.PieceInfo.PieceNum,
				RangeStart:  piece.PieceInfo.RangeStart,
				RangeSize:   piece.PieceInfo.RangeSize,
				PieceMd5:    piece.PieceInfo.PieceMd5,
				PieceOffset: piece.PieceInfo.PieceOffset,
				PieceStyle:  piece.PieceInfo.PieceStyle,
			})
		}
	}
}

func (cm *manager) initCdnPeer(ctx context.Context, task *supervise.Task, ps *cdnsystem.PieceSeed) (*supervise.Peer, error) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent(config.EventCreatePeer)
	var ok bool
	var cdnHost *supervise.PeerHost
	cdnPeer, ok := cm.peerManager.Get(ps.PeerId)
	if !ok {
		logger.Debugf("first seed cdn task for taskID %s", task.TaskID)
		if cdnHost, ok = cm.hostManager.Get(ps.HostUuid); !ok {
			logger.Errorf("cannot find host %s", ps.HostUuid)
			return nil, errors.Wrapf(cdn.ErrInitCDNPeerFail, "cannot find host %s", ps.HostUuid)
		}
		cdnPeer = supervise.NewPeer(ps.PeerId, task, cdnHost)
	}
	cdnPeer.SetStatus(supervise.PeerStatusRunning)
	cm.peerManager.Add(cdnPeer)
	return cdnPeer, nil
}

func (cm *manager) DownloadTinyFileContent(ctx context.Context, task *supervise.Task, cdnHost *supervise.PeerHost) ([]byte, error) {
	span := trace.SpanFromContext(ctx)
	// no need to invoke getPieceTasks method
	// TODO download the tiny file
	// http://host:port/download/{taskId 前3位}/{taskId}?peerId={peerId};
	url := fmt.Sprintf("http://%s:%d/download/%s/%s?peerId=scheduler",
		cdnHost.IP, cdnHost.DownloadPort, task.TaskID[:3], task.TaskID)
	span.SetAttributes(config.AttributeDownloadFileURL.String(url))
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
