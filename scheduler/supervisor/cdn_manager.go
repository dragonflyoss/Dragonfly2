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

package supervisor

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
	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/client"
	cdnclient "d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/client"
	"d7y.io/dragonfly/v2/scheduler/config"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
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

type CDNManager interface {
	// StartSeedTask start seed cdn task
	StartSeedTask(context.Context, *Task) (*Peer, error)
}

type cdnManager struct {
	client      CDNDynmaicClient
	peerManager PeerManager
	hostManager HostManager
}

func NewCDNManager(client CDNDynmaicClient, peerManager PeerManager, hostManager HostManager) CDNManager {
	return &cdnManager{
		client:      client,
		peerManager: peerManager,
		hostManager: hostManager,
	}
}

func (cm *cdnManager) StartSeedTask(ctx context.Context, task *Task) (*Peer, error) {
	logger.Infof("start seed task %s", task.ID)
	defer logger.Infof("finish seed task %s, task status is %s", task.ID, task.GetStatus())

	var seedSpan trace.Span
	ctx, seedSpan = tracer.Start(ctx, config.SpanTriggerCDNSeed)
	defer seedSpan.End()
	seedRequest := &cdnsystem.SeedRequest{
		TaskId:  task.ID,
		Url:     task.URL,
		UrlMeta: task.URLMeta,
	}
	seedSpan.SetAttributes(config.AttributeCDNSeedRequest.String(seedRequest.String()))

	if cm.client == nil {
		err := ErrCDNClientUninitialized
		seedSpan.RecordError(err)
		seedSpan.SetAttributes(config.AttributePeerDownloadSuccess.Bool(false))
		return nil, err
	}

	stream, err := cm.client.ObtainSeeds(trace.ContextWithSpan(context.Background(), seedSpan), seedRequest)
	if err != nil {
		seedSpan.RecordError(err)
		seedSpan.SetAttributes(config.AttributePeerDownloadSuccess.Bool(false))
		if cdnErr, ok := err.(*dferrors.DfError); ok {
			logger.Errorf("failed to obtain cdn seed: %v", cdnErr)
			switch cdnErr.Code {
			case dfcodes.CdnTaskRegistryFail:
				return nil, errors.Wrap(ErrCDNRegisterFail, "obtain seeds")
			case dfcodes.CdnTaskDownloadFail:
				return nil, errors.Wrapf(ErrCDNDownloadFail, "obtain seeds")
			default:
				return nil, errors.Wrapf(ErrCDNUnknown, "obtain seeds")
			}
		}
		return nil, errors.Wrapf(ErrCDNInvokeFail, "obtain seeds from cdn: %v", err)
	}

	return cm.receivePiece(ctx, task, stream)
}

func (cm *cdnManager) receivePiece(ctx context.Context, task *Task, stream *client.PieceSeedStream) (*Peer, error) {
	span := trace.SpanFromContext(ctx)
	var initialized bool
	var cdnPeer *Peer

	for {
		piece, err := stream.Recv()
		if err == io.EOF {
			if task.GetStatus() == TaskStatusSuccess {
				span.SetAttributes(config.AttributePeerDownloadSuccess.Bool(true))
				return cdnPeer, nil
			}
			return cdnPeer, errors.Errorf("cdn stream receive EOF but task status is %s", task.GetStatus())
		}
		if err != nil {
			span.RecordError(err)
			span.SetAttributes(config.AttributePeerDownloadSuccess.Bool(false))
			if recvErr, ok := err.(*dferrors.DfError); ok {
				switch recvErr.Code {
				case dfcodes.CdnTaskRegistryFail:
					return cdnPeer, errors.Wrapf(ErrCDNRegisterFail, "receive piece")
				case dfcodes.CdnTaskDownloadFail:
					return cdnPeer, errors.Wrapf(ErrCDNDownloadFail, "receive piece")
				default:
					return cdnPeer, errors.Wrapf(ErrCDNUnknown, "recive piece")
				}
			}
			return cdnPeer, errors.Wrapf(ErrCDNInvokeFail, "receive piece from cdn: %v", err)
		}
		if piece != nil {
			if !initialized {
				cdnPeer, err = cm.initCDNPeer(ctx, task, piece)
				if !task.CanSchedule() {
					task.SetStatus(TaskStatusSeeding)
				}
				initialized = true
			}
			span.AddEvent(config.EventCDNPieceReceived, trace.WithAttributes(config.AttributePieceReceived.String(piece.String())))
			if err != nil || cdnPeer == nil {
				return cdnPeer, err
			}
			cdnPeer.Touch()
			if piece.Done {
				task.PieceTotal = piece.TotalPieceCount
				task.ContentLength = piece.ContentLength
				task.SetStatus(TaskStatusSuccess)
				cdnPeer.SetStatus(PeerStatusSuccess)
				if task.ContentLength <= TinyFileSize {
					content, er := cm.DownloadTinyFileContent(ctx, task, cdnPeer.Host)
					if er == nil && len(content) == int(task.ContentLength) {
						task.DirectPiece = content
					}
				}
				span.SetAttributes(config.AttributePeerDownloadSuccess.Bool(true))
				span.SetAttributes(config.AttributeContentLength.Int64(task.ContentLength))
				return cdnPeer, nil
			}
			cdnPeer.UpdateProgress(piece.PieceInfo.PieceNum+1, 0)
			task.GetOrAddPiece(piece.PieceInfo)
		}
	}
}

func (cm *cdnManager) initCDNPeer(ctx context.Context, task *Task, ps *cdnsystem.PieceSeed) (*Peer, error) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent(config.EventCreateCDNPeer)

	var host *Host
	var ok bool
	peer, ok := cm.peerManager.Get(ps.PeerId)
	if !ok {
		if host, ok = cm.hostManager.Get(ps.HostUuid); !ok {
			if host, ok = cm.client.GetHost(ps.HostUuid); !ok {
				logger.Errorf("cannot find cdn host %s", ps.HostUuid)
				return nil, errors.Wrapf(ErrInitCDNPeerFail, "cannot find host %s", ps.HostUuid)
			}
			cm.hostManager.Add(host)
		}
		peer = NewPeer(ps.PeerId, task, host)
	}

	peer.SetStatus(PeerStatusRunning)
	cm.peerManager.Add(peer)
	return peer, nil
}

func (cm *cdnManager) DownloadTinyFileContent(ctx context.Context, task *Task, cdnHost *Host) ([]byte, error) {
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

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return data, nil
}

type CDNDynmaicClient interface {
	cdnclient.CdnClient
	config.Observer
	GetHost(hostID string) (*Host, bool)
}

type cdnDynmaicClient struct {
	cdnclient.CdnClient
	hosts map[string]*Host
	cdns  []*config.CDN
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
		hosts:     cdnsToHosts(config.CDNs),
		cdns:      config.CDNs,
	}

	dynConfig.Register(dc)
	return dc, nil
}

func (dc *cdnDynmaicClient) GetHost(id string) (*Host, bool) {
	dc.lock.RLock()
	defer dc.lock.RUnlock()

	cdnHost, ok := dc.hosts[id]
	return cdnHost, ok
}

func (dc *cdnDynmaicClient) OnNotify(c *config.DynconfigData) {
	dc.lock.Lock()
	defer dc.lock.Unlock()

	if reflect.DeepEqual(dc.cdns, c.CDNs) {
		return
	}
	dc.hosts = cdnsToHosts(c.CDNs)
	dc.UpdateState(cdnsToNetAddrs(c.CDNs))
}

// cdnsToHosts coverts []*config.CDN to map[string]*Host.
func cdnsToHosts(cdns []*config.CDN) map[string]*Host {
	hosts := make(map[string]*Host, len(cdns))
	for _, cdn := range cdns {
		id := idgen.CDN(cdn.HostName, cdn.Port)
		hosts[id] = NewCDNHost(id, cdn.IP, cdn.HostName, cdn.Port, cdn.DownloadPort, cdn.SecurityGroup, cdn.Location,
			cdn.IDC, cdn.NetTopology, cdn.LoadLimit)
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
