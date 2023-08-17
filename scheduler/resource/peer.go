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
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sync"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/go-http-utils/headers"
	"github.com/looplab/fsm"
	"go.uber.org/atomic"

	commonv2 "d7y.io/api/v2/pkg/apis/common/v2"
	managerv2 "d7y.io/api/v2/pkg/apis/manager/v2"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"
	schedulerv2 "d7y.io/api/v2/pkg/apis/scheduler/v2"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/container/set"
	nethttp "d7y.io/dragonfly/v2/pkg/net/http"
	"d7y.io/dragonfly/v2/scheduler/config"
)

const (
	// Download tiny file timeout.
	downloadTinyFileContextTimeout = 30 * time.Second
)

const (
	// Peer has been created but did not start running.
	PeerStatePending = "Pending"

	// Peer successfully registered as empty scope size.
	PeerStateReceivedEmpty = "ReceivedEmpty"

	// Peer successfully registered as tiny scope size.
	PeerStateReceivedTiny = "ReceivedTiny"

	// Peer successfully registered as small scope size.
	PeerStateReceivedSmall = "ReceivedSmall"

	// Peer successfully registered as normal scope size.
	PeerStateReceivedNormal = "ReceivedNormal"

	// Peer is downloading resources from peer.
	PeerStateRunning = "Running"

	// Peer is downloading resources from back-to-source.
	PeerStateBackToSource = "BackToSource"

	// Peer has been downloaded successfully.
	PeerStateSucceeded = "Succeeded"

	// Peer has been downloaded failed.
	PeerStateFailed = "Failed"

	// Peer has been left.
	PeerStateLeave = "Leave"
)

const (
	// Peer is registered as empty scope size.
	PeerEventRegisterEmpty = "RegisterEmpty"

	// Peer is registered as tiny scope size.
	PeerEventRegisterTiny = "RegisterTiny"

	// Peer is registered as small scope size.
	PeerEventRegisterSmall = "RegisterSmall"

	// Peer is registered as normal scope size.
	PeerEventRegisterNormal = "RegisterNormal"

	// Peer is downloading.
	PeerEventDownload = "Download"

	// Peer is downloading back-to-source.
	PeerEventDownloadBackToSource = "DownloadBackToSource"

	// Peer downloaded successfully.
	PeerEventDownloadSucceeded = "DownloadSucceeded"

	// Peer downloaded failed.
	PeerEventDownloadFailed = "DownloadFailed"

	// Peer leaves.
	PeerEventLeave = "Leave"
)

// PeerOption is a functional option for peer.
type PeerOption func(peer *Peer)

// WithAnnouncePeerStream set AnnouncePeerStream for peer.
func WithAnnouncePeerStream(stream schedulerv2.Scheduler_AnnouncePeerServer) PeerOption {
	return func(p *Peer) {
		p.StoreAnnouncePeerStream(stream)
	}
}

// WithPriority set Priority for peer.
func WithPriority(priority commonv2.Priority) PeerOption {
	return func(p *Peer) {
		p.Priority = priority
	}
}

// WithRange set Range for peer.
func WithRange(rg nethttp.Range) PeerOption {
	return func(p *Peer) {
		p.Range = &rg
	}
}

// Peer contains content for peer.
type Peer struct {
	// ID is peer id.
	ID string

	// Config is resource config.
	Config *config.ResourceConfig

	// Range is url range of request.
	Range *nethttp.Range

	// Priority is peer priority.
	Priority commonv2.Priority

	// Piece sync map.
	Pieces *sync.Map

	// Pieces is finished pieces bitset.
	FinishedPieces *bitset.BitSet

	// pieceCosts is piece downloaded duration.
	pieceCosts []time.Duration

	// Cost is the cost of downloading.
	Cost *atomic.Duration

	// ReportPieceResultStream is the grpc stream of Scheduler_ReportPieceResultServer,
	// Used only in v1 version of the grpc.
	ReportPieceResultStream *atomic.Value

	// AnnouncePeerStream is the grpc stream of Scheduler_AnnouncePeerServer,
	// Used only in v2 version of the grpc.
	AnnouncePeerStream *atomic.Value

	// Task state machine.
	FSM *fsm.FSM

	// Task is peer task.
	Task *Task

	// Host is peer host.
	Host *Host

	// BlockParents is bad parents ids.
	BlockParents set.SafeSet[string]

	// NeedBackToSource needs downloaded from source.
	//
	// When peer is registering, at the same time,
	// scheduler needs to create the new corresponding task and the seed peer is disabled,
	// NeedBackToSource is set to true.
	NeedBackToSource *atomic.Bool

	// PieceUpdatedAt is piece update time.
	PieceUpdatedAt *atomic.Time

	// CreatedAt is peer create time.
	CreatedAt *atomic.Time

	// UpdatedAt is peer update time.
	UpdatedAt *atomic.Time

	// Peer log.
	Log *logger.SugaredLoggerOnWith
}

// New Peer instance.
func NewPeer(id string, cfg *config.ResourceConfig, task *Task, host *Host, options ...PeerOption) *Peer {
	p := &Peer{
		ID:                      id,
		Config:                  cfg,
		Priority:                commonv2.Priority_LEVEL0,
		Pieces:                  &sync.Map{},
		FinishedPieces:          &bitset.BitSet{},
		pieceCosts:              []time.Duration{},
		Cost:                    atomic.NewDuration(0),
		ReportPieceResultStream: &atomic.Value{},
		AnnouncePeerStream:      &atomic.Value{},
		Task:                    task,
		Host:                    host,
		BlockParents:            set.NewSafeSet[string](),
		NeedBackToSource:        atomic.NewBool(false),
		PieceUpdatedAt:          atomic.NewTime(time.Now()),
		CreatedAt:               atomic.NewTime(time.Now()),
		UpdatedAt:               atomic.NewTime(time.Now()),
		Log:                     logger.WithPeer(host.ID, task.ID, id),
	}

	// Initialize state machine.
	p.FSM = fsm.NewFSM(
		PeerStatePending,
		fsm.Events{
			{Name: PeerEventRegisterEmpty, Src: []string{PeerStatePending}, Dst: PeerStateReceivedEmpty},
			{Name: PeerEventRegisterTiny, Src: []string{PeerStatePending}, Dst: PeerStateReceivedTiny},
			{Name: PeerEventRegisterSmall, Src: []string{PeerStatePending}, Dst: PeerStateReceivedSmall},
			{Name: PeerEventRegisterNormal, Src: []string{PeerStatePending}, Dst: PeerStateReceivedNormal},
			{Name: PeerEventDownload, Src: []string{PeerStateReceivedEmpty, PeerStateReceivedTiny, PeerStateReceivedSmall, PeerStateReceivedNormal}, Dst: PeerStateRunning},
			{Name: PeerEventDownloadBackToSource, Src: []string{PeerStateReceivedEmpty, PeerStateReceivedTiny, PeerStateReceivedSmall, PeerStateReceivedNormal, PeerStateRunning}, Dst: PeerStateBackToSource},
			{Name: PeerEventDownloadSucceeded, Src: []string{
				// Since ReportPeerResult and ReportPieceResult are called in no order,
				// the result may be reported after the register is successful.
				PeerStateReceivedEmpty, PeerStateReceivedTiny, PeerStateReceivedSmall, PeerStateReceivedNormal,
				PeerStateRunning, PeerStateBackToSource,
			}, Dst: PeerStateSucceeded},
			{Name: PeerEventDownloadFailed, Src: []string{
				PeerStatePending, PeerStateReceivedEmpty, PeerStateReceivedTiny, PeerStateReceivedSmall, PeerStateReceivedNormal,
				PeerStateRunning, PeerStateBackToSource, PeerStateSucceeded,
			}, Dst: PeerStateFailed},
			{Name: PeerEventLeave, Src: []string{
				PeerStatePending, PeerStateReceivedEmpty, PeerStateReceivedTiny, PeerStateReceivedSmall, PeerStateReceivedNormal,
				PeerStateRunning, PeerStateBackToSource, PeerStateFailed, PeerStateSucceeded,
			}, Dst: PeerStateLeave},
		},
		fsm.Callbacks{
			PeerEventRegisterEmpty: func(ctx context.Context, e *fsm.Event) {
				p.UpdatedAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventRegisterTiny: func(ctx context.Context, e *fsm.Event) {
				p.UpdatedAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventRegisterSmall: func(ctx context.Context, e *fsm.Event) {
				p.UpdatedAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventRegisterNormal: func(ctx context.Context, e *fsm.Event) {
				p.UpdatedAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventDownload: func(ctx context.Context, e *fsm.Event) {
				p.UpdatedAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventDownloadBackToSource: func(ctx context.Context, e *fsm.Event) {
				p.Task.BackToSourcePeers.Add(p.ID)

				if err := p.Task.DeletePeerInEdges(p.ID); err != nil {
					p.Log.Errorf("delete peer inedges failed: %s", err.Error())
				}

				p.UpdatedAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventDownloadSucceeded: func(ctx context.Context, e *fsm.Event) {
				if e.Src == PeerStateBackToSource {
					p.Task.BackToSourcePeers.Delete(p.ID)
				}

				if err := p.Task.DeletePeerInEdges(p.ID); err != nil {
					p.Log.Errorf("delete peer inedges failed: %s", err.Error())
				}

				p.Task.PeerFailedCount.Store(0)
				p.UpdatedAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventDownloadFailed: func(ctx context.Context, e *fsm.Event) {
				if e.Src == PeerStateBackToSource {
					p.Task.PeerFailedCount.Inc()
					p.Task.BackToSourcePeers.Delete(p.ID)
				}

				if err := p.Task.DeletePeerInEdges(p.ID); err != nil {
					p.Log.Errorf("delete peer inedges failed: %s", err.Error())
				}

				p.UpdatedAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventLeave: func(ctx context.Context, e *fsm.Event) {
				if err := p.Task.DeletePeerInEdges(p.ID); err != nil {
					p.Log.Errorf("delete peer inedges failed: %s", err.Error())
				}

				p.Task.BackToSourcePeers.Delete(p.ID)
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
		},
	)

	for _, opt := range options {
		opt(p)
	}

	return p
}

// AppendPieceCost append piece cost to costs slice.
func (p *Peer) AppendPieceCost(duration time.Duration) {
	p.pieceCosts = append(p.pieceCosts, duration)
}

// PieceCosts return piece costs slice.
func (p *Peer) PieceCosts() []time.Duration {
	return p.pieceCosts
}

// LoadReportPieceResultStream return the grpc stream of Scheduler_ReportPieceResultServer,
// Used only in v1 version of the grpc.
func (p *Peer) LoadReportPieceResultStream() (schedulerv1.Scheduler_ReportPieceResultServer, bool) {
	rawStream := p.ReportPieceResultStream.Load()
	if rawStream == nil {
		return nil, false
	}

	return rawStream.(schedulerv1.Scheduler_ReportPieceResultServer), true
}

// StoreReportPieceResultStream set the grpc stream of Scheduler_ReportPieceResultServer,
// Used only in v1 version of the grpc.
func (p *Peer) StoreReportPieceResultStream(stream schedulerv1.Scheduler_ReportPieceResultServer) {
	p.ReportPieceResultStream.Store(stream)
}

// DeleteReportPieceResultStream deletes the grpc stream of Scheduler_ReportPieceResultServer,
// Used only in v1 version of the grpc.
func (p *Peer) DeleteReportPieceResultStream() {
	p.ReportPieceResultStream = &atomic.Value{}
}

// LoadAnnouncePeerStream return the grpc stream of Scheduler_AnnouncePeerServer,
// Used only in v2 version of the grpc.
func (p *Peer) LoadAnnouncePeerStream() (schedulerv2.Scheduler_AnnouncePeerServer, bool) {
	rawStream := p.AnnouncePeerStream.Load()
	if rawStream == nil {
		return nil, false
	}

	return rawStream.(schedulerv2.Scheduler_AnnouncePeerServer), true
}

// StoreAnnouncePeerStream set the grpc stream of Scheduler_AnnouncePeerServer,
// Used only in v2 version of the grpc.
func (p *Peer) StoreAnnouncePeerStream(stream schedulerv2.Scheduler_AnnouncePeerServer) {
	p.AnnouncePeerStream.Store(stream)
}

// DeleteAnnouncePeerStream deletes the grpc stream of Scheduler_AnnouncePeerServer,
// Used only in v2 version of the grpc.
func (p *Peer) DeleteAnnouncePeerStream() {
	p.AnnouncePeerStream = &atomic.Value{}
}

// LoadPiece return piece for a key.
func (p *Peer) LoadPiece(key int32) (*Piece, bool) {
	rawPiece, loaded := p.Pieces.Load(key)
	if !loaded {
		return nil, false
	}

	return rawPiece.(*Piece), loaded
}

// StorePiece set piece.
func (p *Peer) StorePiece(piece *Piece) {
	p.Pieces.Store(piece.Number, piece)
}

// DeletePiece deletes piece for a key.
func (p *Peer) DeletePiece(key int32) {
	p.Pieces.Delete(key)
}

// Parents returns parents of peer.
func (p *Peer) Parents() []*Peer {
	vertex, err := p.Task.DAG.GetVertex(p.ID)
	if err != nil {
		p.Log.Warn("can not find vertex in dag")
		return nil
	}

	var parents []*Peer
	for _, parent := range vertex.Parents.Values() {
		if parent.Value == nil {
			continue
		}

		parents = append(parents, parent.Value)
	}

	return parents
}

// Children returns children of peer.
func (p *Peer) Children() []*Peer {
	vertex, err := p.Task.DAG.GetVertex(p.ID)
	if err != nil {
		p.Log.Warn("can not find vertex in dag")
		return nil
	}

	var children []*Peer
	for _, child := range vertex.Children.Values() {
		if child.Value == nil {
			continue
		}

		children = append(children, child.Value)
	}

	return children
}

// DownloadTinyFile downloads tiny file from peer without range.
func (p *Peer) DownloadTinyFile() ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), downloadTinyFileContextTimeout)
	defer cancel()
	if len(p.Task.ID) <= 3 {
		return nil, fmt.Errorf("invalid task id")
	}
	// Download path: ${host}:${port}/download/${taskIndex}/${taskID}?peerId=${peerID}
	targetURL := url.URL{
		Scheme:   p.Config.Task.DownloadTiny.Scheme,
		Host:     fmt.Sprintf("%s:%d", p.Host.IP, p.Host.DownloadPort),
		Path:     fmt.Sprintf("download/%s/%s", p.Task.ID[:3], p.Task.ID),
		RawQuery: fmt.Sprintf("peerId=%s", p.ID),
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL.String(), nil)
	if err != nil {
		return []byte{}, err
	}

	req.Header.Set(headers.Range, fmt.Sprintf("bytes=%d-%d", 0, p.Task.ContentLength.Load()-1))
	p.Log.Infof("download tiny file %s, header is : %#v", targetURL.String(), req.Header)

	client := &http.Client{
		Timeout: p.Config.Task.DownloadTiny.Timeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: p.Config.Task.DownloadTiny.TLS.InsecureSkipVerify},
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// The HTTP 206 Partial Content success status response code indicates that
	// the request has succeeded and the body contains the requested ranges of data, as described in the Range header of the request.
	// Refer to https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/206.
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("bad response status %s", resp.Status)
	}

	return io.ReadAll(resp.Body)
}

// CalculatePriority returns priority of peer.
func (p *Peer) CalculatePriority(dynconfig config.DynconfigInterface) commonv2.Priority {
	if p.Priority != commonv2.Priority_LEVEL0 {
		return p.Priority
	}

	pbApplications, err := dynconfig.GetApplications()
	if err != nil {
		p.Log.Info(err)
		return commonv2.Priority_LEVEL0
	}

	// Find peer application.
	var application *managerv2.Application
	for _, pbApplication := range pbApplications {
		if p.Task.Application == pbApplication.Name {
			application = pbApplication
			break
		}
	}

	// If no application matches peer application,
	// then return Priority_LEVEL0.
	if application == nil {
		p.Log.Info("can not found matching application")
		return commonv2.Priority_LEVEL0
	}

	// If application has no priority,
	// then return Priority_LEVEL0.
	if application.Priority == nil {
		p.Log.Info("can not found priority")
		return commonv2.Priority_LEVEL0
	}

	// Match url priority first.
	for _, url := range application.Priority.Urls {
		matched, err := regexp.MatchString(url.Regex, p.Task.URL)
		if err != nil {
			p.Log.Warn(err)
			continue
		}

		if matched {
			return url.Value
		}
	}

	return application.Priority.Value
}
