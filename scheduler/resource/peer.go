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
	"io"
	"net/http"
	"net/url"
	"regexp"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/go-http-utils/headers"
	"github.com/looplab/fsm"
	"go.uber.org/atomic"

	commonv1 "d7y.io/api/pkg/apis/common/v1"
	managerv1 "d7y.io/api/pkg/apis/manager/v1"
	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/scheduler/config"
)

const (
	// Default value of tag.
	DefaultTag = "unknow"

	//DefaultApplication default value of application
	DefaultApplication = "unknown"

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

// PeerOption is a functional option for configuring the peer.
type PeerOption func(p *Peer) *Peer

// WithTag sets peer's Tag.
func WithTag(tag string) PeerOption {
	return func(p *Peer) *Peer {
		p.Tag = tag
		return p
	}
}

// WithApplication sets peer's Application.
func WithApplication(application string) PeerOption {
	return func(p *Peer) *Peer {
		p.Application = application
		return p
	}
}

type Peer struct {
	// ID is peer id.
	ID string

	// Tag is peer tag.
	Tag string

	// Application is peer application.
	Application string

	// Pieces is finished piece set.
	Pieces set.SafeSet[*schedulerv1.PieceResult]

	// Pieces is finished pieces bitset.
	FinishedPieces *bitset.BitSet

	// pieceCosts is piece downloaded time.
	pieceCosts []int64

	// Cost is the cost of downloading.
	Cost *atomic.Duration

	// Stream is grpc stream instance.
	Stream *atomic.Value

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

	// IsBackToSource is downloaded from source.
	//
	// When peer is scheduling and NeedBackToSource is true,
	// scheduler needs to return Code_SchedNeedBackSource and
	// IsBackToSource is set to true.
	IsBackToSource *atomic.Bool

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
func NewPeer(id string, task *Task, host *Host, options ...PeerOption) *Peer {
	p := &Peer{
		ID:               id,
		Tag:              DefaultTag,
		Application:      DefaultApplication,
		Pieces:           set.NewSafeSet[*schedulerv1.PieceResult](),
		FinishedPieces:   &bitset.BitSet{},
		pieceCosts:       []int64{},
		Cost:             atomic.NewDuration(0),
		Stream:           &atomic.Value{},
		Task:             task,
		Host:             host,
		BlockParents:     set.NewSafeSet[string](),
		NeedBackToSource: atomic.NewBool(false),
		IsBackToSource:   atomic.NewBool(false),
		PieceUpdatedAt:   atomic.NewTime(time.Now()),
		CreatedAt:        atomic.NewTime(time.Now()),
		UpdatedAt:        atomic.NewTime(time.Now()),
		Log:              logger.WithPeer(host.ID, task.ID, id),
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
			PeerEventRegisterEmpty: func(e *fsm.Event) {
				p.UpdatedAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventRegisterTiny: func(e *fsm.Event) {
				p.UpdatedAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventRegisterSmall: func(e *fsm.Event) {
				p.UpdatedAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventRegisterNormal: func(e *fsm.Event) {
				p.UpdatedAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventDownload: func(e *fsm.Event) {
				p.UpdatedAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventDownloadBackToSource: func(e *fsm.Event) {
				p.IsBackToSource.Store(true)
				p.Task.BackToSourcePeers.Add(p.ID)

				if err := p.Task.DeletePeerInEdges(p.ID); err != nil {
					p.Log.Errorf("delete peer inedges failed: %s", err.Error())
				}

				p.UpdatedAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventDownloadSucceeded: func(e *fsm.Event) {
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
			PeerEventDownloadFailed: func(e *fsm.Event) {
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
			PeerEventLeave: func(e *fsm.Event) {
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
func (p *Peer) AppendPieceCost(cost int64) {
	p.pieceCosts = append(p.pieceCosts, cost)
}

// PieceCosts return piece costs slice.
func (p *Peer) PieceCosts() []int64 {
	return p.pieceCosts
}

// LoadStream return grpc stream.
func (p *Peer) LoadStream() (schedulerv1.Scheduler_ReportPieceResultServer, bool) {
	rawStream := p.Stream.Load()
	if rawStream == nil {
		return nil, false
	}

	return rawStream.(schedulerv1.Scheduler_ReportPieceResultServer), true
}

// StoreStream set grpc stream.
func (p *Peer) StoreStream(stream schedulerv1.Scheduler_ReportPieceResultServer) {
	p.Stream.Store(stream)
}

// DeleteStream deletes grpc stream.
func (p *Peer) DeleteStream() {
	p.Stream = &atomic.Value{}
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

// DownloadTinyFile downloads tiny file from peer.
func (p *Peer) DownloadTinyFile() ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), downloadTinyFileContextTimeout)
	defer cancel()

	// Download url: http://${host}:${port}/download/${taskIndex}/${taskID}?peerId=${peerID}
	targetURL := url.URL{
		Scheme:   "http",
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

	resp, err := http.DefaultClient.Do(req)
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

// GetPriority returns priority of peer.
func (p *Peer) GetPriority(dynconfig config.DynconfigInterface) commonv1.Priority {
	pbApplications, err := dynconfig.GetApplications()
	if err != nil {
		p.Log.Warn(err)
		return commonv1.Priority_LEVEL0
	}

	// If manager has no applications,
	// then return Priority_LEVEL0.
	if len(pbApplications) == 0 {
		p.Log.Info("can not found applications")
		return commonv1.Priority_LEVEL0
	}

	// Find peer application.
	var application *managerv1.Application
	for _, pbApplication := range pbApplications {
		if p.Application == pbApplication.Name {
			application = pbApplication
			break
		}
	}

	// If no application matches peer application,
	// then return Priority_LEVEL0.
	if application == nil {
		p.Log.Info("can not found matching application")
		return commonv1.Priority_LEVEL0
	}

	// If application has no priority,
	// then return Priority_LEVEL0.
	if application.Priority == nil {
		p.Log.Info("can not found priority")
		return commonv1.Priority_LEVEL0
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
