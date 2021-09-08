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

package core

import (
	"context"
	"sync"
	"time"

	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	schedulerRPC "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/synclock"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/core/scheduler"
	"d7y.io/dragonfly/v2/scheduler/supervisor"
	"d7y.io/dragonfly/v2/scheduler/supervisor/cdn"
	"d7y.io/dragonfly/v2/scheduler/supervisor/host"
	"d7y.io/dragonfly/v2/scheduler/supervisor/peer"
	"d7y.io/dragonfly/v2/scheduler/supervisor/task"
	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/workqueue"
)

const maxRescheduleTimes = 8

type Options struct {
	openTel    bool
	disableCDN bool
}

type Option func(options *Options)

func WithOpenTel(openTel bool) Option {
	return func(options *Options) {
		options.openTel = openTel
	}
}

func WithDisableCDN(disableCDN bool) Option {
	return func(options *Options) {
		options.disableCDN = disableCDN
	}
}

type SchedulerService struct {
	// cdn mgr
	cdnManager supervisor.CDNMgr
	// task mgr
	taskManager supervisor.TaskMgr
	// host mgr
	hostManager supervisor.HostMgr
	// Peer mgr
	peerManager supervisor.PeerMgr

	sched     scheduler.Scheduler
	worker    worker
	config    *config.SchedulerConfig
	monitor   *monitor
	startOnce sync.Once
	stopOnce  sync.Once
	done      chan struct{}
	wg        sync.WaitGroup
}

func NewSchedulerService(cfg *config.SchedulerConfig, dynConfig config.DynconfigInterface, options ...Option) (*SchedulerService, error) {
	ops := &Options{}
	for _, op := range options {
		op(ops)
	}

	hostManager := host.NewManager()
	peerManager := peer.NewManager(cfg.GC, hostManager)
	taskManager := task.NewManager(cfg.GC, peerManager)
	sched, err := scheduler.Get(cfg.Scheduler).Build(cfg, &scheduler.BuildOptions{
		PeerManager: peerManager,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "build scheduler %v", cfg.Scheduler)
	}

	work := newEventLoopGroup(cfg.WorkerNum)
	downloadMonitor := newMonitor(cfg.OpenMonitor, peerManager)
	done := make(chan struct{})
	s := &SchedulerService{
		taskManager: taskManager,
		hostManager: hostManager,
		peerManager: peerManager,
		worker:      work,
		monitor:     downloadMonitor,
		sched:       sched,
		config:      cfg,
		done:        done,
	}
	if !ops.disableCDN {
		var opts []grpc.DialOption
		if ops.openTel {
			opts = append(opts, grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()), grpc.WithChainStreamInterceptor(otelgrpc.StreamClientInterceptor()))
		}
		cdnClient, err := cdn.NewRefreshableCDNClient(dynConfig, opts)
		if err != nil {
			return nil, errors.Wrap(err, "new refreshable cdn client")
		}
		cdnManager, err := cdn.NewManager(cdnClient, peerManager, hostManager)
		if err != nil {
			return nil, errors.Wrap(err, "new cdn manager")
		}
		s.cdnManager = cdnManager
	}
	return s, nil
}

func (s *SchedulerService) Serve() {
	s.startOnce.Do(func() {
		s.wg.Add(3)
		wsdq := workqueue.NewNamedDelayingQueue("wait reSchedule parent")
		go s.runWorkerLoop(wsdq)
		go s.runReScheduleParentLoop(wsdq)
		go s.runMonitor()
		logger.Debugf("start scheduler service successfully")
	})
}

func (s *SchedulerService) runWorkerLoop(wsdq workqueue.DelayingInterface) {
	defer s.wg.Done()
	s.worker.start(newState(s.sched, s.peerManager, s.cdnManager, wsdq))
}

func (s *SchedulerService) runReScheduleParentLoop(wsdq workqueue.DelayingInterface) {
	defer s.wg.Done()
	for {
		select {
		case <-s.done:
			wsdq.ShutDown()
			return
		default:
			v, shutdown := wsdq.Get()
			if shutdown {
				logger.Infof("wait schedule delay queue is shutdown")
				break
			}
			rsPeer := v.(*rsPeer)
			peer := rsPeer.peer
			wsdq.Done(v)
			if rsPeer.times > maxRescheduleTimes {
				if peer.CloseChannel(dferrors.Newf(dfcodes.SchedNeedBackSource, "reschedule parent for peer %s already reaches max reschedule times",
					peer.PeerID)) == nil {
					peer.Task.IncreaseBackSourcePeer(peer.PeerID)
				}
				continue
			}
			if peer.Task.IsBackSourcePeer(peer.PeerID) {
				logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("runReScheduleLoop: peer is back source client, no need to reschedule it")
				continue
			}
			if peer.IsDone() || peer.IsLeave() {
				peer.Log().Debugf("runReScheduleLoop: peer has left from waitScheduleParentPeerQueue because peer is done or leave, peer status is %s, "+
					"isLeave %t", peer.GetStatus(), peer.IsLeave())
				continue
			}
			s.worker.send(reScheduleParentEvent{rsPeer: rsPeer})
		}
	}
}

func (s *SchedulerService) runMonitor() {
	defer s.wg.Done()
	if s.monitor != nil {
		s.monitor.start(s.done)
	}
}
func (s *SchedulerService) Stop() {
	s.stopOnce.Do(func() {
		close(s.done)
		if s.worker != nil {
			s.worker.stop()
		}
		s.wg.Wait()
	})
}

func (s *SchedulerService) GenerateTaskID(url string, meta *base.UrlMeta, peerID string) (taskID string) {
	if s.config.ABTest {
		return idgen.TwinsTaskID(url, meta, peerID)
	}
	return idgen.TaskID(url, meta)
}

func (s *SchedulerService) SelectParent(peer *supervisor.Peer) (parent *supervisor.Peer, err error) {
	parent, _, hasParent := s.sched.ScheduleParent(peer, sets.NewString())
	if !hasParent || parent == nil {
		return nil, errors.Errorf("no parent peer available for peer %v", peer.PeerID)
	}
	return parent, nil
}

func (s *SchedulerService) GetPeerTask(peerTaskID string) (peerTask *supervisor.Peer, ok bool) {
	return s.peerManager.Get(peerTaskID)
}

func (s *SchedulerService) RegisterPeerTask(req *schedulerRPC.PeerTaskRequest, task *supervisor.Task) *supervisor.Peer {
	// get or create host
	reqPeerHost := req.PeerHost
	peerHost, ok := s.hostManager.Get(reqPeerHost.Uuid)
	if !ok {
		peerHost = supervisor.NewClientPeerHost(reqPeerHost.Uuid, reqPeerHost.Ip, reqPeerHost.HostName, reqPeerHost.RpcPort, reqPeerHost.DownPort,
			reqPeerHost.SecurityDomain, reqPeerHost.Location, reqPeerHost.Idc, reqPeerHost.NetTopology, s.config.ClientLoad)
		s.hostManager.Add(peerHost)
	}
	// get or creat PeerTask
	peer, ok := s.peerManager.Get(req.PeerId)
	if ok {
		logger.Warnf("peer %s has already registered", peer.PeerID)
		return peer
	}
	peer = supervisor.NewPeer(req.PeerId, task, peerHost)
	s.peerManager.Add(peer)
	return peer
}

func (s *SchedulerService) GetOrCreateTask(ctx context.Context, task *supervisor.Task) *supervisor.Task {
	span := trace.SpanFromContext(ctx)
	synclock.Lock(task.TaskID, true)
	task, ok := s.taskManager.GetOrAdd(task)
	if ok {
		span.SetAttributes(config.AttributeTaskStatus.String(task.GetStatus().String()))
		span.SetAttributes(config.AttributeLastTriggerTime.String(task.GetLastTriggerTime().String()))
		if task.GetLastTriggerTime().Add(s.config.AccessWindow).After(time.Now()) || task.IsHealth() {
			synclock.UnLock(task.TaskID, true)
			span.SetAttributes(config.AttributeNeedSeedCDN.Bool(false))
			return task
		}
	} else {
		task.Log().Infof("add new task %s", task.TaskID)
	}

	synclock.UnLock(task.TaskID, true)
	// do trigger
	task.UpdateLastTriggerTime(time.Now())

	synclock.Lock(task.TaskID, false)
	defer synclock.UnLock(task.TaskID, false)
	span.SetAttributes(config.AttributeTaskStatus.String(task.GetStatus().String()))
	span.SetAttributes(config.AttributeLastTriggerTime.String(task.GetLastTriggerTime().String()))
	if task.IsHealth() {
		span.SetAttributes(config.AttributeNeedSeedCDN.Bool(false))
		return task
	}
	task.SetStatus(supervisor.TaskStatusRunning)
	if s.cdnManager == nil {
		// client back source
		span.SetAttributes(config.AttributeClientBackSource.Bool(true))
		task.SetClientBackSourceStatusAndLimit(s.config.BackSourceCount)
		return task
	}
	span.SetAttributes(config.AttributeNeedSeedCDN.Bool(true))
	go func() {
		if cdnPeer, err := s.cdnManager.StartSeedTask(ctx, task); err != nil {
			// fall back to client back source
			task.Log().Errorf("seed task failed: %v", err)
			span.AddEvent(config.EventCDNFailBackClientSource, trace.WithAttributes(config.AttributeTriggerCDNError.String(err.Error())))
			task.SetClientBackSourceStatusAndLimit(s.config.BackSourceCount)
			if ok = s.worker.send(taskSeedFailEvent{task}); !ok {
				logger.Error("send taskSeed fail event failed, eventLoop is shutdown")
			}
		} else {
			if ok = s.worker.send(peerDownloadSuccessEvent{cdnPeer, nil}); !ok {
				logger.Error("send taskSeed success event failed, eventLoop is shutdown")
			}
			logger.Debugf("===== successfully obtain seeds from cdn, task: %+v ====", task)
		}
	}()
	return task
}

func (s *SchedulerService) HandlePieceResult(ctx context.Context, peer *supervisor.Peer, pieceResult *schedulerRPC.PieceResult) error {
	peer.Touch()
	if pieceResult.PieceInfo != nil && pieceResult.PieceInfo.PieceNum == common.EndOfPiece {
		return nil
	} else if pieceResult.PieceInfo != nil && pieceResult.PieceInfo.PieceNum == common.ZeroOfPiece {
		s.worker.send(startReportPieceResultEvent{ctx, peer})
		return nil
	} else if pieceResult.Success {
		s.worker.send(peerDownloadPieceSuccessEvent{
			ctx:  ctx,
			peer: peer,
			pr:   pieceResult,
		})
		return nil
	} else if pieceResult.Code != dfcodes.Success {
		s.worker.send(peerDownloadPieceFailEvent{
			ctx:  ctx,
			peer: peer,
			pr:   pieceResult,
		})
		return nil
	}
	return nil
}

func (s *SchedulerService) HandlePeerResult(ctx context.Context, peer *supervisor.Peer, peerResult *schedulerRPC.PeerResult) error {
	peer.Touch()
	if peerResult.Success {
		if !s.worker.send(peerDownloadSuccessEvent{peer: peer, peerResult: peerResult}) {
			logger.Errorf("send peer download success event failed")
		}
	} else if !s.worker.send(peerDownloadFailEvent{peer: peer, peerResult: peerResult}) {
		logger.Errorf("send peer download fail event failed")
	}
	return nil
}

func (s *SchedulerService) HandleLeaveTask(ctx context.Context, peer *supervisor.Peer) error {
	peer.Touch()
	if !s.worker.send(peerLeaveEvent{
		ctx:  ctx,
		peer: peer,
	}) {
		logger.Errorf("send peer leave event failed")
	}
	return nil
}
