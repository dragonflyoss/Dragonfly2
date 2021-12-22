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

	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"k8s.io/client-go/util/workqueue"

	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	pkgsync "d7y.io/dragonfly/v2/pkg/sync"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/metrics"
	"d7y.io/dragonfly/v2/scheduler/supervisor"
)

const maxRescheduleTimes = 8

type SchedulerService struct {
	// cdn manager
	cdn supervisor.CDN
	// task manager
	taskManager supervisor.TaskManager
	// host manager
	hostManager supervisor.HostManager
	// Peer manager
	peerManager supervisor.PeerManager

	sched  Scheduler
	worker worker
	done   chan struct{}
	wg     sync.WaitGroup
	kmu    *pkgsync.Krwmutex

	config    *config.Config
	dynconfig config.DynconfigInterface
}

func NewSchedulerService(cfg *config.Config, pluginDir string, dynConfig config.DynconfigInterface, gc gc.GC, openTel bool) (*SchedulerService, error) {
	// Initialize host manager
	hostManager := supervisor.NewHostManager()

	// Initialize peer manager
	peerManager := supervisor.NewPeerManager(hostManager)

	// Initialize task manager
	taskManager, err := supervisor.NewTaskManager(cfg.Scheduler.GC, gc, peerManager)
	if err != nil {
		return nil, err
	}

	// Initialize scheduler
	sched := newScheduler(cfg.Scheduler, peerManager, pluginDir)

	// Initialize work event loop
	work := newEventLoopGroup(cfg.Scheduler.WorkerNum)

	// Initialize cdn
	var options []grpc.DialOption
	if openTel {
		options = []grpc.DialOption{
			grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
			grpc.WithChainStreamInterceptor(otelgrpc.StreamClientInterceptor()),
		}
	}
	client, err := supervisor.NewCDNDynmaicClient(dynConfig, options)
	if err != nil {
		return nil, errors.Wrap(err, "new refreshable cdn client")
	}
	cdn := supervisor.NewCDN(client, peerManager, hostManager)

	return &SchedulerService{
		cdn:         cdn,
		taskManager: taskManager,
		hostManager: hostManager,
		peerManager: peerManager,
		worker:      work,
		sched:       sched,
		config:      cfg,
		dynconfig:   dynConfig,
		done:        make(chan struct{}),
		wg:          sync.WaitGroup{},
		kmu:         pkgsync.NewKrwmutex(),
	}, nil
}

func (s *SchedulerService) Serve() {
	s.wg.Add(2)
	wsdq := workqueue.NewNamedDelayingQueue("wait reSchedule parent")
	go s.runWorkerLoop(wsdq)
	go s.runReScheduleParentLoop(wsdq)
	logger.Debugf("start scheduler service successfully")
}

func (s *SchedulerService) runWorkerLoop(wsdq workqueue.DelayingInterface) {
	defer s.wg.Done()
	s.worker.start(newState(s.sched, s.peerManager, s.cdn, wsdq))
}

func (s *SchedulerService) runReScheduleParentLoop(wsdq workqueue.DelayingInterface) {
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
				if !peer.Task.ContainsBackToSourcePeer(peer.ID) {
					if peer.CloseChannelWithError(dferrors.Newf(base.Code_SchedNeedBackSource, "reschedule parent for peer %s already reaches max reschedule times",
						peer.ID)) == nil {
						peer.Task.AddBackToSourcePeer(peer.ID)
					}
					continue
				}
			}

			if peer.Task.ContainsBackToSourcePeer(peer.ID) {
				logger.WithTaskAndPeerID(peer.Task.ID, peer.ID).Debugf("runReScheduleLoop: peer is back source client, no need to reschedule it")
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

func (s *SchedulerService) Stop() {
	close(s.done)
	if s.worker != nil {
		s.worker.stop()
	}
	s.wg.Wait()
}

func (s *SchedulerService) ScheduleParent(peer *supervisor.Peer) (*supervisor.Peer, []*supervisor.Peer, bool) {
	return s.sched.ScheduleParent(peer, set.NewSafeSet())
}

func (s *SchedulerService) GetOrAddTask(ctx context.Context, req *rpcscheduler.PeerTaskRequest, backSourceCount int32) *supervisor.Task {
	span := trace.SpanFromContext(ctx)
	task := supervisor.NewTask(idgen.TaskID(req.Url, req.UrlMeta), req.Url, backSourceCount, req.UrlMeta)

	s.kmu.RLock(task.ID)
	task, ok := s.taskManager.GetOrAdd(task)
	span.SetAttributes(config.AttributeTaskStatus.String(task.GetStatus().String()))
	span.SetAttributes(config.AttributeLastTriggerTime.String(task.LastTriggerAt.Load().String()))
	if ok && task.IsHealth() {
		// task is healthy and can be reused
		task.LastAccessAt.Store(time.Now())
		task.Log().Infof("reuse task and status is %s", task.GetStatus())
		span.SetAttributes(config.AttributeNeedSeedCDN.Bool(false))
		s.kmu.RUnlock(task.ID)
		return task
	}
	s.kmu.RUnlock(task.ID)

	s.kmu.Lock(task.ID)
	defer s.kmu.Unlock(task.ID)

	// trigger task
	task.SetStatus(supervisor.TaskStatusRunning)
	task.LastAccessAt.Store(time.Now())

	// start seed cdn task
	go func() {
		span.SetAttributes(config.AttributeNeedSeedCDN.Bool(true))
		task.Log().Info("cdn start seed task")
		peer, result, err := s.cdn.TriggerTask(ctx, task)
		if err != nil {
			// cdn seed task failed
			span.AddEvent(config.EventCDNFailBackClientSource, trace.WithAttributes(config.AttributeTriggerCDNError.String(err.Error())))
			task.Log().Errorf("cdn seed task failed: %v", err)
			if ok = s.worker.send(taskSeedFailEvent{task}); !ok {
				task.Log().Error("send taskSeedFailEvent failed, eventLoop is shutdown")
			}
			return
		}

		if ok = s.worker.send(peerDownloadSuccessEvent{peer, &rpcscheduler.PeerResult{
			TotalPieceCount: result.TotalPieceCount,
			ContentLength:   result.ContentLength,
		}}); !ok {
			peer.Log().Error("send peerDownloadSuccessEvent failed, eventLoop is shutdown")
		}

		peer.Log().Infof("successfully obtain seeds from cdn, task: %#v", task)
	}()

	return task
}

func (s *SchedulerService) GetOrAddHost(ctx context.Context, req *rpcscheduler.PeerTaskRequest) *supervisor.Host {
	rawHost := req.PeerHost
	host, ok := s.hostManager.Get(rawHost.Uuid)
	if !ok {
		var options []supervisor.HostOption
		if clientConfig, ok := s.dynconfig.GetSchedulerClusterClientConfig(); ok {
			options = append(options, supervisor.WithTotalUploadLoad(clientConfig.LoadLimit))
		}

		s.hostManager.Add(supervisor.NewHost(rawHost, options...))
		host.Log().Info("create host")
	}

	host.Log().Info("host already exists")
	return host
}

func (s *SchedulerService) GetPeer(id string) (*supervisor.Peer, bool) {
	return s.peerManager.Get(id)
}

func (s *SchedulerService) GetOrAddPeer(ctx context.Context, req *rpcscheduler.PeerTaskRequest, task *supervisor.Task, host *supervisor.Host) *supervisor.Peer {
	peer, ok := s.peerManager.Get(req.PeerId)
	if !ok {
		peer = supervisor.NewPeer(req.PeerId, task, host)
		s.peerManager.Add(peer)
		peer.Log().Info("create peer")
		return peer
	}

	peer.Log().Info("peer already exists")
	return peer
}

func (s *SchedulerService) HandlePieceResult(ctx context.Context, peer *supervisor.Peer, pieceResult *rpcscheduler.PieceResult) error {
	peer.LastAccessAt.Store(time.Now())
	if pieceResult.Success && s.config.Metrics != nil && s.config.Metrics.EnablePeerHost {
		// TODO parse PieceStyle
		metrics.PeerHostTraffic.WithLabelValues("download", peer.Host.UUID, peer.Host.IP).Add(float64(pieceResult.PieceInfo.RangeSize))
		if p, ok := s.peerManager.Get(pieceResult.DstPid); ok {
			metrics.PeerHostTraffic.WithLabelValues("upload", p.Host.UUID, p.Host.IP).Add(float64(pieceResult.PieceInfo.RangeSize))
		} else {
			logger.Warnf("dst peer %s not found for pieceResult %#v, pieceInfo %#v", pieceResult.DstPid, pieceResult, pieceResult.PieceInfo)
		}
	}
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
	} else if pieceResult.Code != base.Code_Success {
		s.worker.send(peerDownloadPieceFailEvent{
			ctx:  ctx,
			peer: peer,
			pr:   pieceResult,
		})
		return nil
	}
	return nil
}

func (s *SchedulerService) HandlePeerResult(ctx context.Context, peer *supervisor.Peer, peerResult *rpcscheduler.PeerResult) error {
	peer.LastAccessAt.Store(time.Now())
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
	peer.LastAccessAt.Store(time.Now())
	if !s.worker.send(peerLeaveEvent{
		ctx:  ctx,
		peer: peer,
	}) {
		logger.Errorf("send peer leave event failed")
	}
	return nil
}
