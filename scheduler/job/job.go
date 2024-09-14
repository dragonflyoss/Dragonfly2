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

//go:generate mockgen -destination mocks/job_mock.go -source job.go -package mocks

package job

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/RichardKnop/machinery/v1"
	"github.com/go-playground/validator/v10"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	cdnsystemv1 "d7y.io/api/v2/pkg/apis/cdnsystem/v1"
	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	commonv2 "d7y.io/api/v2/pkg/apis/common/v2"
	dfdaemonv2 "d7y.io/api/v2/pkg/apis/dfdaemon/v2"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	internaljob "d7y.io/dragonfly/v2/internal/job"
	managertypes "d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/idgen"
	dfdaemonclient "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

// Job is an interface for job.
type Job interface {
	Serve()
}

// job is an implementation of Job.
type job struct {
	globalJob    *internaljob.Job
	schedulerJob *internaljob.Job
	localJob     *internaljob.Job
	resource     resource.Resource
	config       *config.Config
}

// New creates a new Job.
func New(cfg *config.Config, resource resource.Resource) (Job, error) {
	redisConfig := &internaljob.Config{
		Addrs:            cfg.Database.Redis.Addrs,
		MasterName:       cfg.Database.Redis.MasterName,
		Username:         cfg.Database.Redis.Username,
		Password:         cfg.Database.Redis.Password,
		SentinelUsername: cfg.Database.Redis.SentinelUsername,
		SentinelPassword: cfg.Database.Redis.SentinelPassword,
		BrokerDB:         cfg.Database.Redis.BrokerDB,
		BackendDB:        cfg.Database.Redis.BackendDB,
	}

	globalJob, err := internaljob.New(redisConfig, internaljob.GlobalQueue)
	if err != nil {
		logger.Errorf("create global job queue error: %s", err.Error())
		return nil, err
	}
	logger.Infof("create global job queue: %v", globalJob)

	schedulerJob, err := internaljob.New(redisConfig, internaljob.SchedulersQueue)
	if err != nil {
		logger.Errorf("create scheduler job queue error: %s", err.Error())
		return nil, err
	}
	logger.Infof("create scheduler job queue: %v", schedulerJob)

	localQueue, err := internaljob.GetSchedulerQueue(cfg.Manager.SchedulerClusterID, cfg.Server.Host)
	if err != nil {
		logger.Errorf("get local job queue name error: %s", err.Error())
		return nil, err
	}

	localJob, err := internaljob.New(redisConfig, localQueue)
	if err != nil {
		logger.Errorf("create local job queue error: %s", err.Error())
		return nil, err
	}
	logger.Infof("create local job queue: %v", localQueue)

	t := &job{
		globalJob:    globalJob,
		schedulerJob: schedulerJob,
		localJob:     localJob,
		resource:     resource,
		config:       cfg,
	}

	namedJobFuncs := map[string]any{
		internaljob.PreheatJob:    t.preheat,
		internaljob.SyncPeersJob:  t.syncPeers,
		internaljob.GetTaskJob:    t.getTask,
		internaljob.DeleteTaskJob: t.deleteTask,
	}

	if err := localJob.RegisterJob(namedJobFuncs); err != nil {
		logger.Errorf("register preheat job to local queue error: %s", err.Error())
		return nil, err
	}

	return t, nil
}

// Serve starts the job.
func (j *job) Serve() {
	go func() {
		logger.Infof("ready to launch %d worker(s) on global queue", j.config.Job.GlobalWorkerNum)
		if err := j.globalJob.LaunchWorker("global_worker", int(j.config.Job.GlobalWorkerNum)); err != nil {
			if !errors.Is(err, machinery.ErrWorkerQuitGracefully) {
				logger.Fatalf("global queue worker error: %s", err.Error())
			}
		}
	}()

	go func() {
		logger.Infof("ready to launch %d worker(s) on scheduler queue", j.config.Job.SchedulerWorkerNum)
		if err := j.schedulerJob.LaunchWorker("scheduler_worker", int(j.config.Job.SchedulerWorkerNum)); err != nil {
			if !errors.Is(err, machinery.ErrWorkerQuitGracefully) {
				logger.Fatalf("scheduler queue worker error: %s", err.Error())
			}
		}
	}()

	go func() {
		logger.Infof("ready to launch %d worker(s) on local queue", j.config.Job.LocalWorkerNum)
		if err := j.localJob.LaunchWorker("local_worker", int(j.config.Job.LocalWorkerNum)); err != nil {
			if !errors.Is(err, machinery.ErrWorkerQuitGracefully) {
				logger.Fatalf("scheduler queue worker error: %s", err.Error())
			}
		}
	}()
}

// preheat is a job to preheat, it is not supported to preheat
// with range requests.
func (j *job) preheat(ctx context.Context, data string) (string, error) {
	req := &internaljob.PreheatRequest{}
	if err := internaljob.UnmarshalRequest(data, req); err != nil {
		logger.Errorf("unmarshal request err: %s, request body: %s", err.Error(), data)
		return "", err
	}

	if err := validator.New().Struct(req); err != nil {
		logger.Errorf("preheat %s validate failed: %s", req.URL, err.Error())
		return "", err
	}

	taskID := idgen.TaskIDV2(req.URL, req.Digest, req.Tag, req.Application, strings.Split(req.FilteredQueryParams, idgen.FilteredQueryParamsSeparator))
	log := logger.WithTask(taskID, req.URL)
	log.Infof("preheat %s request: %#v", req.URL, req)

	ctx, cancel := context.WithTimeout(ctx, req.Timeout)
	defer cancel()

	switch req.Scope {
	case managertypes.SinglePeerScope:
		log.Info("preheat single peer")
		resp, err := j.preheatSinglePeer(ctx, taskID, req, log)
		if err != nil {
			return "", err
		}

		resp.SchedulerClusterID = j.config.Manager.SchedulerClusterID
		return internaljob.MarshalResponse(resp)
	case managertypes.AllPeersScope:
		log.Info("preheat all peers")
		resp, err := j.preheatAllPeers(ctx, taskID, req, log)
		if err != nil {
			return "", err
		}

		resp.SchedulerClusterID = j.config.Manager.SchedulerClusterID
		return internaljob.MarshalResponse(resp)
	default:
		log.Warnf("scope is invalid %s, preheat single peer", req.Scope)
		resp, err := j.preheatSinglePeer(ctx, taskID, req, log)
		if err != nil {
			return "", err
		}

		resp.SchedulerClusterID = j.config.Manager.SchedulerClusterID
		return internaljob.MarshalResponse(resp)
	}
}

// preheatSinglePeer preheats job by single seed peer, scheduler will trigger seed peer to download task.
func (j *job) preheatSinglePeer(ctx context.Context, taskID string, req *internaljob.PreheatRequest, log *logger.SugaredLoggerOnWith) (*internaljob.PreheatResponse, error) {
	// If seed peer is disabled, return error.
	if !j.config.SeedPeer.Enable {
		return nil, fmt.Errorf("cluster %d scheduler %s has disabled seed peer", j.config.Manager.SchedulerClusterID, j.config.Server.AdvertiseIP)
	}

	// If scheduler has no available seed peer, return error.
	if len(j.resource.SeedPeer().Client().Addrs()) == 0 {
		return nil, fmt.Errorf("cluster %d scheduler %s has no available seed peer", j.config.Manager.SchedulerClusterID, j.config.Server.AdvertiseIP)
	}

	// Preheat by v2 grpc protocol. If seed peer does not support
	// v2 protocol, preheat by v1 grpc protocol.
	resp, err := j.preheatV2(ctx, taskID, req, log)
	if err != nil {
		log.Errorf("preheat failed: %s", err.Error())
		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.Unimplemented {
				return j.preheatV1(ctx, taskID, req, log)
			}
		}

		return nil, err
	}

	return resp, nil
}

// preheatAllPeers preheats job by all peers, only suoported by v2 protocol. Scheduler will trigger all peers to download task.
// If all the peer download task failed, return error. If some of the peer download task failed, return success tasks and failure tasks.
// Notify the client that the preheat is successful.
func (j *job) preheatAllPeers(ctx context.Context, taskID string, req *internaljob.PreheatRequest, log *logger.SugaredLoggerOnWith) (*internaljob.PreheatResponse, error) {
	var (
		successTasks = sync.Map{}
		failureTasks = sync.Map{}
	)

	eg, _ := errgroup.WithContext(ctx)
	eg.SetLimit(int(req.ConcurrentCount))
	for _, host := range j.resource.HostManager().LoadAll() {
		var (
			hostname = host.Hostname
			ip       = host.IP
			port     = host.Port
		)

		target := fmt.Sprintf("%s:%d", ip, port)
		log := logger.WithHost(host.ID, hostname, ip)

		eg.Go(func() error {
			log.Info("preheat started")
			dialOptions := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
			dfdaemonClient, err := dfdaemonclient.GetV2ByAddr(ctx, target, dialOptions...)
			if err != nil {
				log.Errorf("preheat failed: %s", err.Error())
				failureTasks.Store(ip, &internaljob.PreheatFailureTask{
					URL:         req.URL,
					Hostname:    hostname,
					IP:          ip,
					Description: fmt.Sprintf("task %s failed: %s", taskID, err.Error()),
				})

				return err
			}

			stream, err := dfdaemonClient.DownloadTask(
				ctx,
				taskID,
				&dfdaemonv2.DownloadTaskRequest{Download: &commonv2.Download{
					Url:                 req.URL,
					Digest:              &req.Digest,
					Type:                commonv2.TaskType_DFDAEMON,
					Tag:                 &req.Tag,
					Application:         &req.Application,
					Priority:            commonv2.Priority(req.Priority),
					FilteredQueryParams: strings.Split(req.FilteredQueryParams, idgen.FilteredQueryParamsSeparator),
					RequestHeader:       req.Headers,
					Timeout:             durationpb.New(req.Timeout),
				}})
			if err != nil {
				log.Errorf("preheat failed: %s", err.Error())
				failureTasks.Store(ip, &internaljob.PreheatFailureTask{
					URL:         req.URL,
					Hostname:    hostname,
					IP:          ip,
					Description: fmt.Sprintf("task %s failed: %s", taskID, err.Error()),
				})

				return err
			}

			// Wait for the download task to complete.
			for {
				_, err := stream.Recv()
				if err != nil {
					if err == io.EOF {
						log.Info("preheat succeeded")
						successTasks.Store(ip, &internaljob.PreheatSuccessTask{
							URL:      req.URL,
							Hostname: hostname,
							IP:       ip,
						})

						return nil
					}

					log.Errorf("preheat failed: %s", err.Error())
					failureTasks.Store(ip, &internaljob.PreheatFailureTask{
						URL:         req.URL,
						Hostname:    hostname,
						IP:          ip,
						Description: fmt.Sprintf("task %s failed: %s", taskID, err.Error()),
					})

					return err
				}
			}
		})
	}

	// Wait for all tasks to complete and print the errors.
	if err := eg.Wait(); err != nil {
		log.Errorf("preheat failed: %s", err.Error())
	}

	// If successTasks is not empty, return success tasks and failure tasks.
	// Notify the client that the preheat is successful.
	var preheatResponse internaljob.PreheatResponse
	failureTasks.Range(func(_, value any) bool {
		if failureTask, ok := value.(*internaljob.PreheatFailureTask); ok {
			preheatResponse.FailureTasks = append(preheatResponse.FailureTasks, failureTask)
		}

		return true
	})

	successTasks.Range(func(_, value any) bool {
		if successTask, ok := value.(*internaljob.PreheatSuccessTask); ok {
			for _, failureTask := range preheatResponse.FailureTasks {
				if failureTask.IP == successTask.IP {
					return true
				}
			}

			preheatResponse.SuccessTasks = append(preheatResponse.SuccessTasks, successTask)
		}

		return true
	})

	if len(preheatResponse.SuccessTasks) > 0 {
		return &preheatResponse, nil
	}

	msg := "no error message"
	if len(preheatResponse.FailureTasks) > 0 {
		msg = fmt.Sprintf("%s %s %s %s", taskID, preheatResponse.FailureTasks[0].IP, preheatResponse.FailureTasks[0].Hostname,
			preheatResponse.FailureTasks[0].Description)
	}

	return nil, fmt.Errorf("all peers preheat failed: %s", msg)
}

// preheatV1 preheats job by v1 grpc protocol.
func (j *job) preheatV1(ctx context.Context, taskID string, req *internaljob.PreheatRequest, log *logger.SugaredLoggerOnWith) (*internaljob.PreheatResponse, error) {
	urlMeta := &commonv1.UrlMeta{
		Digest:      req.Digest,
		Tag:         req.Tag,
		Filter:      req.FilteredQueryParams,
		Header:      req.Headers,
		Application: req.Application,
		Priority:    commonv1.Priority(req.Priority),
	}

	// Trigger seed peer download seeds.
	stream, err := j.resource.SeedPeer().Client().ObtainSeeds(ctx, &cdnsystemv1.SeedRequest{
		TaskId:  taskID,
		Url:     req.URL,
		UrlMeta: urlMeta,
	})
	if err != nil {
		log.Errorf("preheat failed: %s", err.Error())
		return nil, err
	}

	for {
		piece, err := stream.Recv()
		if err != nil {
			log.Errorf("recive piece failed: %s", err.Error())
			return nil, err
		}

		if piece.Done == true {
			log.Info("preheat succeeded")
			if host, ok := j.resource.HostManager().Load(piece.HostId); ok {
				return &internaljob.PreheatResponse{
					SuccessTasks: []*internaljob.PreheatSuccessTask{{URL: req.URL, Hostname: host.Hostname, IP: host.IP}},
				}, nil
			}

			log.Warnf("host %s not found", piece.HostId)
			return &internaljob.PreheatResponse{
				SuccessTasks: []*internaljob.PreheatSuccessTask{{URL: req.URL, Hostname: "unknow", IP: "unknow"}},
			}, nil
		}
	}
}

// preheatV2 preheats job by v2 grpc protocol.
func (j *job) preheatV2(ctx context.Context, taskID string, req *internaljob.PreheatRequest, log *logger.SugaredLoggerOnWith) (*internaljob.PreheatResponse, error) {
	filteredQueryParams := strings.Split(req.FilteredQueryParams, idgen.FilteredQueryParamsSeparator)
	stream, err := j.resource.SeedPeer().Client().DownloadTask(ctx, taskID, &dfdaemonv2.DownloadTaskRequest{
		Download: &commonv2.Download{
			Url:                 req.URL,
			Digest:              &req.Digest,
			Type:                commonv2.TaskType_DFDAEMON,
			Tag:                 &req.Tag,
			Application:         &req.Application,
			Priority:            commonv2.Priority(req.Priority),
			FilteredQueryParams: filteredQueryParams,
			RequestHeader:       req.Headers,
		}})
	if err != nil {
		log.Errorf("preheat failed: %s", err.Error())
		return nil, err
	}

	// Wait for the download task to complete.
	var hostID string
	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				log.Info("preheat succeeded")
				if host, ok := j.resource.HostManager().Load(hostID); ok {
					return &internaljob.PreheatResponse{
						SuccessTasks: []*internaljob.PreheatSuccessTask{{URL: req.URL, Hostname: host.Hostname, IP: host.IP}},
					}, nil
				}

				log.Warnf("host %s not found", hostID)
				return &internaljob.PreheatResponse{
					SuccessTasks: []*internaljob.PreheatSuccessTask{{URL: req.URL, Hostname: "unknow", IP: "unknow"}},
				}, nil
			}

			log.Errorf("recive piece failed: %s", err.Error())
			return nil, err
		}

		hostID = resp.HostId
	}
}

// syncPeers is a job to sync peers.
func (j *job) syncPeers() (string, error) {
	var hosts []*resource.Host
	j.resource.HostManager().Range(func(key, value any) bool {
		host, ok := value.(*resource.Host)
		if !ok {
			logger.Errorf("invalid host %v %v", key, value)
			return true
		}

		hosts = append(hosts, host)
		return true
	})

	return internaljob.MarshalResponse(hosts)
}

// getTask is a job to get task.
func (j *job) getTask(ctx context.Context, data string) (string, error) {
	req := &internaljob.GetTaskRequest{}
	if err := internaljob.UnmarshalRequest(data, req); err != nil {
		logger.Errorf("unmarshal request err: %s, request body: %s", err.Error(), data)
		return "", err
	}

	if err := validator.New().Struct(req); err != nil {
		logger.Errorf("getTask %s validate failed: %s", req.TaskID, err.Error())
		return "", err
	}

	task, ok := j.resource.TaskManager().Load(req.TaskID)
	if !ok {
		// Do not return error if task not found, just retunr empty response.
		logger.Warnf("task %s not found", req.TaskID)
		return internaljob.MarshalResponse(&internaljob.GetTaskResponse{})
	}

	// Convert peer struct to peer response.
	var peers []*internaljob.Peer
	for _, peer := range task.LoadPeers() {
		peers = append(peers, convertPeer(peer))
	}

	return internaljob.MarshalResponse(&internaljob.GetTaskResponse{
		Peers: peers,
	})
}

// deleteTask is a job to delete task.
func (j *job) deleteTask(ctx context.Context, data string) (string, error) {
	req := &internaljob.DeleteTaskRequest{}
	if err := internaljob.UnmarshalRequest(data, req); err != nil {
		logger.Errorf("unmarshal request err: %s, request body: %s", err.Error(), data)
		return "", err
	}

	if err := validator.New().Struct(req); err != nil {
		logger.Errorf("deleteTask %s validate failed: %s", req.TaskID, err.Error())
		return "", err
	}

	ctx, cancel := context.WithTimeout(ctx, req.Timeout)
	defer cancel()

	task, ok := j.resource.TaskManager().Load(req.TaskID)
	if !ok {
		// Do not return error if task not found, just retunr empty response.
		logger.Warnf("task %s not found", req.TaskID)
		return internaljob.MarshalResponse(&internaljob.DeleteTaskResponse{})
	}

	successPeers := []*internaljob.DeletePeerResponse{}
	failurePeers := []*internaljob.DeletePeerResponse{}

	finishedPeers := task.LoadFinishedPeers()
	for _, finishedPeer := range finishedPeers {
		log := logger.WithPeer(finishedPeer.Host.ID, finishedPeer.Task.ID, finishedPeer.ID)

		addr := fmt.Sprintf("%s:%d", finishedPeer.Host.IP, finishedPeer.Host.Port)
		dfdaemonClient, err := dfdaemonclient.GetV2ByAddr(ctx, addr)
		if err != nil {
			log.Errorf("get client from %s failed: %s", addr, err.Error())
			failurePeers = append(failurePeers, &internaljob.DeletePeerResponse{
				Peer:        convertPeer(finishedPeer),
				Description: err.Error(),
			})

			continue
		}

		if err = dfdaemonClient.DeleteTask(ctx, &dfdaemonv2.DeleteTaskRequest{
			TaskId: req.TaskID,
		}); err != nil {
			logger.Errorf("delete task failed: %s", err.Error())
			failurePeers = append(failurePeers, &internaljob.DeletePeerResponse{
				Peer:        convertPeer(finishedPeer),
				Description: err.Error(),
			})

			continue
		}

		successPeers = append(successPeers, &internaljob.DeletePeerResponse{
			Peer:        convertPeer(finishedPeer),
			Description: "",
		})
	}

	return internaljob.MarshalResponse(&internaljob.DeleteTaskResponse{
		FailurePeers: failurePeers,
		SuccessPeers: successPeers,
	})
}

func convertPeer(p *resource.Peer) *internaljob.Peer {
	peer := &internaljob.Peer{
		ID:               p.ID,
		Config:           p.Config,
		Range:            p.Range,
		Priority:         int32(p.Priority),
		FinishedPieces:   p.FinishedPieces,
		PieceCosts:       p.PieceCosts(),
		NeedBackToSource: p.NeedBackToSource != nil && p.NeedBackToSource.Load(),
	}

	if p.BlockParents != nil && p.BlockParents.Len() > 0 {
		peer.BlockParents = p.BlockParents.Values()
	}

	if p.Cost != nil {
		peer.Cost = p.Cost.Load()
	}

	if p.PieceUpdatedAt != nil {
		peer.PieceUpdatedAt = p.PieceUpdatedAt.Load()
	}

	if p.CreatedAt != nil {
		peer.CreatedAt = p.CreatedAt.Load()
	}

	if p.UpdatedAt != nil {
		peer.UpdatedAt = p.UpdatedAt.Load()
	}

	// Handle Pieces (sync.Map) safely
	peer.Pieces = make(map[int32]*resource.Piece)
	p.Pieces.Range(func(key, value interface{}) bool {
		k, ok1 := key.(int32)
		v, ok2 := value.(*resource.Piece)
		if ok1 && ok2 {
			peer.Pieces[k] = v
		}
		return true
	})

	return peer
}
