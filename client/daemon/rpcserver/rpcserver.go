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

//go:generate mockgen -destination mocks/rpcserver_mock.go -source rpcserver.go -package mocks

package rpcserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gammazero/deque"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	grpcpeer "google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	cdnsystemv1 "d7y.io/api/v2/pkg/apis/cdnsystem/v1"
	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	dfdaemonv1 "d7y.io/api/v2/pkg/apis/dfdaemon/v1"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/client/util"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/net/http"
	"d7y.io/dragonfly/v2/pkg/os/user"
	dfdaemonserver "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/server"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
	"d7y.io/dragonfly/v2/pkg/safe"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

type Server interface {
	util.KeepAlive
	ServeDownload(listener net.Listener) error
	ServePeer(listener net.Listener) error
	OnNotify(*config.DynconfigData)
	Stop()
}

type server struct {
	util.KeepAlive
	peerHost        *schedulerv1.PeerHost
	peerTaskManager peer.TaskManager
	storageManager  storage.Manager
	schedulerClient schedulerclient.V1

	healthServer   *health.Server
	downloadServer *grpc.Server
	peerServer     *grpc.Server
	uploadAddr     string

	recursiveConcurrent    int
	cacheRecursiveMetadata time.Duration
}

var tracer trace.Tracer

func init() {
	tracer = otel.Tracer("dfget-rpcserver")
}

func New(peerHost *schedulerv1.PeerHost, peerTaskManager peer.TaskManager,
	storageManager storage.Manager, schedulerClient schedulerclient.V1, recursiveConcurrent int, cacheRecursiveMetadata time.Duration,
	downloadOpts []grpc.ServerOption, peerOpts []grpc.ServerOption) (Server, error) {
	s := &server{
		KeepAlive:       util.NewKeepAlive("rpc server"),
		peerHost:        peerHost,
		peerTaskManager: peerTaskManager,
		storageManager:  storageManager,
		schedulerClient: schedulerClient,

		recursiveConcurrent:    recursiveConcurrent,
		cacheRecursiveMetadata: cacheRecursiveMetadata,

		healthServer: health.NewServer(),
	}

	sd := &seeder{
		server: s,
	}

	// set not serving by default
	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)

	s.downloadServer = dfdaemonserver.New(s, s.healthServer, downloadOpts...)
	s.peerServer = dfdaemonserver.New(s, s.healthServer, peerOpts...)
	cdnsystemv1.RegisterSeederServer(s.peerServer, sd)
	return s, nil
}

func (s *server) ServeDownload(listener net.Listener) error {
	return s.downloadServer.Serve(listener)
}

func (s *server) ServePeer(listener net.Listener) error {
	s.uploadAddr = fmt.Sprintf("%s:%d", s.peerHost.Ip, s.peerHost.DownPort)
	return s.peerServer.Serve(listener)
}

func (s *server) OnNotify(data *config.DynconfigData) {
	if len(data.Schedulers) > 0 {
		s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	} else {
		s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	}
}

func (s *server) Stop() {
	s.peerServer.GracefulStop()
	s.downloadServer.GracefulStop()
}

func (s *server) GetPieceTasks(ctx context.Context, request *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error) {
	s.Keep()
	p, err := s.storageManager.GetPieces(ctx, request)
	if err != nil {
		code := commonv1.Code_UnknownError
		if err == dferrors.ErrInvalidArgument {
			code = commonv1.Code_BadRequest
		}
		if err != storage.ErrTaskNotFound {
			logger.Errorf("get piece tasks error: %s, task id: %s, src peer: %s, dst peer: %s, piece num: %d, limit: %d",
				err, request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit)
			return nil, dferrors.New(code, err.Error())
		}

		// check whether dst peer is not running
		task, ok := s.peerTaskManager.IsPeerTaskRunning(request.TaskId, request.DstPid)
		if !ok {
			// 1. no running task
			code = commonv1.Code_PeerTaskNotFound
			logger.Errorf("get piece tasks error: target peer task not found, task id: %s, src peer: %s, dst peer: %s, piece num: %d, limit: %d",
				request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit)
			return nil, dferrors.New(code, err.Error())
		} else if task.GetPeerID() == request.GetSrcPid() {
			// 2. scheduler schedules same peer host, and the running peer is the source peer (schedulers misses the old peer leaved event)
			code = commonv1.Code_PeerTaskNotFound
			logger.Errorf("get piece tasks error: target peer task not found, task id: %s, the src peer is same with running peer,"+
				" src peer: %s, dst peer: %s, piece num: %d, limit: %d",
				request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit)
			return nil, dferrors.New(code, err.Error())
		}

		if task.GetPeerID() != request.GetDstPid() {
			// there is only one running task in same time, redirect request to running peer task
			r := commonv1.PieceTaskRequest{
				TaskId:   request.TaskId,
				SrcPid:   request.SrcPid,
				DstPid:   task.GetPeerID(), // replace to running task peer id
				StartNum: request.StartNum,
				Limit:    request.Limit,
			}
			p, err = s.storageManager.GetPieces(ctx, &r)
			if err == nil {
				logger.Debugf("receive get piece tasks request, task id: %s, src peer: %s, dst peer: %s, replaced dst peer: %s, piece num: %d, limit: %d, length: %d",
					request.TaskId, request.SrcPid, request.DstPid, r.DstPid, request.StartNum, request.Limit, len(p.PieceInfos))
				p.DstAddr = s.uploadAddr
				return p, nil
			}
			code = commonv1.Code_PeerTaskNotFound
			logger.Errorf("get piece tasks error: target peer task and replaced peer task storage not found wit error: %s, task id: %s, src peer: %s, dst peer: %s, piece num: %d, limit: %d",
				err, request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit)
			return nil, dferrors.New(code, err.Error())
		}

		logger.Infof("try to get piece tasks, "+
			"but target peer task is initializing, "+
			"there is no available pieces, "+
			"task id: %s, src peer: %s, dst peer: %s, piece num: %d, limit: %d",
			request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit)
		// dst peer is running, send empty result, src peer will retry later
		return &commonv1.PiecePacket{
			TaskId:        request.TaskId,
			DstPid:        request.DstPid,
			DstAddr:       s.uploadAddr,
			PieceInfos:    nil,
			TotalPiece:    -1,
			ContentLength: -1,
			PieceMd5Sign:  "",
		}, nil
	}

	logger.Debugf("receive get piece tasks request, task id: %s, src peer: %s, dst peer: %s, piece start num: %d, limit: %d, count: %d, total content length: %d",
		request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit, len(p.PieceInfos), p.ContentLength)
	p.DstAddr = s.uploadAddr
	return p, nil
}

// sendExistPieces will send as much as possible pieces
func (s *server) sendExistPieces(
	log *logger.SugaredLoggerOnWith,
	request *commonv1.PieceTaskRequest,
	sync dfdaemonv1.Daemon_SyncPieceTasksServer,
	get func(ctx context.Context, request *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error),
	sentMap map[int32]struct{}) (total int32, err error) {
	return sendExistPieces(sync.Context(), log, get, request, sync, sentMap, true)
}

// sendFirstPieceTasks will send as much as possible pieces, even if no available pieces
func (s *server) sendFirstPieceTasks(
	log *logger.SugaredLoggerOnWith,
	request *commonv1.PieceTaskRequest,
	sync dfdaemonv1.Daemon_SyncPieceTasksServer,
	get func(ctx context.Context, request *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error),
	sentMap map[int32]struct{}) (total int32, err error) {
	return sendExistPieces(sync.Context(), log, get, request, sync, sentMap, false)
}

func printAuthInfo(ctx context.Context) {
	if peerInfo, ok := grpcpeer.FromContext(ctx); ok {
		if tlsInfo, ok := peerInfo.AuthInfo.(credentials.TLSInfo); ok {
			for i, pc := range tlsInfo.State.PeerCertificates {
				logger.Debugf("peer cert depth %d, issuer: %#v", i, pc.Issuer.CommonName)
				logger.Debugf("peer cert depth %d, common name: %#v", i, pc.Subject.CommonName)
				if len(pc.IPAddresses) > 0 {
					var ips []string
					for _, ip := range pc.IPAddresses {
						ips = append(ips, ip.String())
					}
					logger.Debugf("peer cert depth %d, ip: %s", i, strings.Join(ips, " "))
				}
				if len(pc.DNSNames) > 0 {
					logger.Debugf("peer cert depth %d, dns: %#v", i, pc.DNSNames)
				}
			}
		}
	}
}

func (s *server) SyncPieceTasks(sync dfdaemonv1.Daemon_SyncPieceTasksServer) error {
	if logger.IsDebug() {
		printAuthInfo(sync.Context())
	}

	request, err := sync.Recv()
	if err != nil {
		logger.Errorf("receive first sync piece tasks request error: %s", err.Error())
		return err
	}
	log := logger.With("taskID", request.TaskId,
		"localPeerID", request.DstPid, "remotePeerID", request.SrcPid)

	skipPieceCount := request.StartNum
	var (
		sentMap       = make(map[int32]struct{})
		attributeSent bool
	)

	getPieces := func(ctx context.Context, request *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error) {
		p, e := s.GetPieceTasks(ctx, request)
		if e != nil {
			return nil, e
		}
		p.DstAddr = s.uploadAddr
		if !attributeSent && len(p.PieceInfos) > 0 {
			exa, e := s.storageManager.GetExtendAttribute(ctx,
				&storage.PeerTaskMetadata{
					PeerID: request.DstPid,
					TaskID: request.TaskId,
				})
			if e != nil {
				log.Errorf("get extend attribute error: %s", e.Error())
				return nil, e
			}
			p.ExtendAttribute = exa
			attributeSent = true
		}
		return p, e
	}

	// TODO if not found, try to send to peer task conductor, then download it first
	total, err := s.sendFirstPieceTasks(log, request, sync, getPieces, sentMap)
	if err != nil {
		log.Errorf("send first piece tasks error: %s", err)
		return err
	}

	recvReminding := func() error {
		for {
			request, err = sync.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				logger.Errorf("receive reminding piece tasks request error: %s", err)
				return err
			}
			total, err = s.sendExistPieces(log, request, sync, getPieces, sentMap)
			if err != nil {
				logger.Errorf("send reminding piece tasks error: %s", err)
				return err
			}
		}
	}

	// task is done, just receive new piece tasks requests only
	if int(total) == len(sentMap)+int(skipPieceCount) {
		log.Infof("all piece tasks sent, receive new piece tasks requests only")
		return recvReminding()
	}

	// subscribe peer task message for remaining pieces
	result, ok := s.peerTaskManager.Subscribe(request)
	if !ok {
		// running task not found, double check for done task
		request.StartNum = searchNextPieceNum(sentMap, skipPieceCount)
		total, err = s.sendExistPieces(log, request, sync, getPieces, sentMap)
		if err != nil {
			log.Errorf("send exist piece tasks error: %s", err)
			return err
		}

		if int(total) > len(sentMap)+int(skipPieceCount) {
			return status.Errorf(codes.Unavailable, "peer task not finish, but no running task found")
		}
		return recvReminding()
	}

	var sub = &subscriber{
		SubscribeResponse:   result,
		sync:                sync,
		request:             request,
		skipPieceCount:      skipPieceCount,
		totalPieces:         total,
		sentMap:             sentMap,
		done:                make(chan struct{}),
		uploadAddr:          s.uploadAddr,
		SugaredLoggerOnWith: log,
		attributeSent:       atomic.NewBool(attributeSent),
	}

	go sub.receiveRemainingPieceTaskRequests()
	return sub.sendRemainingPieceTasks()
}

func (s *server) CheckHealth(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	s.Keep()
	return new(emptypb.Empty), nil
}

func (s *server) Download(req *dfdaemonv1.DownRequest, stream dfdaemonv1.Daemon_DownloadServer) error {
	s.Keep()
	ctx := stream.Context()
	pr, ok := grpcpeer.FromContext(ctx)

	if !ok {
		return status.Error(codes.FailedPrecondition, "invalid grpc peer info")
	}

	// currently, we only use daemon to download file via unix domain socket
	if pr.Addr.Network() != "unix" {
		err := fmt.Sprintf("invalid incoming source: %v", pr.Addr.String())
		logger.Errorf(err)
		return status.Error(codes.Unauthenticated, err)
	}

	if req.Recursive {
		return s.recursiveDownload(ctx, req, stream)
	}
	return s.download(ctx, req, stream, "")
}

func (s *server) recursiveDownload(ctx context.Context, req *dfdaemonv1.DownRequest, stream dfdaemonv1.Daemon_DownloadServer) error {
	ctx, span := tracer.Start(ctx, config.SpanRecursiveDownload, trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	traceID := span.SpanContext().TraceID()
	logKV := []any{
		"recursive", req.Url,
		"action", "RecursiveDownload",
	}
	if traceID.IsValid() {
		logKV = append(logKV, "trace", traceID.String())
	}

	log := logger.With(logKV...)

	stat, err := os.Stat(req.Output)
	if err == nil {
		if !stat.IsDir() {
			err = fmt.Errorf("target output %s must be directory", req.Output)
			log.Errorf(err.Error())
			return status.Errorf(codes.FailedPrecondition, err.Error())
		}
	} else if !os.IsNotExist(err) {
		log.Errorf("stat %s error: %s", req.Output, err)
		return status.Errorf(codes.FailedPrecondition, err.Error())
	}

	// only try to cache metadata when cacheRecursiveMetadata is set
	if s.cacheRecursiveMetadata > 0 {
		log.Infof("start to download with p2p metadata")
		if err = s.recursiveDownloadWithP2PMetadata(ctx, log, span, traceID, req, stream); err == nil {
			log.Infof("download with p2p metadata success")
			return nil
		}
		log.Warnf("download with p2p metadata failed, fallback to direct metadata")
	}

	return s.recursiveDownloadWithDirectMetadata(ctx, log, span, traceID, req, stream)
}

func (s *server) recursiveDownloadWithP2PMetadata(
	ctx context.Context,
	log *logger.SugaredLoggerOnWith,
	span trace.Span,
	traceID trace.TraceID,
	req *dfdaemonv1.DownRequest,
	stream dfdaemonv1.Daemon_DownloadServer) error {

	requestCh, stopCh, wg, downloadErrors := s.startDownloadWorkers(ctx, span, traceID, stream)
	defer close(stopCh)

	listMetadata := source.ListMetadata{}
	purl, err := url.Parse(req.Url)
	if err != nil {
		return err
	}
	urlMeta := commonv1.UrlMeta{
		Digest:      req.UrlMeta.Digest,
		Tag:         req.UrlMeta.Tag,
		Filter:      req.UrlMeta.Filter,
		Header:      req.UrlMeta.Header,
		Application: req.UrlMeta.Application,
	}
	// set original scheme
	urlMeta.Header[source.ListMetadataOriginScheme] = purl.Scheme
	// update url scheme
	purl.Scheme = source.ListMetadataScheme
	// update cache expire time
	// The target format is GMT time, need convert to UTC time first
	urlMeta.Header[source.ListMetadataExpire] = time.Now().Add(s.cacheRecursiveMetadata).UTC().Format(source.ExpireLayout)

	rc, _, err := s.peerTaskManager.StartStreamTask(ctx, &peer.StreamTaskRequest{
		URL:     purl.String(),
		URLMeta: &urlMeta,
		PeerID:  idgen.PeerIDV1(s.peerHost.Ip),
	})
	if err != nil {
		log.Errorf("start stream task for metadata error: %s", err)
		span.RecordError(err)
		return err
	}
	defer rc.Close()

	dec := json.NewDecoder(rc)
	err = dec.Decode(&listMetadata)
	if err != nil {
		log.Errorf("load metadata error: %s", err)
		span.RecordError(err)
		return err
	}
	log.Infof("url entries count: %d", len(listMetadata.URLEntries))

loop:
	for _, urlEntry := range listMetadata.URLEntries {
		// create new req
		childReq := copyDownRequest(req)
		// update correct output
		childReq.Output = path.Join(req.Output, strings.TrimPrefix(urlEntry.URL.Path, purl.Path))

		u := urlEntry.URL
		childReq.Url = u.String()
		log.Infof("download %s to %s", childReq.Url, childReq.Output)

		if urlEntry.IsDir {
			continue
		}

		// validate new request
		if err := childReq.Validate(); err != nil {
			log.Errorf("validate %#v failed: %s", childReq, err)
			span.RecordError(err)
			return err
		}

		if err := checkOutput(childReq.Output); err != nil {
			log.Errorf("check output %#v failed: %s", childReq, err)
			span.RecordError(err)
			return err
		}

		wg.Add(1)
		select {
		case requestCh <- childReq:
			log.Debugf("sent download request %s", childReq.Url)
		case <-ctx.Done():
			// request did not send, call Done to rollback
			wg.Done()
			log.Warnf("receive context done: %s", ctx.Err())
			break loop
		}
	}

	// wait all sent tasks done or error
	log.Info("all request sent, wait all tasks processed")
	wg.Wait()
	if len(downloadErrors()) > 0 {
		// just return first error
		return downloadErrors()[0]
	}

	log.Info("all tasks processed")
	return nil
}

func (s *server) startDownloadWorkers(
	ctx context.Context,
	span trace.Span,
	traceID trace.TraceID,
	stream dfdaemonv1.Daemon_DownloadServer) (
	requestCh chan *dfdaemonv1.DownRequest,
	stopCh chan struct{},
	wg *sync.WaitGroup,
	getDownloadErrors func() []error,
) {
	var downloadErrors []error

	requestCh = make(chan *dfdaemonv1.DownRequest)
	stopCh = make(chan struct{})
	wg = &sync.WaitGroup{}
	getDownloadErrors = func() []error {
		return downloadErrors
	}

	lock := sync.Mutex{}
	sender := &sequentialResultSender{realSender: stream}

	for i := 0; i < s.recursiveConcurrent; i++ {
		go func(i int) {
			logKV := []any{
				"recursiveDownloader", fmt.Sprintf("%d", i),
			}
			if traceID.IsValid() {
				logKV = append(logKV, "trace", traceID.String())
			}
			dLog := logger.With(logKV...)
			for {
				select {
				case req := <-requestCh:
					dLog.Debugf("downloader %d start to download %s", i, req.Url)
					if err := s.download(ctx, req, sender, ""); err != nil {
						dLog.Errorf("download %#v error: %s", req, err.Error())
						lock.Lock()
						downloadErrors = append(downloadErrors, err)
						span.RecordError(err)
						lock.Unlock()
					}
					dLog.Debugf("downloader %d completed download %s", i, req.Url)
					wg.Done()
				case <-stopCh:
					return
				}
			}
		}(i)
	}
	return requestCh, stopCh, wg, getDownloadErrors
}

func (s *server) recursiveDownloadWithDirectMetadata(
	ctx context.Context,
	log *logger.SugaredLoggerOnWith,
	span trace.Span,
	traceID trace.TraceID,
	req *dfdaemonv1.DownRequest,
	stream dfdaemonv1.Daemon_DownloadServer) error {

	var (
		queue            deque.Deque[*dfdaemonv1.DownRequest]
		start            = time.Now()
		listMilliseconds int64
	)

	requestCh, stopCh, wg, downloadErrors := s.startDownloadWorkers(ctx, span, traceID, stream)
	defer close(stopCh)

	queue.PushBack(req)
	downloadMap := map[url.URL]struct{}{}
	for {
		if queue.Len() == 0 {
			break
		}

		parentReq := queue.PopFront()
		request, err := source.NewRequestWithContext(ctx, parentReq.Url, parentReq.UrlMeta.Header)
		if err != nil {
			log.Errorf("generate url [%v] request error: %v", parentReq.Url, err)
			span.RecordError(err)
			return err
		}

		// prevent loop downloading
		if _, exist := downloadMap[*request.URL]; exist {
			continue
		}
		downloadMap[*request.URL] = struct{}{}

		listStart := time.Now()
		urlEntries, err := source.List(request)
		if err != nil {
			log.Errorf("url [%v] source lister error: %v", request.URL, err)
			span.RecordError(err)
			return err
		}
		cost := time.Now().Sub(listStart).Milliseconds()
		listMilliseconds += cost
		log.Infof("list dir %s cost: %dms", request.URL, cost)

	loop:
		for _, urlEntry := range urlEntries {
			childReq := copyDownRequest(parentReq) //create new req
			childReq.Output = path.Join(parentReq.Output, urlEntry.Name)
			log.Infof("target output: %s", strings.TrimPrefix(childReq.Output, req.Output))

			u := urlEntry.URL
			childReq.Url = u.String()
			log.Infof("download %s to %s", childReq.Url, childReq.Output)

			if urlEntry.IsDir {
				queue.PushBack(childReq)
				continue
			}

			// validate new request
			if err = childReq.Validate(); err != nil {
				log.Errorf("validate %#v failed: %s", childReq, err)
				span.RecordError(err)
				return err
			}

			if err = checkOutput(childReq.Output); err != nil {
				log.Errorf("check output %#v failed: %s", childReq, err)
				span.RecordError(err)
				return err
			}

			wg.Add(1)

			select {
			case requestCh <- childReq:
			case <-ctx.Done():
				// request did not send, call Done to rollback
				wg.Done()
				log.Warnf("receive context done: %s", ctx.Err())
				break loop
			}
		}
	}

	// wait all sent tasks done or error
	wg.Wait()
	if len(downloadErrors()) > 0 {
		// just return first error
		return downloadErrors()[0]
	}

	log.Infof("list dirs cost: %dms, download cost: %dms", listMilliseconds, time.Now().Sub(start).Milliseconds())
	return nil
}

func copyDownRequest(req *dfdaemonv1.DownRequest) *dfdaemonv1.DownRequest {
	return &dfdaemonv1.DownRequest{
		Uuid:               uuid.New().String(),
		Url:                req.Url,
		Output:             req.Output,
		Timeout:            req.Timeout,
		Limit:              req.Limit,
		DisableBackSource:  req.DisableBackSource,
		UrlMeta:            req.UrlMeta,
		Uid:                req.Uid,
		Gid:                req.Gid,
		KeepOriginalOffset: req.KeepOriginalOffset,
		Recursive:          false, // not used anymore
	}
}

type ResultSender interface {
	Send(*dfdaemonv1.DownResult) error
}

type sequentialResultSender struct {
	realSender ResultSender
	sync.Mutex
}

func (s *sequentialResultSender) Send(result *dfdaemonv1.DownResult) error {
	s.Lock()
	err := s.realSender.Send(result)
	s.Unlock()
	return err
}

func (s *server) download(ctx context.Context, req *dfdaemonv1.DownRequest, stream ResultSender, peerID string) error {
	ctx, span := tracer.Start(ctx, config.SpanDownload, trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	if req.UrlMeta == nil {
		req.UrlMeta = &commonv1.UrlMeta{}
	}

	// init peer task request, peer uses different peer id to generate every request
	// if peerID is not specified
	if peerID == "" {
		peerID = idgen.PeerIDV1(s.peerHost.Ip)
	}
	peerTask := &peer.FileTaskRequest{
		PeerTaskRequest: schedulerv1.PeerTaskRequest{
			Url:      req.Url,
			UrlMeta:  req.UrlMeta,
			PeerId:   peerID,
			PeerHost: s.peerHost,
		},
		Output:             req.Output,
		Limit:              req.Limit,
		DisableBackSource:  req.DisableBackSource,
		KeepOriginalOffset: req.KeepOriginalOffset,
	}
	if len(req.UrlMeta.Range) > 0 {
		r, err := http.ParseURLMetaRange(req.UrlMeta.Range, math.MaxInt64)
		if err != nil {
			err = fmt.Errorf("parse range %s error: %s", req.UrlMeta.Range, err)
			return err
		}
		peerTask.Range = &http.Range{
			Start:  r.Start,
			Length: r.Length,
		}
	}

	traceID := span.SpanContext().TraceID()
	logKV := []any{
		"peer", peerTask.PeerId,
		"component", "downloadService",
	}
	if traceID.IsValid() {
		logKV = append(logKV, "trace", traceID.String())
	}
	log := logger.With(logKV...)

	peerTaskProgress, err := s.peerTaskManager.StartFileTask(ctx, peerTask)
	if err != nil {
		return dferrors.New(commonv1.Code_UnknownError, fmt.Sprintf("%s", err))
	}

	for {
		select {
		case p, ok := <-peerTaskProgress:
			if !ok {
				err = errors.New("progress closed unexpected")
				log.Errorf(err.Error())
				return dferrors.New(commonv1.Code_UnknownError, err.Error())
			}
			if !p.State.Success {
				log.Errorf("task %s/%s failed: %d/%s", p.PeerID, p.TaskID, p.State.Code, p.State.Msg)
				return dferrors.New(p.State.Code, p.State.Msg)
			}
			err = stream.Send(&dfdaemonv1.DownResult{
				TaskId:          p.TaskID,
				PeerId:          p.PeerID,
				CompletedLength: uint64(p.CompletedLength),
				Done:            p.PeerTaskDone,
				Output:          req.Output,
			})
			if err != nil {
				log.Infof("send download result error: %s", err.Error())
				return err
			}
			// peer task sets PeerTaskDone to true only once
			if p.PeerTaskDone {
				p.DoneCallback()
				log.Infof("task %s/%s done", p.PeerID, p.TaskID)
				if req.Uid != 0 && req.Gid != 0 {
					log.Infof("change own to uid %d gid %d", req.Uid, req.Gid)
					if err = os.Chown(req.Output, int(req.Uid), int(req.Gid)); err != nil {
						log.Errorf("change own failed: %s", err)
						return err
					}
				}
				return nil
			}
		case <-ctx.Done():
			err = stream.Send(&dfdaemonv1.DownResult{
				CompletedLength: 0,
				Done:            true,
				Output:          req.Output,
			})
			if err != nil {
				log.Infof("send download result error: %s", err.Error())
				return err
			}
			log.Infof("context done due to %s", ctx.Err())
			return status.Error(codes.Canceled, ctx.Err().Error())
		}
	}
}

func (s *server) StatTask(ctx context.Context, req *dfdaemonv1.StatTaskRequest) (*emptypb.Empty, error) {
	s.Keep()
	taskID := idgen.TaskIDV1(req.Url, req.UrlMeta)
	log := logger.With("function", "StatTask", "URL", req.Url, "Tag", req.UrlMeta.Tag, "taskID", taskID, "LocalOnly", req.LocalOnly)

	log.Info("new stat task request")
	if completed := s.isTaskCompleted(taskID); completed {
		log.Info("task found in local storage")
		return new(emptypb.Empty), nil
	}

	// If only stat local cache and task doesn't exist, return not found
	if req.LocalOnly {
		msg := "task not found in local cache"
		log.Info(msg)
		return nil, dferrors.New(commonv1.Code_PeerTaskNotFound, msg)
	}

	// Check scheduler if other peers hold the task
	task, se := s.peerTaskManager.StatTask(ctx, taskID)
	if se != nil {
		return new(emptypb.Empty), se
	}

	// Task available for download only if task is in succeeded state and has available peer
	if task.State == resource.TaskStateSucceeded && task.HasAvailablePeer {
		return new(emptypb.Empty), nil
	}
	msg := fmt.Sprintf("task found but not available for download, state %s, has available peer %t", task.State, task.HasAvailablePeer)
	log.Info(msg)
	return nil, dferrors.New(commonv1.Code_PeerTaskNotFound, msg)
}

func (s *server) isTaskCompleted(taskID string) bool {
	return s.storageManager.FindCompletedTask(taskID) != nil
}

func (s *server) ImportTask(ctx context.Context, req *dfdaemonv1.ImportTaskRequest) (*emptypb.Empty, error) {
	s.Keep()
	peerID := idgen.PeerIDV1(s.peerHost.Ip)
	taskID := idgen.TaskIDV1(req.Url, req.UrlMeta)
	log := logger.With("function", "ImportTask", "URL", req.Url, "Tag", req.UrlMeta.Tag, "taskID", taskID, "file", req.Path)

	log.Info("new import task request")
	ptm := storage.PeerTaskMetadata{
		PeerID: peerID,
		TaskID: taskID,
	}
	announceFunc := func() {
		// TODO: retry announce on error
		start := time.Now()
		err := s.peerTaskManager.AnnouncePeerTask(context.Background(), ptm, req.Url, req.Type, req.UrlMeta)
		if err != nil {
			log.Warnf("Failed to announce task to scheduler: %s", err)
		} else {
			log.Infof("Announce task (peerID %s) to scheduler in %.6f seconds", ptm.PeerID, time.Since(start).Seconds())
		}
	}

	// 0. Task exists in local storage
	if task := s.storageManager.FindCompletedTask(taskID); task != nil {
		msg := fmt.Sprintf("import file skipped, task already exists with peerID %s", task.PeerID)
		log.Info(msg)

		// Announce to scheduler as well, but in background
		ptm.PeerID = task.PeerID
		go announceFunc()
		return new(emptypb.Empty), nil
	}

	// 1. Register to storageManager
	// TODO: compute and check hash digest if digest exists in ImportTaskRequest
	tsd, err := s.storageManager.RegisterTask(ctx, &storage.RegisterTaskRequest{
		PeerTaskMetadata: storage.PeerTaskMetadata{
			PeerID: peerID,
			TaskID: taskID,
		},
	})
	if err != nil {
		msg := fmt.Sprintf("register task to storage manager failed: %v", err)
		log.Error(msg)
		return nil, errors.New(msg)
	}

	// 2. Import task file
	pieceManager := s.peerTaskManager.GetPieceManager()
	if err := pieceManager.ImportFile(ctx, ptm, tsd, req); err != nil {
		msg := fmt.Sprintf("import file failed: %v", err)
		log.Error(msg)
		return nil, errors.New(msg)
	}
	log.Info("import file succeeded")

	// 3. Announce to scheduler asynchronously
	go announceFunc()

	return new(emptypb.Empty), nil
}

func (s *server) ExportTask(ctx context.Context, req *dfdaemonv1.ExportTaskRequest) (*emptypb.Empty, error) {
	_, err := os.Stat(req.Output)
	if err == nil {
		// we did not export file to exist file
		// TODO add white list folders to write files
		return nil, dferrors.New(commonv1.Code_BadRequest, "output file is already exist")
	}

	// check other stat errors, only os.ErrNotExist is okay
	if !os.IsNotExist(err) {
		return nil, dferrors.New(commonv1.Code_ClientError, err.Error())
	}
	s.Keep()
	taskID := idgen.TaskIDV1(req.Url, req.UrlMeta)
	log := logger.With("function", "ExportTask", "URL", req.Url, "Tag", req.UrlMeta.Tag, "taskID", taskID, "destination", req.Output)

	log.Info("new export task request")
	task := s.storageManager.FindCompletedTask(taskID)
	if task == nil {
		// If only use local cache and task doesn't exist, return error
		if req.LocalOnly {
			msg := fmt.Sprintf("task not found in local storage")
			log.Info(msg)
			return nil, dferrors.New(commonv1.Code_PeerTaskNotFound, msg)
		}
		log.Info("task not found, try from peers")
		return new(emptypb.Empty), s.exportFromPeers(ctx, log, req)
	}

	if err := s.exportFromLocal(ctx, req, task.PeerID, log); err != nil {
		log.Errorf("export from local failed: %s", err)
		return nil, err
	}

	return new(emptypb.Empty), nil
}

func (s *server) exportFromLocal(ctx context.Context, req *dfdaemonv1.ExportTaskRequest, peerID string, log *logger.SugaredLoggerOnWith) error {
	if err := s.storageManager.Store(ctx, &storage.StoreRequest{
		CommonTaskRequest: storage.CommonTaskRequest{
			PeerID:      peerID,
			TaskID:      idgen.TaskIDV1(req.Url, req.UrlMeta),
			Destination: req.Output,
		},
		StoreDataOnly: true,
	}); err != nil {
		return err
	}

	// Change file own if uid and gid are set.
	if req.Uid != 0 && req.Gid != 0 {
		log.Infof("change own to uid %d gid %d", req.Uid, req.Gid)
		if err := os.Chown(req.Output, int(req.Uid), int(req.Gid)); err != nil {
			log.Errorf("change own failed: %s", err)
			return err
		}
	}

	return nil
}

func (s *server) exportFromPeers(ctx context.Context, log *logger.SugaredLoggerOnWith, req *dfdaemonv1.ExportTaskRequest) error {
	peerID := idgen.PeerIDV1(s.peerHost.Ip)
	taskID := idgen.TaskIDV1(req.Url, req.UrlMeta)

	task, err := s.peerTaskManager.StatTask(ctx, taskID)
	if err != nil {
		if dferrors.CheckError(err, commonv1.Code_PeerTaskNotFound) {
			log.Info("task not found in P2P network")
		} else {
			msg := fmt.Sprintf("failed to StatTask from peers: %s", err)
			log.Error(msg)
		}
		return err
	}
	if task.State != resource.TaskStateSucceeded || !task.HasAvailablePeer {
		msg := fmt.Sprintf("task found but not available for download, state %s, has available peer %t", task.State, task.HasAvailablePeer)
		log.Info(msg)
		return dferrors.New(commonv1.Code_PeerTaskNotFound, msg)
	}

	// Task exists in peers
	var (
		start     = time.Now()
		drc       = make(chan *dfdaemonv1.DownResult, 1)
		errChan   = make(chan error, 3)
		result    *dfdaemonv1.DownResult
		downError error
	)
	downRequest := &dfdaemonv1.DownRequest{
		Url:               req.Url,
		Output:            req.Output,
		Timeout:           req.Timeout,
		Limit:             req.Limit,
		DisableBackSource: true,
		UrlMeta:           req.UrlMeta,
		Uid:               req.Uid,
		Gid:               req.Gid,
	}

	go call(ctx, peerID, &simpleResultSender{drc}, s, downRequest, errChan)
	go func() {
		for result = range drc {
			if result.Done {
				log.Infof("export from peer successfully, length: %d bytes cost: %.6f s", result.CompletedLength, time.Since(start).Seconds())
				break
			}
		}
		errChan <- dferrors.ErrEndOfStream
	}()

	if downError = <-errChan; dferrors.IsEndOfStream(downError) {
		downError = nil
	}

	if downError != nil {
		msg := fmt.Sprintf("export from peer failed: %s", downError)
		log.Error(msg)
		return downError
	}
	return nil
}

// TODO remove this wrapper in exportFromPeers
type simpleResultSender struct {
	drc chan *dfdaemonv1.DownResult
}

func (s *simpleResultSender) Send(result *dfdaemonv1.DownResult) error {
	s.drc <- result
	return nil
}

func call(ctx context.Context, peerID string, sender ResultSender, s *server, req *dfdaemonv1.DownRequest, errChan chan error) {
	err := safe.Call(func() {
		if err := s.download(ctx, req, sender, peerID); err != nil {
			errChan <- err
		}
	})

	if err != nil {
		errChan <- err
	}
}

func (s *server) DeleteTask(ctx context.Context, req *dfdaemonv1.DeleteTaskRequest) (*emptypb.Empty, error) {
	s.Keep()
	taskID := idgen.TaskIDV1(req.Url, req.UrlMeta)
	log := logger.With("function", "DeleteTask", "URL", req.Url, "Tag", req.UrlMeta.Tag, "taskID", taskID)

	log.Info("new delete task request")
	task := s.storageManager.FindCompletedTask(taskID)
	if task == nil {
		log.Info("task not found, skip delete")
		return new(emptypb.Empty), nil
	}

	// Unregister task
	unregReq := storage.CommonTaskRequest{
		PeerID: task.PeerID,
		TaskID: taskID,
	}
	if err := s.storageManager.UnregisterTask(ctx, unregReq); err != nil {
		msg := fmt.Sprintf("failed to UnregisterTask: %s", err)
		log.Errorf(msg)
		return nil, errors.New(msg)
	}

	return new(emptypb.Empty), nil
}

// LeaveHost will leave host from scheduler
func (s *server) LeaveHost(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	return new(emptypb.Empty), s.schedulerClient.LeaveHost(ctx, &schedulerv1.LeaveHostRequest{
		Id: s.peerHost.Id,
	})
}

func checkOutput(output string) error {
	if !filepath.IsAbs(output) {
		return fmt.Errorf("path[%s] is not absolute path", output)
	}
	outputDir, _ := path.Split(output)
	if err := config.MkdirAll(outputDir, 0700, os.Getuid(), os.Getgid()); err != nil {
		return err
	}

	// check permission
	for dir := output; dir != ""; dir = filepath.Dir(dir) {
		if err := syscall.Access(dir, syscall.O_RDWR); err == nil {
			break
		} else if os.IsPermission(err) || dir == "/" {
			return fmt.Errorf("user[%s] path[%s] %v", user.Username(), output, err)
		}
	}
	return nil
}
