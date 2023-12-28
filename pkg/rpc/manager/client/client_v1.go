/*
 *     Copyright 2023 The Dragonfly Authors
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

//go:generate mockgen -destination mocks/client_v1_mock.go -source client_v1.go -package mocks

package client

import (
	"context"
	"errors"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	managerv1 "d7y.io/api/v2/pkg/apis/manager/v1"
	securityv1 "d7y.io/api/v2/pkg/apis/security/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	healthclient "d7y.io/dragonfly/v2/pkg/rpc/health/client"
)

// GetV1ByAddr returns v1 version of the manager client by address.
func GetV1ByAddr(ctx context.Context, target string, opts ...grpc.DialOption) (V1, error) {
	conn, err := grpc.DialContext(
		ctx,
		target,
		append([]grpc.DialOption{
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
			grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
				grpc_prometheus.UnaryClientInterceptor,
				grpc_zap.UnaryClientInterceptor(logger.GrpcLogger.Desugar()),
				grpc_retry.UnaryClientInterceptor(
					grpc_retry.WithMax(maxRetries),
					grpc_retry.WithBackoff(grpc_retry.BackoffLinear(backoffWaitBetween)),
				),
			)),
			grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
				grpc_prometheus.StreamClientInterceptor,
				grpc_zap.StreamClientInterceptor(logger.GrpcLogger.Desugar()),
			)),
		}, opts...)...,
	)
	if err != nil {
		return nil, err
	}

	return &v1{
		ManagerClient:     managerv1.NewManagerClient(conn),
		CertificateClient: securityv1.NewCertificateClient(conn),
		ClientConn:        conn,
	}, nil
}

// GetV1ByNetAddrs returns v1 version of the manager client with net addresses.
func GetV1ByNetAddrs(ctx context.Context, netAddrs []dfnet.NetAddr, opts ...grpc.DialOption) (V1, error) {
	for _, netAddr := range netAddrs {
		if err := healthclient.Check(context.Background(), netAddr.String(), opts...); err == nil {
			logger.Infof("manager address %s is reachable", netAddr.String())
			return GetV1ByAddr(ctx, netAddr.Addr, opts...)
		}
		logger.Warnf("manager address %s is unreachable", netAddr.String())
	}

	return nil, errors.New("can not find reachable manager addresses")
}

// V1 is the interface for v1 version of the grpc client.
type V1 interface {
	// List Seed peer configuration.
	ListSeedPeers(context.Context, *managerv1.ListSeedPeersRequest, ...grpc.CallOption) (*managerv1.ListSeedPeersResponse, error)

	// Update Seed peer configuration.
	UpdateSeedPeer(context.Context, *managerv1.UpdateSeedPeerRequest, ...grpc.CallOption) (*managerv1.SeedPeer, error)

	// Get Scheduler and Scheduler cluster configuration.
	GetScheduler(context.Context, *managerv1.GetSchedulerRequest, ...grpc.CallOption) (*managerv1.Scheduler, error)

	// Update scheduler configuration.
	UpdateScheduler(context.Context, *managerv1.UpdateSchedulerRequest, ...grpc.CallOption) (*managerv1.Scheduler, error)

	// List acitve schedulers configuration.
	ListSchedulers(context.Context, *managerv1.ListSchedulersRequest, ...grpc.CallOption) (*managerv1.ListSchedulersResponse, error)

	// Get object storage configuration.
	GetObjectStorage(context.Context, *managerv1.GetObjectStorageRequest, ...grpc.CallOption) (*managerv1.ObjectStorage, error)

	// List buckets configuration.
	ListBuckets(context.Context, *managerv1.ListBucketsRequest, ...grpc.CallOption) (*managerv1.ListBucketsResponse, error)

	// List applications configuration.
	ListApplications(context.Context, *managerv1.ListApplicationsRequest, ...grpc.CallOption) (*managerv1.ListApplicationsResponse, error)

	// Create model and update data of model to object storage.
	CreateModel(context.Context, *managerv1.CreateModelRequest, ...grpc.CallOption) error

	// KeepAlive with manager.
	KeepAlive(time.Duration, *managerv1.KeepAliveRequest, <-chan struct{}, ...grpc.CallOption)

	// Close tears down the ClientConn and all underlying connections.
	Close() error
}

// v1 provides v1 version of the manager grpc function.
type v1 struct {
	managerv1.ManagerClient
	securityv1.CertificateClient
	*grpc.ClientConn
}

// List acitve seed peers configuration.
func (v *v1) ListSeedPeers(ctx context.Context, req *managerv1.ListSeedPeersRequest, opts ...grpc.CallOption) (*managerv1.ListSeedPeersResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.ListSeedPeers(ctx, req, opts...)
}

// Update SeedPeer configuration.
func (v *v1) UpdateSeedPeer(ctx context.Context, req *managerv1.UpdateSeedPeerRequest, opts ...grpc.CallOption) (*managerv1.SeedPeer, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.UpdateSeedPeer(ctx, req, opts...)
}

// Get Scheduler and Scheduler cluster configuration.
func (v *v1) GetScheduler(ctx context.Context, req *managerv1.GetSchedulerRequest, opts ...grpc.CallOption) (*managerv1.Scheduler, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.GetScheduler(ctx, req, opts...)
}

// Update scheduler configuration.
func (v *v1) UpdateScheduler(ctx context.Context, req *managerv1.UpdateSchedulerRequest, opts ...grpc.CallOption) (*managerv1.Scheduler, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.UpdateScheduler(ctx, req, opts...)
}

// List acitve schedulers configuration.
func (v *v1) ListSchedulers(ctx context.Context, req *managerv1.ListSchedulersRequest, opts ...grpc.CallOption) (*managerv1.ListSchedulersResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.ListSchedulers(ctx, req, opts...)
}

// Get object storage configuration.
func (v *v1) GetObjectStorage(ctx context.Context, req *managerv1.GetObjectStorageRequest, opts ...grpc.CallOption) (*managerv1.ObjectStorage, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.GetObjectStorage(ctx, req, opts...)
}

// List buckets configuration.
func (v *v1) ListBuckets(ctx context.Context, req *managerv1.ListBucketsRequest, opts ...grpc.CallOption) (*managerv1.ListBucketsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.ListBuckets(ctx, req, opts...)
}

// List applications configuration.
func (v *v1) ListApplications(ctx context.Context, req *managerv1.ListApplicationsRequest, opts ...grpc.CallOption) (*managerv1.ListApplicationsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.ListApplications(ctx, req, opts...)
}

// Create model and update data of model to object storage.
func (v *v1) CreateModel(ctx context.Context, req *managerv1.CreateModelRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, createModelContextTimeout)
	defer cancel()

	_, err := v.ManagerClient.CreateModel(ctx, req, opts...)
	return err
}

// List acitve schedulers configuration.
func (v *v1) KeepAlive(interval time.Duration, keepalive *managerv1.KeepAliveRequest, done <-chan struct{}, opts ...grpc.CallOption) {
	log := logger.WithKeepAlive(keepalive.Hostname, keepalive.Ip, keepalive.SourceType.Enum().String(), keepalive.ClusterId)
retry:
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := v.ManagerClient.KeepAlive(ctx, opts...)
	if err != nil {
		if status.Code(err) == codes.Canceled {
			log.Info("keepalive canceled")
			cancel()
			return
		}

		time.Sleep(interval)
		cancel()
		goto retry
	}

	tick := time.NewTicker(interval)
	for {
		select {
		case <-tick.C:
			if err := stream.Send(&managerv1.KeepAliveRequest{
				SourceType: keepalive.SourceType,
				Hostname:   keepalive.Hostname,
				Ip:         keepalive.Ip,
				ClusterId:  keepalive.ClusterId,
			}); err != nil {
				if _, err := stream.CloseAndRecv(); err != nil {
					log.Infof("recv stream failed: %s", err.Error())
				}

				cancel()
				goto retry
			}
		case <-done:
			log.Info("keepalive done")
			cancel()
			return
		}
	}
}
