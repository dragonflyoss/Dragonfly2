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

//go:generate mockgen -destination mocks/client_v2_mock.go -source client_v2.go -package mocks

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

	managerv2 "d7y.io/api/v2/pkg/apis/manager/v2"
	securityv1 "d7y.io/api/v2/pkg/apis/security/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	healthclient "d7y.io/dragonfly/v2/pkg/rpc/health/client"
)

// GetV2ByAddr returns v2 version of the manager client by address.
func GetV2ByAddr(ctx context.Context, target string, opts ...grpc.DialOption) (V2, error) {
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

	return &v2{
		ManagerClient:     managerv2.NewManagerClient(conn),
		CertificateClient: securityv1.NewCertificateClient(conn),
		ClientConn:        conn,
	}, nil
}

// GetV2ByNetAddrs returns v2 version of the manager client with net addresses.
func GetV2ByNetAddrs(ctx context.Context, netAddrs []dfnet.NetAddr, opts ...grpc.DialOption) (V2, error) {
	for _, netAddr := range netAddrs {
		if err := healthclient.Check(context.Background(), netAddr.String(), opts...); err == nil {
			logger.Infof("manager address %s is reachable", netAddr.String())
			return GetV2ByAddr(ctx, netAddr.Addr, opts...)
		}
		logger.Warnf("manager address %s is unreachable", netAddr.String())
	}

	return nil, errors.New("can not find reachable manager addresses")
}

// V2 is the interface for v2 version of the grpc client.
type V2 interface {
	// List Seed peer configuration.
	ListSeedPeers(context.Context, *managerv2.ListSeedPeersRequest, ...grpc.CallOption) (*managerv2.ListSeedPeersResponse, error)

	// Update Seed peer configuration.
	UpdateSeedPeer(context.Context, *managerv2.UpdateSeedPeerRequest, ...grpc.CallOption) (*managerv2.SeedPeer, error)

	// Delete Seed peer configuration.
	DeleteSeedPeer(context.Context, *managerv2.DeleteSeedPeerRequest, ...grpc.CallOption) error

	// Get Scheduler and Scheduler cluster configuration.
	GetScheduler(context.Context, *managerv2.GetSchedulerRequest, ...grpc.CallOption) (*managerv2.Scheduler, error)

	// Update scheduler configuration.
	UpdateScheduler(context.Context, *managerv2.UpdateSchedulerRequest, ...grpc.CallOption) (*managerv2.Scheduler, error)

	// List acitve schedulers configuration.
	ListSchedulers(context.Context, *managerv2.ListSchedulersRequest, ...grpc.CallOption) (*managerv2.ListSchedulersResponse, error)

	// List applications configuration.
	ListApplications(context.Context, *managerv2.ListApplicationsRequest, ...grpc.CallOption) (*managerv2.ListApplicationsResponse, error)

	// KeepAlive with manager.
	KeepAlive(time.Duration, *managerv2.KeepAliveRequest, <-chan struct{}, ...grpc.CallOption)

	// Close tears down the ClientConn and all underlying connections.
	Close() error
}

// v2 provides v2 version of the manager grpc function.
type v2 struct {
	managerv2.ManagerClient
	securityv1.CertificateClient
	*grpc.ClientConn
}

// List acitve seed peers configuration.
func (v *v2) ListSeedPeers(ctx context.Context, req *managerv2.ListSeedPeersRequest, opts ...grpc.CallOption) (*managerv2.ListSeedPeersResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.ListSeedPeers(ctx, req, opts...)
}

// Update SeedPeer configuration.
func (v *v2) UpdateSeedPeer(ctx context.Context, req *managerv2.UpdateSeedPeerRequest, opts ...grpc.CallOption) (*managerv2.SeedPeer, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.UpdateSeedPeer(ctx, req, opts...)
}

// Delete SeedPeer configuration.
func (v *v2) DeleteSeedPeer(ctx context.Context, req *managerv2.DeleteSeedPeerRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	_, err := v.ManagerClient.DeleteSeedPeer(ctx, req, opts...)
	return err
}

// Get Scheduler and Scheduler cluster configuration.
func (v *v2) GetScheduler(ctx context.Context, req *managerv2.GetSchedulerRequest, opts ...grpc.CallOption) (*managerv2.Scheduler, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.GetScheduler(ctx, req, opts...)
}

// Update scheduler configuration.
func (v *v2) UpdateScheduler(ctx context.Context, req *managerv2.UpdateSchedulerRequest, opts ...grpc.CallOption) (*managerv2.Scheduler, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.UpdateScheduler(ctx, req, opts...)
}

// List acitve schedulers configuration.
func (v *v2) ListSchedulers(ctx context.Context, req *managerv2.ListSchedulersRequest, opts ...grpc.CallOption) (*managerv2.ListSchedulersResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.ListSchedulers(ctx, req, opts...)
}

// List applications configuration.
func (v *v2) ListApplications(ctx context.Context, req *managerv2.ListApplicationsRequest, opts ...grpc.CallOption) (*managerv2.ListApplicationsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.ListApplications(ctx, req, opts...)
}

// List acitve schedulers configuration.
func (v *v2) KeepAlive(interval time.Duration, keepalive *managerv2.KeepAliveRequest, done <-chan struct{}, opts ...grpc.CallOption) {
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
			if err := stream.Send(&managerv2.KeepAliveRequest{
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
