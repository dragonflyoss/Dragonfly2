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

//go:generate mockgen -destination mocks/client_mock.go -source client.go -package mocks

package client

import (
	"context"
	"errors"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	managerv1 "d7y.io/api/pkg/apis/manager/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/reachable"
)

const (
	// maxRetries is maximum number of retries.
	maxRetries = 3

	// backoffWaitBetween is waiting for a fixed period of
	// time between calls in backoff linear.
	backoffWaitBetween = 500 * time.Millisecond

	// perRetryTimeout is GRPC timeout per call (including initial call) on this call.
	perRetryTimeout = 5 * time.Second
)

// defaultDialOptions is default dial options of manager client.
var defaultDialOptions = []grpc.DialOption{
	grpc.WithTransportCredentials(insecure.NewCredentials()),
	grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
		grpc_prometheus.UnaryClientInterceptor,
		grpc_zap.UnaryClientInterceptor(logger.GrpcLogger.Desugar()),
		grpc_retry.UnaryClientInterceptor(
			grpc_retry.WithPerRetryTimeout(perRetryTimeout),
			grpc_retry.WithMax(maxRetries),
			grpc_retry.WithBackoff(grpc_retry.BackoffLinear(backoffWaitBetween)),
		),
	)),
	grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
		grpc_prometheus.StreamClientInterceptor,
		grpc_zap.StreamClientInterceptor(logger.GrpcLogger.Desugar()),
	)),
}

// GetClient returns manager client.
func GetClient(target string, options ...grpc.DialOption) (Client, error) {
	conn, err := grpc.Dial(
		target,
		append(defaultDialOptions, options...)...,
	)
	if err != nil {
		return nil, err
	}

	return &client{
		ManagerClient: managerv1.NewManagerClient(conn),
		conn:          conn,
	}, nil
}

// GetClientByAddr returns manager client with addresses.
func GetClientByAddr(netAddrs []dfnet.NetAddr, opts ...grpc.DialOption) (Client, error) {
	for _, netAddr := range netAddrs {
		ipReachable := reachable.New(&reachable.Config{Address: netAddr.Addr})
		if err := ipReachable.Check(); err == nil {
			logger.Infof("use %s address for manager grpc client", netAddr.Addr)
			return GetClient(netAddr.Addr, opts...)
		}
		logger.Warnf("%s manager address can not reachable", netAddr.Addr)
	}

	return nil, errors.New("can not find available manager addresses")
}

// Client is the interface for grpc client.
type Client interface {
	// Update Seed peer configuration.
	UpdateSeedPeer(context.Context, *managerv1.UpdateSeedPeerRequest) (*managerv1.SeedPeer, error)

	// Get Scheduler and Scheduler cluster configuration.
	GetScheduler(context.Context, *managerv1.GetSchedulerRequest) (*managerv1.Scheduler, error)

	// Update scheduler configuration.
	UpdateScheduler(context.Context, *managerv1.UpdateSchedulerRequest) (*managerv1.Scheduler, error)

	// List acitve schedulers configuration.
	ListSchedulers(context.Context, *managerv1.ListSchedulersRequest) (*managerv1.ListSchedulersResponse, error)

	// Get object storage configuration.
	GetObjectStorage(context.Context, *managerv1.GetObjectStorageRequest) (*managerv1.ObjectStorage, error)

	// List buckets configuration.
	ListBuckets(context.Context, *managerv1.ListBucketsRequest) (*managerv1.ListBucketsResponse, error)

	// KeepAlive with manager.
	KeepAlive(time.Duration, *managerv1.KeepAliveRequest)

	// Close client connect.
	Close() error
}

// client provides manager grpc function.
type client struct {
	managerv1.ManagerClient
	conn *grpc.ClientConn
}

// Update SeedPeer configuration.
func (c *client) UpdateSeedPeer(ctx context.Context, req *managerv1.UpdateSeedPeerRequest) (*managerv1.SeedPeer, error) {
	return c.ManagerClient.UpdateSeedPeer(ctx, req)
}

// Get Scheduler and Scheduler cluster configuration.
func (c *client) GetScheduler(ctx context.Context, req *managerv1.GetSchedulerRequest) (*managerv1.Scheduler, error) {
	return c.ManagerClient.GetScheduler(ctx, req)
}

// Update scheduler configuration.
func (c *client) UpdateScheduler(ctx context.Context, req *managerv1.UpdateSchedulerRequest) (*managerv1.Scheduler, error) {
	return c.ManagerClient.UpdateScheduler(ctx, req)
}

// List acitve schedulers configuration.
func (c *client) ListSchedulers(ctx context.Context, req *managerv1.ListSchedulersRequest) (*managerv1.ListSchedulersResponse, error) {
	return c.ManagerClient.ListSchedulers(ctx, req)
}

// Get object storage configuration.
func (c *client) GetObjectStorage(ctx context.Context, req *managerv1.GetObjectStorageRequest) (*managerv1.ObjectStorage, error) {
	return c.ManagerClient.GetObjectStorage(ctx, req)
}

// List buckets configuration.
func (c *client) ListBuckets(ctx context.Context, req *managerv1.ListBucketsRequest) (*managerv1.ListBucketsResponse, error) {
	return c.ManagerClient.ListBuckets(ctx, req)
}

// List acitve schedulers configuration.
func (c *client) KeepAlive(interval time.Duration, keepalive *managerv1.KeepAliveRequest) {
retry:
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := c.ManagerClient.KeepAlive(ctx)
	if err != nil {
		if status.Code(err) == codes.Canceled {
			logger.Infof("hostname %s ip %s cluster id %d stop keepalive", keepalive.HostName, keepalive.Ip, keepalive.ClusterId)
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
				HostName:   keepalive.HostName,
				Ip:         keepalive.Ip,
				ClusterId:  keepalive.ClusterId,
			}); err != nil {
				if _, err := stream.CloseAndRecv(); err != nil {
					logger.Errorf("hostname %s ip %s cluster id %d close and recv stream failed: %v", keepalive.HostName, keepalive.Ip, keepalive.ClusterId, err)
				}

				cancel()
				goto retry
			}
		}
	}
}

// Close grpc service.
func (c *client) Close() error {
	return c.conn.Close()
}
