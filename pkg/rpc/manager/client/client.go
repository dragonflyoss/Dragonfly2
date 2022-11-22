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
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	managerv1 "d7y.io/api/pkg/apis/manager/v1"
	securityv1 "d7y.io/api/pkg/apis/security/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/reachable"
)

const (
	// contextTimeout is timeout of grpc invoke.
	contextTimeout = 2 * time.Minute

	// maxRetries is maximum number of retries.
	maxRetries = 3

	// backoffWaitBetween is waiting for a fixed period of
	// time between calls in backoff linear.
	backoffWaitBetween = 500 * time.Millisecond
)

// GetClient returns manager client.
func GetClient(ctx context.Context, target string, opts ...grpc.DialOption) (Client, error) {
	conn, err := grpc.DialContext(
		ctx,
		target,
		append([]grpc.DialOption{
			grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
				otelgrpc.UnaryClientInterceptor(),
				grpc_prometheus.UnaryClientInterceptor,
				grpc_zap.UnaryClientInterceptor(logger.GrpcLogger.Desugar()),
				grpc_retry.UnaryClientInterceptor(
					grpc_retry.WithMax(maxRetries),
					grpc_retry.WithBackoff(grpc_retry.BackoffLinear(backoffWaitBetween)),
				),
			)),
			grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
				otelgrpc.StreamClientInterceptor(),
				grpc_prometheus.StreamClientInterceptor,
				grpc_zap.StreamClientInterceptor(logger.GrpcLogger.Desugar()),
			)),
		}, opts...)...,
	)
	if err != nil {
		return nil, err
	}

	return &client{
		ManagerClient:            managerv1.NewManagerClient(conn),
		CertificateServiceClient: securityv1.NewCertificateServiceClient(conn),
		ClientConn:               conn,
	}, nil
}

// GetClientByAddr returns manager client with addresses.
func GetClientByAddr(ctx context.Context, netAddrs []dfnet.NetAddr, opts ...grpc.DialOption) (Client, error) {
	for _, netAddr := range netAddrs {
		ipReachable := reachable.New(&reachable.Config{Address: netAddr.Addr})
		if err := ipReachable.Check(); err == nil {
			logger.Infof("use %s address for manager grpc client", netAddr.Addr)
			return GetClient(ctx, netAddr.Addr, opts...)
		}
		logger.Warnf("%s manager address can not reachable", netAddr.Addr)
	}

	return nil, errors.New("can not find available manager addresses")
}

// Client is the interface for grpc client.
type Client interface {
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

	// List models information.
	ListModels(context.Context, *managerv1.ListModelsRequest, ...grpc.CallOption) (*managerv1.ListModelsResponse, error)

	// Get model information.
	GetModel(context.Context, *managerv1.GetModelRequest, ...grpc.CallOption) (*managerv1.Model, error)

	// Create model information.
	CreateModel(context.Context, *managerv1.CreateModelRequest, ...grpc.CallOption) (*managerv1.Model, error)

	// Update model information.
	UpdateModel(context.Context, *managerv1.UpdateModelRequest, ...grpc.CallOption) (*managerv1.Model, error)

	// Delete model information.
	DeleteModel(context.Context, *managerv1.DeleteModelRequest, ...grpc.CallOption) error

	// List model versions information.
	ListModelVersions(context.Context, *managerv1.ListModelVersionsRequest, ...grpc.CallOption) (*managerv1.ListModelVersionsResponse, error)

	// Get model version information.
	GetModelVersion(context.Context, *managerv1.GetModelVersionRequest, ...grpc.CallOption) (*managerv1.ModelVersion, error)

	// Create model version information.
	CreateModelVersion(context.Context, *managerv1.CreateModelVersionRequest, ...grpc.CallOption) (*managerv1.ModelVersion, error)

	// Update model version information.
	UpdateModelVersion(context.Context, *managerv1.UpdateModelVersionRequest, ...grpc.CallOption) (*managerv1.ModelVersion, error)

	// Delete model version information.
	DeleteModelVersion(context.Context, *managerv1.DeleteModelVersionRequest, ...grpc.CallOption) error

	// IssueCertificate issues certificate for client.
	IssueCertificate(context.Context, *securityv1.CertificateRequest, ...grpc.CallOption) (*securityv1.CertificateResponse, error)

	// KeepAlive with manager.
	KeepAlive(time.Duration, *managerv1.KeepAliveRequest, <-chan struct{}, ...grpc.CallOption)

	// Close tears down the ClientConn and all underlying connections.
	Close() error
}

// client provides manager grpc function.
type client struct {
	managerv1.ManagerClient
	securityv1.CertificateServiceClient
	*grpc.ClientConn
}

// Update SeedPeer configuration.
func (c *client) UpdateSeedPeer(ctx context.Context, req *managerv1.UpdateSeedPeerRequest, opts ...grpc.CallOption) (*managerv1.SeedPeer, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return c.ManagerClient.UpdateSeedPeer(ctx, req, opts...)
}

// Get Scheduler and Scheduler cluster configuration.
func (c *client) GetScheduler(ctx context.Context, req *managerv1.GetSchedulerRequest, opts ...grpc.CallOption) (*managerv1.Scheduler, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return c.ManagerClient.GetScheduler(ctx, req, opts...)
}

// Update scheduler configuration.
func (c *client) UpdateScheduler(ctx context.Context, req *managerv1.UpdateSchedulerRequest, opts ...grpc.CallOption) (*managerv1.Scheduler, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return c.ManagerClient.UpdateScheduler(ctx, req, opts...)
}

// List acitve schedulers configuration.
func (c *client) ListSchedulers(ctx context.Context, req *managerv1.ListSchedulersRequest, opts ...grpc.CallOption) (*managerv1.ListSchedulersResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return c.ManagerClient.ListSchedulers(ctx, req, opts...)
}

// Get object storage configuration.
func (c *client) GetObjectStorage(ctx context.Context, req *managerv1.GetObjectStorageRequest, opts ...grpc.CallOption) (*managerv1.ObjectStorage, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return c.ManagerClient.GetObjectStorage(ctx, req, opts...)
}

// List buckets configuration.
func (c *client) ListBuckets(ctx context.Context, req *managerv1.ListBucketsRequest, opts ...grpc.CallOption) (*managerv1.ListBucketsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return c.ManagerClient.ListBuckets(ctx, req, opts...)
}

// List models information.
func (c *client) ListModels(ctx context.Context, req *managerv1.ListModelsRequest, opts ...grpc.CallOption) (*managerv1.ListModelsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return c.ManagerClient.ListModels(ctx, req, opts...)
}

// Get model information.
func (c *client) GetModel(ctx context.Context, req *managerv1.GetModelRequest, opts ...grpc.CallOption) (*managerv1.Model, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return c.ManagerClient.GetModel(ctx, req, opts...)

}

// Create model information.
func (c *client) CreateModel(ctx context.Context, req *managerv1.CreateModelRequest, opts ...grpc.CallOption) (*managerv1.Model, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return c.ManagerClient.CreateModel(ctx, req, opts...)
}

// Update model information.
func (c *client) UpdateModel(ctx context.Context, req *managerv1.UpdateModelRequest, opts ...grpc.CallOption) (*managerv1.Model, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return c.ManagerClient.UpdateModel(ctx, req, opts...)
}

// Delete model information.
func (c *client) DeleteModel(ctx context.Context, req *managerv1.DeleteModelRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	_, err := c.ManagerClient.DeleteModel(ctx, req, opts...)
	return err
}

// List model versions information.
func (c *client) ListModelVersions(ctx context.Context, req *managerv1.ListModelVersionsRequest, opts ...grpc.CallOption) (*managerv1.ListModelVersionsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return c.ManagerClient.ListModelVersions(ctx, req, opts...)
}

// Get model version information.
func (c *client) GetModelVersion(ctx context.Context, req *managerv1.GetModelVersionRequest, opts ...grpc.CallOption) (*managerv1.ModelVersion, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return c.ManagerClient.GetModelVersion(ctx, req, opts...)

}

// Create model version information.
func (c *client) CreateModelVersion(ctx context.Context, req *managerv1.CreateModelVersionRequest, opts ...grpc.CallOption) (*managerv1.ModelVersion, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return c.ManagerClient.CreateModelVersion(ctx, req, opts...)
}

// Update model version information.
func (c *client) UpdateModelVersion(ctx context.Context, req *managerv1.UpdateModelVersionRequest, opts ...grpc.CallOption) (*managerv1.ModelVersion, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return c.ManagerClient.UpdateModelVersion(ctx, req, opts...)

}

// Delete model version information.
func (c *client) DeleteModelVersion(ctx context.Context, req *managerv1.DeleteModelVersionRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	_, err := c.ManagerClient.DeleteModelVersion(ctx, req, opts...)
	return err
}

func (c *client) IssueCertificate(ctx context.Context, req *securityv1.CertificateRequest, opts ...grpc.CallOption) (*securityv1.CertificateResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return c.CertificateServiceClient.IssueCertificate(ctx, req, opts...)
}

// List acitve schedulers configuration.
func (c *client) KeepAlive(interval time.Duration, keepalive *managerv1.KeepAliveRequest, done <-chan struct{}, opts ...grpc.CallOption) {
	log := logger.WithKeepAlive(keepalive.HostName, keepalive.Ip, keepalive.SourceType.Enum().String(), keepalive.ClusterId)
retry:
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := c.ManagerClient.KeepAlive(ctx, opts...)
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
				HostName:   keepalive.HostName,
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
