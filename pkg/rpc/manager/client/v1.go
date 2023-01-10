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

//go:generate mockgen -destination mocks/v1_mock.go -source v1.go -package mocks

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

// GetV1 returns v1 version of the manager client.
func GetV1(ctx context.Context, target string, opts ...grpc.DialOption) (V1, error) {
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

	return &v1{
		ManagerClient:            managerv1.NewManagerClient(conn),
		CertificateServiceClient: securityv1.NewCertificateServiceClient(conn),
		ClientConn:               conn,
	}, nil
}

// GetV1ByAddr returns v1 version of the manager client with addresses.
func GetV1ByAddr(ctx context.Context, netAddrs []dfnet.NetAddr, opts ...grpc.DialOption) (V1, error) {
	for _, netAddr := range netAddrs {
		ipReachable := reachable.New(&reachable.Config{Address: netAddr.Addr})
		if err := ipReachable.Check(); err == nil {
			logger.Infof("use %s address for manager grpc client", netAddr.Addr)
			return GetV1(ctx, netAddr.Addr, opts...)
		}
		logger.Warnf("%s manager address can not reachable", netAddr.Addr)
	}

	return nil, errors.New("can not find available manager addresses")
}

// V1 is the interface for v1 version of the grpc client.
type V1 interface {
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

	// List applications configuration.
	ListApplications(context.Context, *managerv1.ListApplicationsRequest, ...grpc.CallOption) (*managerv1.ListApplicationsResponse, error)

	// KeepAlive with manager.
	KeepAlive(time.Duration, *managerv1.KeepAliveRequest, <-chan struct{}, ...grpc.CallOption)

	// Close tears down the ClientConn and all underlying connections.
	Close() error
}

// v1 provides v1 version of the manager grpc function.
type v1 struct {
	managerv1.ManagerClient
	securityv1.CertificateServiceClient
	*grpc.ClientConn
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

// List models information.
func (v *v1) ListModels(ctx context.Context, req *managerv1.ListModelsRequest, opts ...grpc.CallOption) (*managerv1.ListModelsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.ListModels(ctx, req, opts...)
}

// Get model information.
func (v *v1) GetModel(ctx context.Context, req *managerv1.GetModelRequest, opts ...grpc.CallOption) (*managerv1.Model, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.GetModel(ctx, req, opts...)

}

// Create model information.
func (v *v1) CreateModel(ctx context.Context, req *managerv1.CreateModelRequest, opts ...grpc.CallOption) (*managerv1.Model, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.CreateModel(ctx, req, opts...)
}

// Update model information.
func (v *v1) UpdateModel(ctx context.Context, req *managerv1.UpdateModelRequest, opts ...grpc.CallOption) (*managerv1.Model, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.UpdateModel(ctx, req, opts...)
}

// Delete model information.
func (v *v1) DeleteModel(ctx context.Context, req *managerv1.DeleteModelRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	_, err := v.ManagerClient.DeleteModel(ctx, req, opts...)
	return err
}

// List model versions information.
func (v *v1) ListModelVersions(ctx context.Context, req *managerv1.ListModelVersionsRequest, opts ...grpc.CallOption) (*managerv1.ListModelVersionsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.ListModelVersions(ctx, req, opts...)
}

// Get model version information.
func (v *v1) GetModelVersion(ctx context.Context, req *managerv1.GetModelVersionRequest, opts ...grpc.CallOption) (*managerv1.ModelVersion, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.GetModelVersion(ctx, req, opts...)
}

// Create model version information.
func (v *v1) CreateModelVersion(ctx context.Context, req *managerv1.CreateModelVersionRequest, opts ...grpc.CallOption) (*managerv1.ModelVersion, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.CreateModelVersion(ctx, req, opts...)
}

// Update model version information.
func (v *v1) UpdateModelVersion(ctx context.Context, req *managerv1.UpdateModelVersionRequest, opts ...grpc.CallOption) (*managerv1.ModelVersion, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.UpdateModelVersion(ctx, req, opts...)
}

// Delete model version information.
func (v *v1) DeleteModelVersion(ctx context.Context, req *managerv1.DeleteModelVersionRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	_, err := v.ManagerClient.DeleteModelVersion(ctx, req, opts...)
	return err
}

// List applications configuration.
func (v *v1) ListApplications(ctx context.Context, req *managerv1.ListApplicationsRequest, opts ...grpc.CallOption) (*managerv1.ListApplicationsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.ListApplications(ctx, req, opts...)
}

// List acitve schedulers configuration.
func (v *v1) KeepAlive(interval time.Duration, keepalive *managerv1.KeepAliveRequest, done <-chan struct{}, opts ...grpc.CallOption) {
	log := logger.WithKeepAlive(keepalive.HostName, keepalive.Ip, keepalive.SourceType.Enum().String(), keepalive.ClusterId)
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
