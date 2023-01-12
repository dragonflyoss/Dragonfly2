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

	managerv2 "d7y.io/api/pkg/apis/manager/v2"
	securityv1 "d7y.io/api/pkg/apis/security/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/reachable"
)

// GetV2 returns v2 version of the manager client.
func GetV2(ctx context.Context, target string, opts ...grpc.DialOption) (V2, error) {
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

	return &v2{
		ManagerClient:            managerv2.NewManagerClient(conn),
		CertificateServiceClient: securityv1.NewCertificateServiceClient(conn),
		ClientConn:               conn,
	}, nil
}

// GetV2ByAddr returns v2 version of the manager client with addresses.
func GetV2ByAddr(ctx context.Context, netAddrs []dfnet.NetAddr, opts ...grpc.DialOption) (V2, error) {
	for _, netAddr := range netAddrs {
		ipReachable := reachable.New(&reachable.Config{Address: netAddr.Addr})
		if err := ipReachable.Check(); err == nil {
			logger.Infof("use %s address for manager grpc client", netAddr.Addr)
			return GetV2(ctx, netAddr.Addr, opts...)
		}
		logger.Warnf("%s manager address can not reachable", netAddr.Addr)
	}

	return nil, errors.New("can not find available manager addresses")
}

// V2 is the interface for v2 version of the grpc client.
type V2 interface {
	// Update Seed peer configuration.
	UpdateSeedPeer(context.Context, *managerv2.UpdateSeedPeerRequest, ...grpc.CallOption) (*managerv2.SeedPeer, error)

	// Get Scheduler and Scheduler cluster configuration.
	GetScheduler(context.Context, *managerv2.GetSchedulerRequest, ...grpc.CallOption) (*managerv2.Scheduler, error)

	// Update scheduler configuration.
	UpdateScheduler(context.Context, *managerv2.UpdateSchedulerRequest, ...grpc.CallOption) (*managerv2.Scheduler, error)

	// List acitve schedulers configuration.
	ListSchedulers(context.Context, *managerv2.ListSchedulersRequest, ...grpc.CallOption) (*managerv2.ListSchedulersResponse, error)

	// Get object storage configuration.
	GetObjectStorage(context.Context, *managerv2.GetObjectStorageRequest, ...grpc.CallOption) (*managerv2.ObjectStorage, error)

	// List buckets configuration.
	ListBuckets(context.Context, *managerv2.ListBucketsRequest, ...grpc.CallOption) (*managerv2.ListBucketsResponse, error)

	// List models information.
	ListModels(context.Context, *managerv2.ListModelsRequest, ...grpc.CallOption) (*managerv2.ListModelsResponse, error)

	// Get model information.
	GetModel(context.Context, *managerv2.GetModelRequest, ...grpc.CallOption) (*managerv2.Model, error)

	// Create model information.
	CreateModel(context.Context, *managerv2.CreateModelRequest, ...grpc.CallOption) (*managerv2.Model, error)

	// Update model information.
	UpdateModel(context.Context, *managerv2.UpdateModelRequest, ...grpc.CallOption) (*managerv2.Model, error)

	// Delete model information.
	DeleteModel(context.Context, *managerv2.DeleteModelRequest, ...grpc.CallOption) error

	// List model versions information.
	ListModelVersions(context.Context, *managerv2.ListModelVersionsRequest, ...grpc.CallOption) (*managerv2.ListModelVersionsResponse, error)

	// Get model version information.
	GetModelVersion(context.Context, *managerv2.GetModelVersionRequest, ...grpc.CallOption) (*managerv2.ModelVersion, error)

	// Create model version information.
	CreateModelVersion(context.Context, *managerv2.CreateModelVersionRequest, ...grpc.CallOption) (*managerv2.ModelVersion, error)

	// Update model version information.
	UpdateModelVersion(context.Context, *managerv2.UpdateModelVersionRequest, ...grpc.CallOption) (*managerv2.ModelVersion, error)

	// Delete model version information.
	DeleteModelVersion(context.Context, *managerv2.DeleteModelVersionRequest, ...grpc.CallOption) error

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
	securityv1.CertificateServiceClient
	*grpc.ClientConn
}

// Update SeedPeer configuration.
func (v *v2) UpdateSeedPeer(ctx context.Context, req *managerv2.UpdateSeedPeerRequest, opts ...grpc.CallOption) (*managerv2.SeedPeer, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.UpdateSeedPeer(ctx, req, opts...)
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

// Get object storage configuration.
func (v *v2) GetObjectStorage(ctx context.Context, req *managerv2.GetObjectStorageRequest, opts ...grpc.CallOption) (*managerv2.ObjectStorage, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.GetObjectStorage(ctx, req, opts...)
}

// List buckets configuration.
func (v *v2) ListBuckets(ctx context.Context, req *managerv2.ListBucketsRequest, opts ...grpc.CallOption) (*managerv2.ListBucketsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.ListBuckets(ctx, req, opts...)
}

// List models information.
func (v *v2) ListModels(ctx context.Context, req *managerv2.ListModelsRequest, opts ...grpc.CallOption) (*managerv2.ListModelsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.ListModels(ctx, req, opts...)
}

// Get model information.
func (v *v2) GetModel(ctx context.Context, req *managerv2.GetModelRequest, opts ...grpc.CallOption) (*managerv2.Model, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.GetModel(ctx, req, opts...)
}

// Create model information.
func (v *v2) CreateModel(ctx context.Context, req *managerv2.CreateModelRequest, opts ...grpc.CallOption) (*managerv2.Model, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.CreateModel(ctx, req, opts...)
}

// Update model information.
func (v *v2) UpdateModel(ctx context.Context, req *managerv2.UpdateModelRequest, opts ...grpc.CallOption) (*managerv2.Model, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.UpdateModel(ctx, req, opts...)
}

// Delete model information.
func (v *v2) DeleteModel(ctx context.Context, req *managerv2.DeleteModelRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	_, err := v.ManagerClient.DeleteModel(ctx, req, opts...)
	return err
}

// List model versions information.
func (v *v2) ListModelVersions(ctx context.Context, req *managerv2.ListModelVersionsRequest, opts ...grpc.CallOption) (*managerv2.ListModelVersionsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.ListModelVersions(ctx, req, opts...)
}

// Get model version information.
func (v *v2) GetModelVersion(ctx context.Context, req *managerv2.GetModelVersionRequest, opts ...grpc.CallOption) (*managerv2.ModelVersion, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.GetModelVersion(ctx, req, opts...)

}

// Create model version information.
func (v *v2) CreateModelVersion(ctx context.Context, req *managerv2.CreateModelVersionRequest, opts ...grpc.CallOption) (*managerv2.ModelVersion, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.CreateModelVersion(ctx, req, opts...)
}

// Update model version information.
func (v *v2) UpdateModelVersion(ctx context.Context, req *managerv2.UpdateModelVersionRequest, opts ...grpc.CallOption) (*managerv2.ModelVersion, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.UpdateModelVersion(ctx, req, opts...)
}

// Delete model version information.
func (v *v2) DeleteModelVersion(ctx context.Context, req *managerv2.DeleteModelVersionRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	_, err := v.ManagerClient.DeleteModelVersion(ctx, req, opts...)
	return err
}

// List applications configuration.
func (v *v2) ListApplications(ctx context.Context, req *managerv2.ListApplicationsRequest, opts ...grpc.CallOption) (*managerv2.ListApplicationsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.ManagerClient.ListApplications(ctx, req, opts...)
}

// List acitve schedulers configuration.
func (v *v2) KeepAlive(interval time.Duration, keepalive *managerv2.KeepAliveRequest, done <-chan struct{}, opts ...grpc.CallOption) {
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
			if err := stream.Send(&managerv2.KeepAliveRequest{
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
