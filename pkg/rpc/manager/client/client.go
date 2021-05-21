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

package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

func NewClient(addrs []dfnet.NetAddr, opts ...grpc.DialOption) (ManagerClient, error) {
	if len(addrs) == 0 {
		return nil, errors.New("manager address list is empty")
	}
	mc := &managerClient{
		locker: &sync.RWMutex{},
		Connection: rpc.NewConnection(context.Background(), "manager", addrs, []rpc.ConnOption{
			rpc.WithConnExpireTime(10 * time.Second),
			rpc.WithDialOption(opts),
		}),
		adders: addrs,
	}
	for idx := range addrs {
		serverNode := addrs[idx].GetEndpoint()
		clientConn, err := mc.Connection.GetClientConnByTarget(serverNode)
		if err == nil {
			mc.curClient = manager.NewManagerClient(clientConn)
			mc.curManagerNode = serverNode
			logger.Infof("connect %s manager server successfully", serverNode)
			return mc, nil
		}
		logger.Warnf("connect %s manager server failed: %v", serverNode, err)
	}
	return nil, fmt.Errorf("all manager nodes cannot be connected, server list: %s", addrs)
}

// see manager.ManagerClient
type ManagerClient interface {
	// GetSchedulers
	GetSchedulers(ctx context.Context, req *manager.GetSchedulersRequest, opts ...grpc.CallOption) (*manager.SchedulerNodes, error)

	// KeepAlive keep alive for cdn or scheduler
	KeepAlive(ctx context.Context, req *manager.KeepAliveRequest, opts ...grpc.CallOption)

	// GetClusterConfig get cluster config for cdn or scheduler(client) from manager
	GetSchedulerClusterConfig(ctx context.Context, req *manager.GetClusterConfigRequest, opts ...grpc.CallOption) (*manager.SchedulerConfig, error)

	// GetCdnClusterConfig
	GetCdnClusterConfig(ctx context.Context, req *manager.GetClusterConfigRequest, opts ...grpc.CallOption) (*manager.CdnConfig, error)

	Close() error
}

type managerClient struct {
	locker *sync.RWMutex
	*rpc.Connection
	curClient      manager.ManagerClient
	curManagerNode string
	adders         []dfnet.NetAddr
}

func init() {
	var client *managerClient = nil
	var _ ManagerClient = client
}

func (mc *managerClient) replaceManager(cause error) error {
	if e, ok := cause.(*dferrors.DfError); ok {
		if e.Code != dfcodes.UnknownError {
			return cause
		}
	}
	mc.locker.Lock()
	defer mc.locker.Unlock()
	// todo Shatters adders slice
	for idx := range mc.adders {
		serverNode := mc.adders[idx].GetEndpoint()
		clientConn, err := mc.Connection.GetClientConnByTarget(serverNode)
		if err == nil {
			preManagerNode := mc.curManagerNode
			mc.curClient = manager.NewManagerClient(clientConn)
			mc.curManagerNode = serverNode
			logger.Infof("replace manager server from %s to %s", preManagerNode, serverNode)
			return nil
		}
		logger.Debugf("connect %s manager server failed: %v", serverNode, err)
	}
	return errors.New("failed to replace manager after attempt to connect all managers")
}

func (mc *managerClient) GetSchedulers(ctx context.Context, req *manager.GetSchedulersRequest, opts ...grpc.CallOption) (*manager.SchedulerNodes, error) {
	if stringutils.IsBlank(req.HostName) && stringutils.IsBlank(req.Ip) {
		return nil, dferrors.Newf(dfcodes.BadRequest, "hostname and ip can not both empty")
	}
	mc.Connection.UpdateAccessNodeMapByServerNode(mc.curManagerNode)
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return mc.curClient.GetSchedulers(ctx, req, opts...)
	}, 0.5, 5.0, 5, nil)
	if err != nil && mc.replaceManager(err) == nil {
		return mc.GetSchedulers(ctx, req, opts...)
	}
	if err != nil {
		return nil, err
	}
	return res.(*manager.SchedulerNodes), nil
}

func (mc *managerClient) KeepAlive(ctx context.Context, req *manager.KeepAliveRequest, opts ...grpc.CallOption) {
	logger.Info("start keep alive")
	mc.Connection.UpdateAccessNodeMapByServerNode(mc.curManagerNode)
	_, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return mc.curClient.KeepAlive(ctx, req, opts...)
	}, 0.5, 5.0, 5, nil)
	if err != nil && mc.replaceManager(err) == nil {
		mc.KeepAlive(ctx, req, opts...)
		return
	}
	if err != nil {
		logger.KeepAliveLogger.Errorf("----- failed to keep %s alive to %s :%v ------", req.HostName, mc.curManagerNode, err)
	} else {
		logger.KeepAliveLogger.Infof("===== successfully keep %s alive to %s =====", req.HostName, mc.curManagerNode)
	}
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			mc.Connection.UpdateAccessNodeMapByServerNode(mc.curManagerNode)
			select {
			case <-ctx.Done():
				logger.KeepAliveLogger.Warnf("ctx cancel, quit keep %s alive to %s", req.HostName, mc.curManagerNode)
				return
			case <-ticker.C:
				_, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
					return mc.curClient.KeepAlive(ctx, req, opts...)
				}, 0.5, 5.0, 5, nil)
				if err != nil && mc.replaceManager(err) == nil {
					_, err := mc.curClient.KeepAlive(ctx, req, opts...)
					if err != nil {
						logger.KeepAliveLogger.Errorf("----- failed to keep %s alive to %s: %v -----", req.HostName, mc.curManagerNode, err)
					} else {
						logger.KeepAliveLogger.Infof("===== successfully keep %s alive to %s =====", req.HostName, mc.curManagerNode)
					}
				}
				if err != nil {
					logger.KeepAliveLogger.Errorf("----- failed to keep %s alive to %s: %v -----", req.HostName, mc.curManagerNode, err)
				} else {
					logger.KeepAliveLogger.Infof("===== successfully keep %s alive to %s =====", req.HostName, mc.curManagerNode)
				}
			}
		}

	}()
}

// GetClusterConfig get cluster config for cdn or scheduler(client) from manager
func (mc *managerClient) GetSchedulerClusterConfig(ctx context.Context, req *manager.GetClusterConfigRequest,
	opts ...grpc.CallOption) (cc *manager.SchedulerConfig, err error) {
	mc.Connection.UpdateAccessNodeMapByServerNode(mc.curManagerNode)
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return mc.curClient.GetClusterConfig(ctx, req, opts...)
	}, 0.5, 5.0, 5, nil)
	if err != nil && mc.replaceManager(err) == nil {
		return mc.GetSchedulerClusterConfig(ctx, req, opts...)
	}
	if err != nil {
		logger.Error("get scheduler cluster config failed: %v", err)
		return nil, err
	}
	cc = res.(*manager.ClusterConfig).GetSchedulerConfig()
	return cc, nil
}

// GetClusterConfig get cluster config for cdn or scheduler(client) from manager
func (mc *managerClient) GetCdnClusterConfig(ctx context.Context, req *manager.GetClusterConfigRequest, opts ...grpc.CallOption) (cc *manager.CdnConfig,
	err error) {
	mc.Connection.UpdateAccessNodeMapByServerNode(mc.curManagerNode)
	ccr := &manager.GetClusterConfigRequest{
		HostName: iputils.HostName,
	}
	ccr.Type = manager.ResourceType_Cdn
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return mc.curClient.GetClusterConfig(ctx, ccr, opts...)
	}, 0.5, 5.0, 5, nil)
	if err != nil && mc.replaceManager(err) == nil {
		return mc.GetCdnClusterConfig(ctx, req, opts...)
	}
	if err != nil {
		logger.Error("get cdn cluster config failed: %v", err)
		return nil, err
	}
	cc = res.(*manager.ClusterConfig).GetCdnConfig()
	return cc, nil
}

func init() {
	var mc *managerClient = nil
	var _ ManagerClient = mc
}
