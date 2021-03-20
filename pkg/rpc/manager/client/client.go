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
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"errors"
	"github.com/golang/protobuf/jsonpb"
	"google.golang.org/grpc"
	"sync"
	"time"
)

// see manager.ManagerClient
type ManagerClient interface {
	AddConfig(ctx context.Context, req *manager.AddConfigRequest, opts ...grpc.CallOption) (rep *manager.AddConfigResponse, err error)
	DeleteConfig(ctx context.Context, req *manager.DeleteConfigRequest, opts ...grpc.CallOption) (rep *manager.DeleteConfigResponse, err error)
	UpdateConfig(ctx context.Context, req *manager.UpdateConfigRequest, opts ...grpc.CallOption) (rep *manager.UpdateConfigResponse, err error)
	GetConfig(ctx context.Context, req *manager.GetConfigRequest, opts ...grpc.CallOption) (rep *manager.GetConfigResponse, err error)
	ListConfigs(ctx context.Context, req *manager.ListConfigsRequest, opts ...grpc.CallOption) (rep *manager.ListConfigsResponse, err error)

	KeepAlive(ctx context.Context, req *manager.KeepAliveRequest, opts ...grpc.CallOption) (rep *manager.KeepAliveResponse, err error)
	ListSchedulers(ctx context.Context, req *manager.ListSchedulersRequest, opts ...grpc.CallOption) (rep *manager.ListSchedulersResponse, err error)

	NewKeepAliveRoutine(object string, objType string, intervalSecond uint32) (chan struct{}, error)
}

type managerClient struct {
	*rpc.Connection

	schedulerConfig *manager.SchedulerConfig
	cdnConfig       *manager.CdnConfig
	cdns            map[string]*manager.ServerInfo

	mu        sync.Mutex
	ch        chan struct{}
	closeDone bool
}

func (mc *managerClient) getManagerClient(key string) (manager.ManagerClient, error) {
	if clientConn, err := mc.Connection.GetClientConn(key); err != nil {
		return nil, err
	} else {
		return manager.NewManagerClient(clientConn), nil
	}
}

func CreateClient(addrs []dfnet.NetAddr, opts ...grpc.DialOption) (ManagerClient, error) {
	if len(addrs) == 0 {
		return nil, errors.New("address list of cdn is empty")
	}
	return &managerClient{
		Connection: rpc.NewConnection("management", addrs, opts...),
		cdns:       make(map[string]*manager.ServerInfo),
		ch:         make(chan struct{}),
		closeDone:  false,
	}, nil
}

func (mc *managerClient) AddConfig(ctx context.Context, req *manager.AddConfigRequest, opts ...grpc.CallOption) (rep *manager.AddConfigResponse, err error) {
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		if client, err := mc.getManagerClient(req.GetConfig().GetObject()); err != nil {
			return nil, err
		} else {
			return client.AddConfig(ctx, req, opts...)
		}
	}, 0.5, 5.0, 5, nil)

	if err != nil {
		logger.Errorf("add config: object %s, objType %s",
			req.Config.GetObject(),
			req.Config.GetType())
	} else {
		rep = res.(*manager.AddConfigResponse)
		logger.Infof("add config: object %s, objType %s, id %s, state %s",
			req.Config.GetObject(),
			req.Config.GetType(),
			rep.GetId(),
			rep.GetState().String())
	}

	return
}

func (mc *managerClient) DeleteConfig(ctx context.Context, req *manager.DeleteConfigRequest, opts ...grpc.CallOption) (rep *manager.DeleteConfigResponse, err error) {
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		if client, err := mc.getManagerClient(req.GetId()); err != nil {
			return nil, err
		} else {
			return client.DeleteConfig(ctx, req, opts...)
		}
	}, 0.5, 5.0, 5, nil)

	if err != nil {
		logger.Errorf("delete config: id %s", req.GetId())
	} else {
		rep = res.(*manager.DeleteConfigResponse)
		logger.Infof("delete config: id %s, state %s",
			req.GetId(),
			rep.GetState().String())
	}

	return
}

func (mc *managerClient) UpdateConfig(ctx context.Context, req *manager.UpdateConfigRequest, opts ...grpc.CallOption) (rep *manager.UpdateConfigResponse, err error) {
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		if client, err := mc.getManagerClient(req.GetId()); err != nil {
			return nil, err
		} else {
			return client.UpdateConfig(ctx, req, opts...)
		}
	}, 0.5, 5.0, 5, nil)

	if err != nil {
		logger.Errorf("update config: object %s, objType %s, id %s",
			req.Config.GetObject(),
			req.Config.GetType(),
			req.GetId())
	} else {
		rep = res.(*manager.UpdateConfigResponse)
		logger.Infof("update config: object %s, objType %s, id %s, state %s",
			req.Config.GetObject(),
			req.Config.GetType(),
			req.GetId(),
			rep.GetState().String())
	}

	return
}

func (mc *managerClient) GetConfig(ctx context.Context, req *manager.GetConfigRequest, opts ...grpc.CallOption) (rep *manager.GetConfigResponse, err error) {
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		if client, err := mc.getManagerClient(req.GetId()); err != nil {
			return nil, err
		} else {
			return client.GetConfig(ctx, req, opts...)
		}
	}, 0.5, 5.0, 5, nil)

	if err != nil {
		logger.Errorf("get config: id %s", req.GetId())
	} else {
		rep = res.(*manager.GetConfigResponse)
		logger.Infof("get config: object %s, objType %s, id %s, state %s",
			rep.Config.GetObject(),
			rep.Config.GetType(),
			req.GetId(),
			rep.GetState().String())
	}

	return
}

func (mc *managerClient) ListConfigs(ctx context.Context, req *manager.ListConfigsRequest, opts ...grpc.CallOption) (rep *manager.ListConfigsResponse, err error) {
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		if client, err := mc.getManagerClient(req.GetObject()); err != nil {
			return nil, err
		} else {
			return client.ListConfigs(ctx, req, opts...)
		}
	}, 0.5, 5.0, 5, nil)

	if err != nil {
		logger.Errorf("list configs: object %s", req.GetObject())
	} else {
		rep = res.(*manager.ListConfigsResponse)
		logger.Infof("list configs: object %s, state %s",
			req.GetObject(),
			rep.GetState().String())
	}

	return
}

func (mc *managerClient) KeepAlive(ctx context.Context, req *manager.KeepAliveRequest, opts ...grpc.CallOption) (rep *manager.KeepAliveResponse, err error) {
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		if client, err := mc.getManagerClient(req.GetObject()); err != nil {
			return nil, err
		} else {
			return client.KeepAlive(ctx, req, opts...)
		}
	}, 0.5, 5.0, 5, nil)

	if err != nil {
		logger.Errorf("keepalive: object %s, objType %s",
			req.GetObject(),
			req.GetType())
	} else {
		rep = res.(*manager.KeepAliveResponse)
		logger.Infof("keepalive: object %s, objType %s, version %s, state %s",
			req.GetObject(),
			req.GetType(),
			rep.Config.GetVersion(),
			rep.GetState().String())
	}

	return
}

func (mc *managerClient) ListSchedulers(ctx context.Context, req *manager.ListSchedulersRequest, opts ...grpc.CallOption) (rep *manager.ListSchedulersResponse, err error) {
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		if client, err := mc.getManagerClient(req.GetHostName()); err != nil {
			return nil, err
		} else {
			return client.ListSchedulers(ctx, req, opts...)
		}
	}, 0.5, 5.0, 5, nil)

	if err != nil {
		logger.Errorf("list configs: ip %s, host %s, hostTag",
			req.GetIp(),
			req.GetHostName(),
			req.GetHostTag())
	} else {
		rep = res.(*manager.ListSchedulersResponse)
		logger.Infof("list configs: ip %s, host %s, hostTag",
			req.GetIp(),
			req.GetHostName(),
			req.GetHostTag())
	}

	return
}

func (mc *managerClient) NewKeepAliveRoutine(object string, objType string, intervalSecond uint32) (chan struct{}, error) {
	if (objType != manager.ObjType_Scheduler.String()) && (objType != manager.ObjType_Cdn.String()) {
		return nil, dferrors.Newf(dfcodes.InvalidObjType, "Invalid objType %s", objType)
	}

	if intervalSecond == 0 {
		intervalSecond = 5
	}

	close := make(chan struct{})
	ticker := time.NewTicker(time.Duration(intervalSecond) * time.Second)
	go func() {
		for {
			select {
			case <-close:
				return
			case <-ticker.C:
				rep, err := mc.KeepAlive(context.TODO(), &manager.KeepAliveRequest{
					Object:  object,
					Type: objType,
				})

				if err != nil {
					logger.Errorf("keepalive error:%+v", err)
					continue
				}

				if rep.GetState().Success {
					mc.fillBackConfig(rep.GetConfig())
				} else {
					err = errors.New(rep.GetState().GetMsg())
					logger.Errorf("keepalive error:%+v", err)
				}
			}
		}
	}()

	return close, nil
}

func (mc *managerClient) fillBackConfig(config *manager.Config) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	switch config.GetType() {
	case manager.ObjType_Scheduler.String():
		protoConfig := &manager.SchedulerConfig{}
		if err := jsonpb.UnmarshalString(string(config.GetData()), protoConfig); err != nil {
			logger.Errorf("scheduler config data unmarshal error:%+v", err)
		} else {
			mc.schedulerConfig = protoConfig
			if mc.schedulerConfig.CdnHosts != nil {
				mc.cdns = make(map[string]*manager.ServerInfo)
				for _, h := range mc.schedulerConfig.CdnHosts {
					mc.cdns[h.HostInfo.HostName] = h
				}
			}
		}
	case manager.ObjType_Cdn.String():
		protoConfig := &manager.CdnConfig{}
		if err := jsonpb.UnmarshalString(string(config.GetData()), protoConfig); err != nil {
			logger.Errorf("cdn config data unmarshal error:%+v", err)
		} else {
			mc.cdnConfig = protoConfig
		}
	default:
		break
	}
}
