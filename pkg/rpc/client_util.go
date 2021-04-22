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

package rpc

import (
	"context"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/util/mathutils"
)

func (conn *Connection) startGC() {
	// todo 从hashing环中删除频繁失败的节点
	logger.GrpcLogger.With("conn", conn.name).Debugf("start the gc connections job")
	// execute the GC by fixed delay
	ticker := time.NewTicker(conn.gcConnInterval)
	for range ticker.C {
		removedConnCount := 0
		totalNodeSize := 0
		startTime := time.Now()

		// todo use anther locker, @santong
		//conn.rwMutex.Lock()
		// range all connections and determine whether they are expired
		conn.accessNodeMap.Range(func(node, accessTime interface{}) bool {
			serverNode := node.(string)
			totalNodeSize += 1
			atime := accessTime.(time.Time)
			if time.Since(atime) < conn.connExpireTime {
				return true
			}
			conn.gcConn(serverNode)
			removedConnCount++
			return true
		})
		// todo use anther locker, @santong
		//conn.rwMutex.Unlock()
		// slow GC detected, report it with a log warning
		if timeElapse := time.Since(startTime); timeElapse > conn.gcConnTimeout {
			logger.GrpcLogger.With("conn", conn.name).Warnf("gc %d conns, cost:%.3f seconds", removedConnCount, timeElapse.Seconds())
		}
		actualTotal := 0
		conn.node2ClientMap.Range(func(key, value interface{}) bool {
			if value != nil {
				actualTotal++
			}
			return true
		})
		logger.GrpcLogger.With("conn", conn.name).Infof("successfully gc clientConn count(%d), remainder count(%d), actualTotalConnCount(%d)",
			removedConnCount, totalNodeSize-removedConnCount, actualTotal)
	}
}

// gcConn gc keys and clients associated with server node
func (conn *Connection) gcConn(node string) {
	conn.rwMutex.Lock(node, false)
	defer conn.rwMutex.UnLock(node, false)
	logger.GrpcLogger.With("conn", conn.name).Infof("gc keys and clients associated with server node:%s starting", node)
	value, ok := conn.node2ClientMap.Load(node)
	if ok {
		clientCon := value.(*grpc.ClientConn)
		err := clientCon.Close()
		if err == nil {
			conn.node2ClientMap.Delete(node)
			logger.GrpcLogger.With("conn", conn.name).Infof("success gc clientConn:%s", node)
		} else {
			logger.GrpcLogger.With("conn", conn.name).Warnf("failed to close clientConn:%s: %v", node, err)
		}
	} else {
		logger.GrpcLogger.With("conn", conn.name).Warnf("server node:%s dose not found in node2ClientMap", node)
	}
	// gc hash keys
	conn.key2NodeMap.Range(func(key, value interface{}) bool {
		if value == node {
			conn.key2NodeMap.Delete(key)
			logger.GrpcLogger.With("conn", conn.name).Infof("success gc key:%s associated with server node %s", key, node)
		}
		return true
	})
	conn.accessNodeMap.Delete(node)
	logger.GrpcLogger.With("conn", conn.name).Infof("gc keys and clients associated with server node:%s ending", node)
}

type wrappedClientStream struct {
	grpc.ClientStream
	method string
	cc     *grpc.ClientConn
}

func (w *wrappedClientStream) RecvMsg(m interface{}) error {
	err := w.ClientStream.RecvMsg(m)
	if err != nil && err != io.EOF {
		err = convertClientError(err)
		logger.GrpcLogger.Errorf("client receive a message:%T error:%v for method:%s target:%s connState:%s", m, err, w.method, w.cc.Target(), w.cc.GetState().String())
	}

	return err
}

func (w *wrappedClientStream) SendMsg(m interface{}) error {
	err := w.ClientStream.SendMsg(m)
	if err != nil {
		logger.GrpcLogger.Errorf("client send a message:%T error:%v for method:%s target:%s connState:%s", m, err, w.method, w.cc.Target(), w.cc.GetState().String())
	}

	return err
}

func streamClientInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	s, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		err = convertClientError(err)
		logger.GrpcLogger.Errorf("create client stream error:%v for method:%s target:%s connState:%s", err, method, cc.Target(), cc.GetState().String())
		return nil, err
	}

	return &wrappedClientStream{
		ClientStream: s,
		method:       method,
		cc:           cc,
	}, nil
}

func unaryClientInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	if err != nil {
		err = convertClientError(err)
		logger.GrpcLogger.Errorf("do unary client error:%v for method:%s target:%s connState:%s", err, method, cc.Target(), cc.GetState().String())
	}

	return err
}

func convertClientError(err error) error {
	if err == nil {
		return nil
	}
	s := status.Convert(err)
	for _, d := range s.Details() {
		switch internal := d.(type) {
		case *base.GrpcDfError:
			return &dferrors.DfError{
				Code:    internal.Code,
				Message: internal.Message,
			}
		}
	}
	// grpc framework error
	return err
}

type RetryMeta struct {
	StreamTimes int     // times of replacing stream on the current client
	MaxAttempts int     // limit times for execute
	InitBackoff float64 // second
	MaxBackOff  float64 // second
}

func ExecuteWithRetry(f func() (interface{}, error), initBackoff float64, maxBackoff float64, maxAttempts int, cause error) (interface{}, error) {
	var res interface{}
	for i := 0; i < maxAttempts; i++ {
		if e, ok := cause.(*dferrors.DfError); ok {
			if e.Code != dfcodes.UnknownError {
				return res, cause
			}
		}

		if i > 0 {
			time.Sleep(mathutils.RandBackoff(initBackoff, maxBackoff, 2.0, i))
		}

		res, cause = f()
		if cause == nil {
			break
		}
	}

	return res, cause
}

func ExecuteWithRetryAndContext(ctx context.Context, f func() (data interface{}, err error), initBackoff float64, maxBackoff float64, maxAttempts int, cause error) (interface{}, error) {
	var (
		res interface{}
	)
	for i := 0; i < maxAttempts; i++ {
		if e, ok := cause.(*dferrors.DfError); ok {
			if e.Code != dfcodes.UnknownError {
				return res, cause
			}
		}

		if i > 0 {
			time.Sleep(mathutils.RandBackoff(initBackoff, maxBackoff, 2.0, i))
		}

		res, cause = f()
		if cause == nil {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}

	return res, cause
}
