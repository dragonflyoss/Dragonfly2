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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/math"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
)

const (
	// Identifier of message transmitted or received.
	RPCMessageIDKey = attribute.Key("message.id")

	// The uncompressed size of the message transmitted or received in
	// bytes.
	RPCMessageUncompressedSizeKey = attribute.Key("message.uncompressed_size")
)

func (conn *Connection) startGC() {
	// TODO 从hashing环中删除频繁失败的节点
	logger.GrpcLogger.With("conn", conn.name).Debugf("start the gc connections job")
	// execute the GC by fixed delay
	ticker := time.NewTicker(conn.gcConnInterval)
	for {
		select {
		case <-conn.ctx.Done():
			logger.GrpcLogger.With("conn", conn.name).Info("conn close, exit gc")
			return
		case <-ticker.C:
			removedConnCount := 0
			totalNodeSize := 0
			startTime := time.Now()

			// TODO use anther locker, @santong
			conn.rwMutex.Lock()
			// range all connections and determine whether they are expired
			conn.accessNodeMap.Range(func(node, accessTime any) bool {
				serverNode := node.(string)
				totalNodeSize++
				atime := accessTime.(time.Time)
				if conn.connExpireTime == 0 || time.Since(atime) < conn.connExpireTime {
					return true
				}
				conn.gcConn(serverNode)
				removedConnCount++
				return true
			})
			// TODO use anther locker, @santong
			conn.rwMutex.Unlock()
			// slow GC detected, report it with a log warning
			if timeElapse := time.Since(startTime); timeElapse > conn.gcConnTimeout {
				logger.GrpcLogger.With("conn", conn.name).Warnf("gc %d conns, cost: %.3f seconds", removedConnCount, timeElapse.Seconds())
			}
			actualTotal := 0
			conn.node2ClientMap.Range(func(key, value any) bool {
				if value != nil {
					actualTotal++
				}
				return true
			})
			logger.GrpcLogger.With("conn", conn.name).Infof("successfully gc clientConn count(%d), remainder count(%d), actualTotalConnCount(%d)",
				removedConnCount, totalNodeSize-removedConnCount, actualTotal)
		}
	}
}

// gcConn gc keys and clients associated with server node
func (conn *Connection) gcConn(node string) {
	logger.GrpcLogger.With("conn", conn.name).Infof("gc keys and clients associated with server node: %s starting", node)
	conn.node2ClientMap.Delete(node)
	logger.GrpcLogger.With("conn", conn.name).Infof("success gc clientConn: %s", node)
	// gc hash keys
	conn.key2NodeMap.Range(func(key, value any) bool {
		if value == node {
			conn.key2NodeMap.Delete(key)
			logger.GrpcLogger.With("conn", conn.name).Infof("success gc key: %s associated with server node %s", key, node)
		}
		return true
	})
	conn.accessNodeMap.Delete(node)
	logger.GrpcLogger.With("conn", conn.name).Infof("gc keys and clients associated with server node: %s ending", node)
}

type messageType attribute.KeyValue

var (
	messageSent     = messageType(attribute.Key("message.type").String("request"))
	messageReceived = messageType(attribute.Key("message.type").String("response"))
)

func (m messageType) Event(ctx context.Context, id int, message any) {
	span := trace.SpanFromContext(ctx)
	if p, ok := message.(proto.Message); ok {
		content, _ := proto.Marshal(p)
		span.AddEvent("message", trace.WithAttributes(
			attribute.KeyValue(m),
			RPCMessageIDKey.Int(id),
			RPCMessageUncompressedSizeKey.String(string(content)),
		))
	}
}

type wrappedClientStream struct {
	grpc.ClientStream
	method string
	cc     *grpc.ClientConn

	receivedMessageID int
	sentMessageID     int
}

func (w *wrappedClientStream) RecvMsg(m any) error {
	err := w.ClientStream.RecvMsg(m)
	if err != nil && err != io.EOF {
		err = convertClientError(err)
		logger.GrpcLogger.Errorf("client receive a message: %T error: %v for method: %s target: %s connState: %s", m, err, w.method, w.cc.Target(), w.cc.GetState().String())
	}
	if err == nil {
		w.receivedMessageID++
		messageReceived.Event(w.Context(), w.receivedMessageID, m)
	}
	return err
}

func (w *wrappedClientStream) SendMsg(m any) error {
	err := w.ClientStream.SendMsg(m)
	w.sentMessageID++
	messageSent.Event(w.Context(), w.sentMessageID, m)
	if err != nil && err != io.EOF {
		logger.GrpcLogger.Errorf("client send a message: %T error: %v for method: %s target: %s connState: %s", m, err, w.method, w.cc.Target(), w.cc.GetState().String())
	}

	return err
}

func streamClientInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	s, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		err = convertClientError(err)
		logger.GrpcLogger.Errorf("create client stream error: %v for method: %s target: %s connState: %s", err, method, cc.Target(), cc.GetState().String())
		return nil, err
	}

	return &wrappedClientStream{
		ClientStream: s,
		method:       method,
		cc:           cc,
	}, nil
}

func unaryClientInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	messageSent.Event(ctx, 1, req)
	err := invoker(ctx, method, req, reply, cc, opts...)

	messageReceived.Event(ctx, 1, reply)
	if err != nil {
		err = convertClientError(err)
		logger.GrpcLogger.Errorf("do unary client error: %v for method: %s target: %s connState: %s", err, method, cc.Target(), cc.GetState().String())
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

func ExecuteWithRetry(f func() (any, error), initBackoff float64, maxBackoff float64, maxAttempts int, cause error) (any, error) {
	var res any
	for i := 0; i < maxAttempts; i++ {
		if _, ok := cause.(*dferrors.DfError); ok {
			return res, cause
		}
		if status.Code(cause) == codes.DeadlineExceeded || status.Code(cause) == codes.Canceled {
			return res, cause
		}
		if i > 0 {
			time.Sleep(math.RandBackoffSeconds(initBackoff, maxBackoff, 2.0, i))
		}

		res, cause = f()
		if cause == nil {
			break
		}
	}

	return res, cause
}
