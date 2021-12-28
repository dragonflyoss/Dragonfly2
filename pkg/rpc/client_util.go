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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/semconv"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
)

type messageType attribute.KeyValue

var (
	messageSent     = messageType(attribute.Key("message.type").String("request"))
	messageReceived = messageType(attribute.Key("message.type").String("response"))
)

func (m messageType) Event(ctx context.Context, id int, message interface{}) {
	span := trace.SpanFromContext(ctx)
	if p, ok := message.(proto.Message); ok {
		content, _ := proto.Marshal(p)
		span.AddEvent("message", trace.WithAttributes(
			attribute.KeyValue(m),
			semconv.RPCMessageIDKey.Int(id),
			semconv.RPCMessageUncompressedSizeKey.String(string(content)),
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

func (w *wrappedClientStream) RecvMsg(m interface{}) error {
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

func (w *wrappedClientStream) SendMsg(m interface{}) error {
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

func unaryClientInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
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
	StreamTimes int // times of replacing stream on the current client
	MaxAttempts int // limit times for execute
}
