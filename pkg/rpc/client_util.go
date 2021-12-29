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
	"time"

	"d7y.io/dragonfly/v2/internal/dferrors"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/semconv"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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

func TryMigrate(ctx context.Context, cause error) error {
	logger.GrpcLogger.With("conn", conn.name).Infof("start try migrate server node for key %s, cause err: %v", key, cause)
	if status.Code(cause) == codes.DeadlineExceeded || status.Code(cause) == codes.Canceled {
		logger.GrpcLogger.With("conn", conn.name).Infof("migrate server node for key %s failed, cause err: %v", key, cause)
		return cause
	}
	// TODO recover findCandidateClientConn error
	if e, ok := cause.(*dferrors.DfError); ok {
		if e.Code != base.Code_ResourceLacked {
			return "", cause
		}
	}
	currentNode := ""
	conn.rwMutex.RLock()
	if node, ok := conn.key2NodeMap.Load(key); ok {
		currentNode = node.(string)
		preNode = currentNode
		exclusiveNodes = append(exclusiveNodes, preNode)
	}
	conn.rwMutex.RUnlock()
	conn.rwMutex.Lock()
	defer conn.rwMutex.Unlock()
	client, err := conn.findCandidateClientConn(key, sets.NewString(exclusiveNodes...))
	if err != nil {
		return "", errors.Wrapf(err, "find candidate client conn for hash key %s", key)
	}
	logger.GrpcLogger.With("conn", conn.name).Infof("successfully migrate hash key %s from server node %s to %s", key, currentNode, client.node)
	conn.key2NodeMap.Store(key, client.node)
	conn.node2ClientMap.Store(client.node, client.Ref)
	conn.accessNodeMap.Store(client.node, time.Now())
	return
}
