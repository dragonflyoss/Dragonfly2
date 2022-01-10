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
	"d7y.io/dragonfly/v2/internal/dferrors"
	"d7y.io/dragonfly/v2/pkg/util/mathutils"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/semconv"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"time"
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

func ExecuteWithRetry(f func() (interface{}, error), initBackoff float64, maxBackoff float64, maxAttempts int, cause error) (interface{}, error) {
	var res interface{}
	for i := 0; i < maxAttempts; i++ {
		if _, ok := cause.(*dferrors.DfError); ok {
			return res, cause
		}
		if status.Code(cause) == codes.DeadlineExceeded || status.Code(cause) == codes.Canceled {
			return res, cause
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
