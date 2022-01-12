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

package pickreq

import (
	"context"

	"k8s.io/apimachinery/pkg/util/sets"
)

// PickRequest contains the clues to pick subConn
type PickRequest struct {
	HashKey     string
	FailedNodes sets.String
	IsStick     bool
	TargetAddr  string
}

type pickKey struct{}

// NewContext creates a new context with pick clues attached.
func NewContext(ctx context.Context, p *PickRequest) context.Context {
	return context.WithValue(ctx, pickKey{}, p)
}

// FromContext returns the pickReq clues in ctx if it exists.
func FromContext(ctx context.Context) (p *PickRequest, ok bool) {
	p, ok = ctx.Value(pickKey{}).(*PickRequest)
	return
}
