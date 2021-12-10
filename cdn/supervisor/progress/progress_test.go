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

package progress

import (
	"container/list"
	"context"
	"reflect"
	"sync"
	"testing"

	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	"go.uber.org/atomic"
)

func Test_newProgressPublisher(t *testing.T) {
	type args struct {
		taskID string
	}
	tests := []struct {
		name string
		args args
		want *publisher
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newProgressPublisher(tt.args.taskID); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newProgressPublisher() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newProgressSubscriber(t *testing.T) {
	type args struct {
		ctx        context.Context
		clientAddr string
		taskID     string
		taskPieces map[uint32]*task.PieceInfo
	}
	tests := []struct {
		name string
		args args
		want *subscriber
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newProgressSubscriber(tt.args.ctx, tt.args.clientAddr, tt.args.taskID, tt.args.taskPieces); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newProgressSubscriber() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_publisher_AddSubscriber(t *testing.T) {
	type fields struct {
		taskID      string
		subscribers *list.List
	}
	type args struct {
		sub *subscriber
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pub := &publisher{
				taskID:      tt.fields.taskID,
				subscribers: tt.fields.subscribers,
			}
		})
	}
}

func Test_publisher_NotifySubscribers(t *testing.T) {
	type fields struct {
		taskID      string
		subscribers *list.List
	}
	type args struct {
		seedPiece *task.PieceInfo
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pub := &publisher{
				taskID:      tt.fields.taskID,
				subscribers: tt.fields.subscribers,
			}
		})
	}
}

func Test_publisher_RemoveAllSubscribers(t *testing.T) {
	type fields struct {
		taskID      string
		subscribers *list.List
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pub := &publisher{
				taskID:      tt.fields.taskID,
				subscribers: tt.fields.subscribers,
			}
		})
	}
}

func Test_publisher_RemoveSubscriber(t *testing.T) {
	type fields struct {
		taskID      string
		subscribers *list.List
	}
	type args struct {
		sub *subscriber
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pub := &publisher{
				taskID:      tt.fields.taskID,
				subscribers: tt.fields.subscribers,
			}
		})
	}
}

func Test_subscriber_Close(t *testing.T) {
	type fields struct {
		ctx       context.Context
		scheduler string
		taskID    string
		done      chan struct{}
		once      sync.Once
		pieces    map[uint32]*task.PieceInfo
		pieceChan chan *task.PieceInfo
		cond      *sync.Cond
		closed    *atomic.Bool
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sub := &subscriber{
				ctx:       tt.fields.ctx,
				scheduler: tt.fields.scheduler,
				taskID:    tt.fields.taskID,
				done:      tt.fields.done,
				once:      tt.fields.once,
				pieces:    tt.fields.pieces,
				pieceChan: tt.fields.pieceChan,
				cond:      tt.fields.cond,
				closed:    tt.fields.closed,
			}
		})
	}
}

func Test_subscriber_Notify(t *testing.T) {
	type fields struct {
		ctx       context.Context
		scheduler string
		taskID    string
		done      chan struct{}
		once      sync.Once
		pieces    map[uint32]*task.PieceInfo
		pieceChan chan *task.PieceInfo
		cond      *sync.Cond
		closed    *atomic.Bool
	}
	type args struct {
		seedPiece *task.PieceInfo
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sub := &subscriber{
				ctx:       tt.fields.ctx,
				scheduler: tt.fields.scheduler,
				taskID:    tt.fields.taskID,
				done:      tt.fields.done,
				once:      tt.fields.once,
				pieces:    tt.fields.pieces,
				pieceChan: tt.fields.pieceChan,
				cond:      tt.fields.cond,
				closed:    tt.fields.closed,
			}
		})
	}
}

func Test_subscriber_Receiver(t *testing.T) {
	type fields struct {
		ctx       context.Context
		scheduler string
		taskID    string
		done      chan struct{}
		once      sync.Once
		pieces    map[uint32]*task.PieceInfo
		pieceChan chan *task.PieceInfo
		cond      *sync.Cond
		closed    *atomic.Bool
	}
	tests := []struct {
		name   string
		fields fields
		want   <-chan *task.PieceInfo
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sub := &subscriber{
				ctx:       tt.fields.ctx,
				scheduler: tt.fields.scheduler,
				taskID:    tt.fields.taskID,
				done:      tt.fields.done,
				once:      tt.fields.once,
				pieces:    tt.fields.pieces,
				pieceChan: tt.fields.pieceChan,
				cond:      tt.fields.cond,
				closed:    tt.fields.closed,
			}
			if got := sub.Receiver(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Receiver() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_subscriber_readLoop(t *testing.T) {
	type fields struct {
		ctx       context.Context
		scheduler string
		taskID    string
		done      chan struct{}
		once      sync.Once
		pieces    map[uint32]*task.PieceInfo
		pieceChan chan *task.PieceInfo
		cond      *sync.Cond
		closed    *atomic.Bool
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sub := &subscriber{
				ctx:       tt.fields.ctx,
				scheduler: tt.fields.scheduler,
				taskID:    tt.fields.taskID,
				done:      tt.fields.done,
				once:      tt.fields.once,
				pieces:    tt.fields.pieces,
				pieceChan: tt.fields.pieceChan,
				cond:      tt.fields.cond,
				closed:    tt.fields.closed,
			}
		})
	}
}
