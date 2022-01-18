/*
*
* Copyright 2020 gRPC authors.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
 */

package rpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/sets"

	"d7y.io/dragonfly/v2/pkg/rpc/pickreq"
)

// testSubConn contains a list of SubConns to be used in tests.
var testSubConns []*testSubConn

const (
	testSubConnsCount       = 16
	defaultTestShortTimeout = 10 * time.Millisecond
)

func init() {
	for i := 0; i < testSubConnsCount; i++ {
		testSubConns = append(testSubConns, &testSubConn{
			id:        fmt.Sprintf("sc%d", i),
			ConnectCh: make(chan struct{}, 1),
		})
	}
}

// testSubConn implements the SubConn interface, to be used in tests.
type testSubConn struct {
	id        string
	ConnectCh chan struct{}
}

// UpdateAddresses is a no-op.
func (tsc *testSubConn) UpdateAddresses([]resolver.Address) {}

// Connect is a no-op.
func (tsc *testSubConn) Connect() {
	select {
	case tsc.ConnectCh <- struct{}{}:
	default:
	}
}

// String implements stringer to print human friendly error message.
func (tsc *testSubConn) String() string {
	return tsc.id
}

func newTestPicker(cStats []connectivity.State, pickHistory map[string]string) balancer.Picker {
	var subConns = make(map[resolver.Address]balancer.SubConn)
	var scStates = make(map[string]connectivity.State)
	var info = d7yPickerBuildInfo{
		subConns:    subConns,
		scStates:    scStates,
		pickHistory: pickHistory,
	}
	for i, st := range cStats {
		testSC := testSubConns[i]
		subConns[resolver.Address{Addr: testSC.String()}] = testSC
		scStates[testSC.String()] = st
	}
	return newD7yPicker(info)
}

func TestPickerPickFirstTwo(t *testing.T) {
	tests := []struct {
		name            string
		picker          balancer.Picker
		pickReq         *pickreq.PickRequest
		wantSC          balancer.SubConn
		wantErr         error
		wantSCToConnect balancer.SubConn
	}{
		{
			name:   "pick specified target success",
			picker: newTestPicker([]connectivity.State{connectivity.Ready, connectivity.Idle}, map[string]string{}),
			pickReq: &pickreq.PickRequest{
				HashKey:     "",
				FailedNodes: nil,
				IsStick:     false,
				TargetAddr:  testSubConns[0].String(),
			},
			wantSC: testSubConns[0],
		},
		{
			name:   "pick specified target not found",
			picker: newTestPicker([]connectivity.State{connectivity.Ready, connectivity.Idle}, map[string]string{}),
			pickReq: &pickreq.PickRequest{
				HashKey:     "",
				FailedNodes: nil,
				IsStick:     false,
				TargetAddr:  "unknown",
			},
			wantErr: status.Error(codes.FailedPrecondition, "cannot find target addr unknown"),
		},
		{
			name: "pick stick is true and success",
			picker: newTestPicker([]connectivity.State{connectivity.Connecting, connectivity.Idle}, map[string]string{
				"test1": testSubConns[2].String(),
			}),
			pickReq: &pickreq.PickRequest{
				HashKey:     "test1",
				FailedNodes: nil,
				IsStick:     true,
				TargetAddr:  "",
			},
			wantSC: testSubConns[2],
		},
		{
			name:   "pick stick is true and not found hashKey in history",
			picker: newTestPicker([]connectivity.State{connectivity.Connecting, connectivity.Idle}, map[string]string{}),
			pickReq: &pickreq.PickRequest{
				HashKey:     "test1",
				FailedNodes: nil,
				IsStick:     true,
				TargetAddr:  "",
			},
			wantErr: status.Errorf(codes.FailedPrecondition, "rpc call is required to stick to hashKey %s, but cannot find it in pick history", "test1"),
		},
		{
			name:   "pick stick is true and not specify hashKey",
			picker: newTestPicker([]connectivity.State{connectivity.Connecting, connectivity.Idle}, map[string]string{}),
			pickReq: &pickreq.PickRequest{
				HashKey:     "",
				FailedNodes: nil,
				IsStick:     true,
				TargetAddr:  "",
			},
			wantErr: status.Errorf(codes.FailedPrecondition, "rpc call is required to stick to hashKey, but hashKey is empty"),
		},
		{
			name:   "pick result filter out failedNodes",
			picker: newTestPicker([]connectivity.State{connectivity.Connecting, connectivity.Idle}, map[string]string{}),
			pickReq: &pickreq.PickRequest{
				HashKey:     "",
				FailedNodes: sets.NewString(testSubConns[0].String()),
				IsStick:     false,
				TargetAddr:  "",
			},
			wantSC: testSubConns[1],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.picker.Pick(balancer.PickInfo{
				Ctx: pickreq.NewContext(context.Background(), tt.pickReq),
			})
			if (err != nil || tt.wantErr != nil) && err.Error() != tt.wantErr.Error() {
				t.Errorf("Pick() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got.SubConn != tt.wantSC {
				t.Errorf("Pick() got = %v, want picked SubConn: %v", got, tt.wantSC)
			}
			if sc := tt.wantSCToConnect; sc != nil {
				select {
				case <-sc.(*testSubConn).ConnectCh:
				case <-time.After(defaultTestShortTimeout):
					t.Errorf("timeout waiting for Connect() from SubConn %v", sc)
				}
			}
		})
	}
}
