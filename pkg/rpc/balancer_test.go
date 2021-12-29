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
	"fmt"
	testpb "google.golang.org/grpc/test/grpc_testing"
	"log"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"
)

const (
	testPickKey = "balancer_test"
)

type balancerTester struct{}

func BalancerTest(t *testing.T) {
	RunSubTests(t, balancerTester{})
}

func RunSubTests(t *testing.T, x balancerTester) {
	xt := reflect.TypeOf(x)
	xv := reflect.ValueOf(x)

	for i := 0; i < xt.NumMethod(); i++ {
		methodName := xt.Method(i).Name
		if !strings.HasPrefix(methodName, "Test") {
			continue
		}
		tfunc := xv.MethodByName(methodName).Interface().(func(*testing.T))
		t.Run(strings.TrimPrefix(methodName, "Test"), func(t *testing.T) {
			tfunc(t)
		})
	}
}

type testServer struct {
	testpb.UnimplementedTestServiceServer

	testChan chan struct{}
}

func newTestServer() *testServer {
	// Each testServer is disposable.
	return &testServer{testChan: make(chan struct{}, 1)}
}

func (s *testServer) EmptyCall(ctx context.Context, in *testpb.Empty) (*testpb.Empty, error) {
	s.testChan <- struct{}{}
	return &testpb.Empty{}, nil
}

type testServerData struct {
	servers     []*grpc.Server
	serverImpls []*testServer
	addresses   []string
}

func (t *testServerData) cleanup() {
	for _, s := range t.servers {
		s.Stop()
	}
}

func startTestServers(count int) (_ *testServerData, err error) {
	t := &testServerData{}

	defer func() {
		if err != nil {
			t.cleanup()
		}
	}()
	for i := 0; i < count; i++ {
		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			return nil, fmt.Errorf("failed to listen %v", err)
		}

		s := grpc.NewServer()
		sImpl := newTestServer()
		testpb.RegisterTestServiceServer(s, sImpl)
		t.servers = append(t.servers, s)
		t.serverImpls = append(t.serverImpls, sImpl)
		t.addresses = append(t.addresses, lis.Addr().String())

		go func(s *grpc.Server, l net.Listener) {
			if err := s.Serve(l); err != nil {
				log.Fatalf("failed to serve %v", err)
			}
		}(s, lis)
	}

	return t, nil
}

func (balancerTester) TestOneBackend(t *testing.T) {
	r := manual.NewBuilderWithScheme("whatever")

	test, err := startTestServers(1)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}
	defer test.cleanup()

	cc, err := grpc.Dial(r.Scheme()+":///test.server", grpc.WithInsecure(), grpc.WithResolvers(r),
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy": "%s"}`, D7yBalancerPolicy)))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()

	testc := testpb.NewTestServiceClient(cc)

	// The first RPC should fail because there's no address.
	{
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := testc.EmptyCall(ctx, &testpb.Empty{}); err == nil || status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("EmptyCall() = _, %v, want _, DeadlineExceeded", err)
		}
	}

	r.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: test.addresses[0]}}})

	// The second RPC should succeed.
	{
		if _, err := testc.EmptyCall(context.Background(), &testpb.Empty{}); err != nil {
			t.Fatalf("EmptyCall() = _, %v, want _, <nil>", err)
		}
	}
}

func (balancerTester) TestMigration(t *testing.T) {
	r := manual.NewBuilderWithScheme("whatever")

	test, err := startTestServers(2)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}
	defer test.cleanup()

	cc, err := grpc.Dial(r.Scheme()+":///test.server", grpc.WithInsecure(), grpc.WithResolvers(r),
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy": "%s"}`, D7yBalancerPolicy)))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()

	r.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: test.addresses[0]}, {Addr: test.addresses[1]}}})
	testc := testpb.NewTestServiceClient(cc)

	// The first RPC should succeed.
	{
		ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), PickKey{}, &PickReq{Key: testPickKey, Attempt: 1}), 5*time.Second)
		defer cancel()
		if _, err := testc.EmptyCall(ctx, &testpb.Empty{}); err != nil {
			t.Fatalf("EmptyCall() = _, %v, want _, <nil>", err)
		}
	}

	// Because each testServer is disposable, the second RPC should fail.
	{
		ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), PickKey{}, &PickReq{Key: testPickKey, Attempt: 1}), 5*time.Second)
		defer cancel()
		if _, err := testc.EmptyCall(ctx, &testpb.Empty{}); err == nil || status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("EmptyCall() = _, %v, want _, DeadlineExceeded", err)
		}
	}

	// The third RPC change the Attempt in PickReq, so it should succeed.
	{
		ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), PickKey{}, &PickReq{Key: testPickKey, Attempt: 2}), 5*time.Second)
		defer cancel()
		if _, err := testc.EmptyCall(ctx, &testpb.Empty{}); err != nil {
			t.Fatalf("EmptyCall() = _, %v, want _, <nil>", err)
		}
	}
}
