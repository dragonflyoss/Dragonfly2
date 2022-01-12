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
	"net"
	"testing"
	"time"

	"d7y.io/dragonfly/v2/pkg/rpc/pickreq"
	"google.golang.org/grpc/peer"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"
	testpb "google.golang.org/grpc/test/grpc_testing"
	"k8s.io/apimachinery/pkg/util/sets"
)

func errorDesc(err error) string {
	if s, ok := status.FromError(err); ok {
		return s.Message()
	}
	return err.Error()
}

type testServer struct {
	testpb.UnimplementedTestServiceServer
	addr     string
	testChan chan struct{}
}

func newTestServer(addr string) *testServer {
	// Each testServer is disposable.
	return &testServer{addr: addr, testChan: make(chan struct{}, 1)}
}

func (s *testServer) UnaryCall(ctx context.Context, in *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
	s.testChan <- struct{}{}
	return &testpb.SimpleResponse{
		Payload: &testpb.Payload{Body: []byte(s.addr)},
	}, nil
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

func startTestServers(t *testing.T, numServers int) (_ *testServerData, err error) {
	testData := &testServerData{}

	defer func() {
		if err != nil {
			testData.cleanup()
		}
	}()
	for i := 0; i < numServers; i++ {
		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			return nil, fmt.Errorf("failed to listen %v", err)
		}

		s := grpc.NewServer()
		sImpl := newTestServer(lis.Addr().String())
		testpb.RegisterTestServiceServer(s, sImpl)
		testData.servers = append(testData.servers, s)
		testData.serverImpls = append(testData.serverImpls, sImpl)
		testData.addresses = append(testData.addresses, lis.Addr().String())

		go func(s *grpc.Server, l net.Listener) {
			if err := s.Serve(l); err != nil {
				t.Fatalf("failed to serve %v", err)
			}
		}(s, lis)
	}

	return testData, nil
}

func TestOneBackend(t *testing.T) {
	r := manual.NewBuilderWithScheme("whatever")

	test, err := startTestServers(t, 1)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}
	defer test.cleanup()
	cc, err := grpc.Dial(r.Scheme()+":///test.server", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithResolvers(r),
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy": "%s"}`, D7yBalancerPolicy)),
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()

	testc := testpb.NewTestServiceClient(cc)

	// The first RPC should fail because there's no address.
	{
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := testc.UnaryCall(ctx, &testpb.SimpleRequest{}); err == nil || status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("UnaryCall() = _, %v, want _, DeadlineExceeded", err)
		}
	}

	r.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: test.addresses[0]}}})

	// The second RPC should succeed.
	{
		resp, err := testc.UnaryCall(context.Background(), &testpb.SimpleRequest{})
		if err != nil {
			t.Fatalf("UnaryCall() = _, %v, want _, <nil>", err)
		}
		if string(resp.Payload.GetBody()) != test.serverImpls[0].addr {
			t.Fatalf("UnaryCall() = _, %s, want _, %s", string(resp.Payload.GetBody()), test.serverImpls[0].addr)
		}
	}
}

func TestBackends(t *testing.T) {
	r := manual.NewBuilderWithScheme("whatever")

	test, err := startTestServers(t, 2)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}
	defer test.cleanup()

	cc, err := grpc.Dial(r.Scheme()+":///test.server", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithResolvers(r),
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy": "%s"}`, D7yBalancerPolicy)))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()

	r.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: test.addresses[0]}, {Addr: test.addresses[1]}}})
	testc := testpb.NewTestServiceClient(cc)

	// The first RPC should succeed.
	{
		ctx, cancel := context.WithTimeout(pickreq.NewContext(context.Background(), &pickreq.PickRequest{
			HashKey:     "testPickKey",
			FailedNodes: nil,
			IsStick:     false,
			TargetAddr:  test.addresses[0],
		}), time.Second)
		defer cancel()
		resp, err := testc.UnaryCall(ctx, &testpb.SimpleRequest{})
		if err != nil {
			t.Fatalf("UnaryCall() = _, %v, want _, <nil>", err)
		}
		if string(resp.Payload.GetBody()) != test.serverImpls[0].addr {
			t.Fatalf("UnaryCall() = _, %s, want _, %s", string(resp.Payload.GetBody()), test.serverImpls[0].addr)
		}
	}

	// Because each testServer is one shot, the second RPC should fail.
	{
		ctx, cancel := context.WithTimeout(pickreq.NewContext(context.Background(), &pickreq.PickRequest{
			HashKey:     "testPickKey",
			FailedNodes: nil,
			IsStick:     false,
			TargetAddr:  test.addresses[0],
		}), time.Second)
		defer cancel()
		if _, err := testc.UnaryCall(ctx, &testpb.SimpleRequest{}); err == nil || status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("UnaryCall() = _, %v, want _, DeadlineExceeded", err)
		}
	}
	// Because stick is true, the third RPC should fail.
	{
		ctx, cancel := context.WithTimeout(pickreq.NewContext(context.Background(), &pickreq.PickRequest{
			HashKey:     "testPickKey",
			FailedNodes: nil,
			IsStick:     true,
			TargetAddr:  "",
		}), time.Second)
		defer cancel()
		if _, err := testc.UnaryCall(ctx, &testpb.SimpleRequest{}); err == nil || status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("UnaryCall() = _, %v, want _, DeadlineExceeded", err)
		}
	}

	// The forth RPC change the targetAddr in PickReq, so it should succeed.
	{
		ctx, cancel := context.WithTimeout(pickreq.NewContext(context.Background(), &pickreq.PickRequest{
			HashKey:     "testPickKey",
			FailedNodes: sets.NewString(test.addresses[0]),
			IsStick:     false,
			TargetAddr:  "",
		}), time.Second)
		defer cancel()
		resp, err := testc.UnaryCall(ctx, &testpb.SimpleRequest{})
		if err != nil {
			t.Fatalf("UnaryCall() = _, %v, want _, <nil>", err)
		}
		if string(resp.Payload.GetBody()) != test.serverImpls[1].addr {
			t.Fatalf("UnaryCall() = _, %s, want _, %s", string(resp.Payload.GetBody()), test.serverImpls[0].addr)
		}
	}
}

func TestMigration(t *testing.T) {
	r := manual.NewBuilderWithScheme("whatever")

	test, err := startTestServers(t, 3)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}
	defer test.cleanup()

	cc, err := grpc.Dial(r.Scheme()+":///test.server", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithResolvers(r),
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy": "%s"}`, D7yBalancerPolicy)))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()

	r.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: test.addresses[0]}, {Addr: test.addresses[1]}}})
	testc := testpb.NewTestServiceClient(cc)

	// The first RPC should succeed.
	{
		ctx, cancel := context.WithTimeout(pickreq.NewContext(context.Background(), &pickreq.PickRequest{
			HashKey:     "testPickKey",
			FailedNodes: nil,
			IsStick:     false,
			TargetAddr:  test.addresses[1],
		}), time.Second)
		defer cancel()
		var temp peer.Peer
		if _, err := testc.UnaryCall(ctx, &testpb.SimpleRequest{}, grpc.Peer(&temp)); err != nil {
			t.Fatalf("UnaryCall() = _, %v, want _, <nil>", err)
		}
		if temp.Addr.String() != test.addresses[1] {
			t.Fatalf("UnaryCall() = _, %s, want _, %s", temp.Addr.String(), test.addresses[1])
		}
	}

	// Because each testServer is one shot, the second RPC should fail.
	{
		ctx, cancel := context.WithTimeout(pickreq.NewContext(context.Background(), &pickreq.PickRequest{
			HashKey:     "testPickKey",
			FailedNodes: nil,
			IsStick:     false,
			TargetAddr:  test.addresses[1],
		}), time.Second)
		defer cancel()
		var temp peer.Peer
		if _, err := testc.UnaryCall(ctx, &testpb.SimpleRequest{}, grpc.Peer(&temp)); err == nil || status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("EmptyCall() = _, %v, want _, DeadlineExceeded", err)
		}
		if temp.Addr.String() != test.addresses[1] {
			t.Fatalf("UnaryCall() = _, %s, want _, %s", temp.Addr.String(), test.addresses[1])
		}
	}

	// The third RPC change the targetAddr in PickReq, so it should succeed.
	{
		ctx, cancel := context.WithTimeout(pickreq.NewContext(context.Background(), &pickreq.PickRequest{
			HashKey:     "testPickKey",
			FailedNodes: sets.NewString(test.addresses[1]),
			IsStick:     false,
			TargetAddr:  "",
		}), time.Second)
		defer cancel()
		var temp peer.Peer
		if _, err := testc.UnaryCall(ctx, &testpb.SimpleRequest{}, grpc.Peer(&temp)); err != nil {
			t.Fatalf("UnaryCall() = _, %v, want _, <nil>", err)
		}
		if temp.Addr.String() != test.addresses[0] {
			t.Fatalf("UnaryCall() = _, %s, want _, %s", temp.Addr.String(), test.addresses[0])
		}
	}
	// The forth RPC should fail because all server nodes in FailedNodes, and
	{
		ctx, cancel := context.WithTimeout(pickreq.NewContext(context.Background(), &pickreq.PickRequest{
			HashKey:     "testPickKey",
			FailedNodes: sets.NewString(test.addresses[0], test.addresses[1]),
			IsStick:     true,
			TargetAddr:  "",
		}), time.Second)
		defer cancel()
		var temp peer.Peer
		if _, err := testc.UnaryCall(ctx, &testpb.SimpleRequest{}, grpc.Peer(&temp)); err == nil || errorDesc(err) != fmt.Sprintf("stick targetAddr %s is in failedNodes: %s", test.addresses[0], sets.NewString(test.addresses[0], test.addresses[1])) {
			t.Fatalf("UnaryCall() = _, %v, want _, <nil>", err)
		}
	}
}
