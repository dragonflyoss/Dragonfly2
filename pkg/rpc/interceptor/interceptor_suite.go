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

package interceptor

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	testpb "google.golang.org/grpc/test/grpc_testing"
)

type testService struct {
	testpb.UnimplementedTestServiceServer
	mu sync.Mutex

	reqCounter uint
	reqModulo  uint
	reqSleep   time.Duration
	reqError   codes.Code
}

func newTestServer(addr string) *testService {
	// Each testServer is disposable.
	return &testService{}
}

func (s *testService) resetFailingConfiguration(modulo uint, errorCode codes.Code, sleepTime time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.reqCounter = 0
	s.reqModulo = modulo
	s.reqError = errorCode
	s.reqSleep = sleepTime
}

func (s *testService) requestCount() uint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reqCounter
}

func (s *testService) maybeFailRequest() error {
	s.mu.Lock()
	s.reqCounter++
	reqModulo := s.reqModulo
	reqCounter := s.reqCounter
	reqSleep := s.reqSleep
	reqError := s.reqError
	s.mu.Unlock()
	if (reqModulo > 0) && (reqCounter%reqModulo == 0) {
		return nil
	}
	time.Sleep(reqSleep)
	return status.Errorf(reqError, "maybeFailRequest: failing it")
}

func (s *testService) EmptyCall(ctx context.Context, empty *testpb.Empty) (*testpb.Empty, error) {
	if err := s.maybeFailRequest(); err != nil {
		return nil, err
	}
	return &testpb.Empty{}, nil
}

func (s *testService) StreamingOutputCall(in *testpb.StreamingOutputCallRequest, stream testpb.TestService_StreamingOutputCallServer) error {
	if err := s.maybeFailRequest(); err != nil {
		return err
	}
	for i := 0; i < 100; i++ {
		stream.Send(&testpb.StreamingOutputCallResponse{})
	}
	return nil
}

func (s *testService) FullDuplexCall(stream testpb.TestService_FullDuplexCallServer) error {
	if err := s.maybeFailRequest(); err != nil {
		return err
	}
	count := 0
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		stream.Send(&testpb.StreamingOutputCallResponse{
			Payload: req.Payload,
		})
		count++
	}
	return nil
}

type testServerData struct {
	servers     []*grpc.Server
	serverImpls []*testService
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
				log.Fatalf("failed to serve %v", err)
			}
		}(s, lis)
	}

	return testData, nil
}
