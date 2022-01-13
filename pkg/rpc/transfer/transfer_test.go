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

package transfer

import (
	"google.golang.org/grpc/codes"
)

var (
	unTransferableErrors = []codes.Code{codes.Unavailable, codes.DataLoss}
)

//func TestTransferSuite(t *testing.T) {
//	service := &testService{}
//	unaryInterceptor := UnaryClientInterceptor(WithCodes(unTransferableErrors...))
//
//	streamInterceptor := StreamClientInterceptor(WithCodes(unTransferableErrors...))
//	s := &TransferSuite{
//		srv: service,
//		InterceptorTestSuite: &grpc_testing.InterceptorTestSuite{
//			TestService: service,
//			ClientOpts: []grpc.DialOption{
//				grpc.WithStreamInterceptor(streamInterceptor),
//				grpc.WithUnaryInterceptor(unaryInterceptor),
//			},
//		},
//	}
//	suite.Run(t, s)
//}
//
//type TransferSuite struct {
//	suite.Suite
//	client testpb.TestServiceClient
//	srv    *testService
//}
//
//func (s *TransferSuite) SetupTest() {
//	s.srv.resetFailingConfiguration( /* don't fail */ 0, codes.OK, noSleep)
//}
//
//func (s *TransferSuite) TestUnary_FailsOnNonRetriableError() {
//	startTestServers(s.T(), 3)
//	s.srv.resetFailingConfiguration(5, codes.Internal, noSleep)
//	_, err := s.client.EmptyCall(context.Background(), &testpb.Empty{})
//	require.Error(s.T(), err, "error must occur from the failing service")
//	require.Equal(s.T(), codes.Internal, status.Code(err), "failure code must come from retrier")
//	require.EqualValues(s.T(), 1, s.srv.requestCount(), "one request should have been made")
//}
//
//func (s *TransferSuite) TestUnary_FailsOnNonRetriableContextError() {
//	s.srv.resetFailingConfiguration(5, codes.Canceled, noSleep)
//	_, err := s.client.EmptyCall(s.SimpleCtx(), goodPing)
//	require.Error(s.T(), err, "error must occur from the failing service")
//	require.Equal(s.T(), codes.Canceled, status.Code(err), "failure code must come from retrier")
//	require.EqualValues(s.T(), 1, s.srv.requestCount(), "one request should have been made")
//}
