/*
 *     Copyright 2024 The Dragonfly Authors
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

package rpcserver

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dfdaemonv1 "d7y.io/api/v2/pkg/apis/dfdaemon/v1"
)

func (s *server) PeerExchange(exchangeServer dfdaemonv1.Daemon_PeerExchangeServer) error {
	if s.peerExchanger == nil {
		return status.New(codes.Unavailable, "peer exchange is disabled").Err()
	}
	return s.peerExchanger.PeerExchange(exchangeServer)
}
