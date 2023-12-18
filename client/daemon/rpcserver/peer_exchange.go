/*
 *     Copyright 2023 The Dragonfly Authors
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
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	dfdaemonv1 "d7y.io/api/v2/pkg/apis/dfdaemon/v1"
	"d7y.io/dragonfly/v2/client/daemon/pex"
	logger "d7y.io/dragonfly/v2/internal/dflog"
)

func (s *server) PeerExchange(exchangeServer dfdaemonv1.Daemon_PeerExchangeServer) error {
	p, ok := peer.FromContext(exchangeServer.Context())
	if !ok {
		logger.Errorf("peer info not found")
		return status.Error(codes.InvalidArgument, "peer info not found")
	}

	log := logger.With("member", p.Addr.String(), "grpcCall", "PeerExchange")

	ip := p.Addr.String()
	member, err := s.peerExchangeMember.FindMember(ip)
	if errors.Is(err, pex.ErrNotFound) {
		log.Errorf("member not found")
		return status.Error(codes.NotFound, "member not found")
	}
	if err != nil {
		log.Errorf("failed to extract peer info: %s", err)
		return status.Errorf(codes.InvalidArgument, "failed to extract peer info: %s", err)
	}

	err = s.peerExchangeMember.Register(ip,
		pex.NewPeerMetadataSendReceiveCloser(
			exchangeServer,
			func() error {
				return nil
			},
		),
	)
	if errors.Is(err, pex.ErrIsAlreadyExists) {
		log.Debugf("member is already exist")
		return status.Error(codes.AlreadyExists, err.Error())
	}

	defer s.peerExchangeMember.UnRegister(ip)

	if err != nil {
		log.Debugf("failed to register member: %s", err)
		return status.Errorf(codes.Internal, "failed to register member: %s", err)
	}

	var peerMetadata *dfdaemonv1.PeerMetadata
	for {
		peerMetadata, err = exchangeServer.Recv()
		if err != nil {
			log.Debugf("failed to receive peer metadata: %s", err)
			return err
		}
		s.peerExchangeSync.Sync(member, peerMetadata)
	}
}
