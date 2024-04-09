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

package pex

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	dfdaemonv1 "d7y.io/api/v2/pkg/apis/dfdaemon/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/retry"
)

func (p *peerExchange) PeerExchange(exchangeServer dfdaemonv1.Daemon_PeerExchangeServer) error {
	ctx := exchangeServer.Context()
	pr, ok := peer.FromContext(ctx)
	if !ok {
		logger.Errorf("grpc peer info not found")
		return status.Error(codes.InvalidArgument, "grpc peer info not found")
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		logger.Errorf("grpc metadata not found")
		return status.Error(codes.InvalidArgument, "grpc metadata not found")
	}

	hostIDs := md.Get(GRPCMetadataHostID)
	if len(hostIDs) < 1 {
		logger.Errorf("grpc metadata HostID not found")
		return status.Error(codes.InvalidArgument, "grpc metadata HostID not found")
	}

	hostID := hostIDs[0]
	log := logger.With("member", pr.Addr.String(), "hostID", p.localMember.HostID, "targetHostID", hostID, "grpcCall", "PeerExchange")

	// try to find member
	var member *MemberMeta
	_, _, err := retry.Run(ctx, 0.01, 0.1, 10, func() (data any, cancel bool, err error) {
		member, err = p.PeerExchangeMember().FindMember(hostID)
		if errors.Is(err, ErrNotFound) {
			log.Errorf("%s member not found, retry later", hostID)
			select {
			case <-ctx.Done():
				cancel = true
			default:
			}
			return member, cancel, status.Error(codes.NotFound, "member not found")
		}
		return member, cancel, nil
	})

	if err != nil {
		log.Errorf("failed to extract peer info: %s", err)
		return status.Errorf(codes.InvalidArgument, "failed to extract peer info: %s", err)
	}

	err = p.PeerExchangeMember().Register(member,
		NewPeerMetadataSendReceiveCloser(
			exchangeServer,
			func() error {
				return nil
			},
		),
	)

	if IsErrAlreadyExists(err) {
		log.Infof("member is already exist")
		return status.Error(codes.AlreadyExists, err.Error())
	}

	if err != nil {
		log.Debugf("failed to register member: %s", err)
		return status.Errorf(codes.Internal, "failed to register member: %s", err)
	}

	defer p.PeerExchangeMember().UnRegister(member)

	log.Infof("receive connection from %s/%s, start receive peer metadata, member metadata: %#v",
		member.HostID, member.IP, member)

	// TODO send exist peers

	var data *dfdaemonv1.PeerExchangeData
	for {
		data, err = exchangeServer.Recv()
		if err != nil {
			if !IsErrAlreadyExists(err) {
				log.Errorf("failed to receive peer metadata: %s, member: %s, local host id: %s",
					err, member.HostID, p.localMember.HostID)
			}
			return err
		}
		p.PeerExchangeSynchronizer().Sync(member, data)
	}
}
