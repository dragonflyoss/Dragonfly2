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

package client

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	cdnclient "d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/client"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

func GetPieceTasks(ctx context.Context,
	destPeer *scheduler.PeerPacket_DestPeer,
	ptr *base.PieceTaskRequest,
	opts ...grpc.CallOption) (*base.PiecePacket, error) {
	destAddr := fmt.Sprintf("%s:%d", destPeer.Ip, destPeer.RpcPort)
	peerID := destPeer.PeerId
	toCdn := strings.HasSuffix(peerID, common.CdnSuffix)
	var err error
	netAddr := dfnet.NetAddr{
		Type: dfnet.TCP,
		Addr: destAddr,
	}
	client, err := getClient(netAddr, toCdn)
	if err != nil {
		return nil, err
	}
	if toCdn {
		return client.(cdnclient.CdnClient).GetPieceTasks(ctx, netAddr, ptr, opts...)
	}
	return client.(DaemonClient).GetPieceTasks(ctx, netAddr, ptr, opts...)
}

func SyncPieceTasks(ctx context.Context,
	destPeer *scheduler.PeerPacket_DestPeer,
	ptr *base.PieceTaskRequest,
	opts ...grpc.CallOption) (dfdaemon.Daemon_SyncPieceTasksClient, error) {
	destAddr := fmt.Sprintf("%s:%d", destPeer.Ip, destPeer.RpcPort)
	peerID := destPeer.PeerId
	toCdn := strings.HasSuffix(peerID, common.CdnSuffix)
	var err error
	netAddr := dfnet.NetAddr{
		Type: dfnet.TCP,
		Addr: destAddr,
	}
	client, err := getClient(netAddr, toCdn)
	if err != nil {
		return nil, err
	}
	if toCdn {
		return client.(cdnclient.CdnClient).SyncPieceTasks(ctx, netAddr, ptr, opts...)
	}
	return client.(DaemonClient).SyncPieceTasks(ctx, netAddr, ptr, opts...)
}

func getClient(netAddr dfnet.NetAddr, toCdn bool) (rpc.Closer, error) {
	if toCdn {
		return cdnclient.GetElasticClientByAddrs([]dfnet.NetAddr{netAddr})
	}
	return GetElasticClientByAddrs([]dfnet.NetAddr{netAddr})
}
