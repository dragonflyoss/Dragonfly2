/*
 *     Copyright 2022 The Dragonfly Authors
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
	"strconv"
	"strings"

	"github.com/mdlayher/vsock"
	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/pkg/dfnet"
)

// VsockDialer is the dialer for vsock, it expects `address` to be in dfnet.NetAddr.GetEndpoint()
// format, that is "vsock://cid:port"
func VsockDialer(_ctx context.Context, address string) (net.Conn, error) {
	addrStr := strings.TrimPrefix(address, dfnet.VsockEndpointPrefix)
	addr := strings.Split(addrStr, ":")
	if len(addr) != 2 {
		return nil, fmt.Errorf("invalid vsock address (%s), expected %scid:port", address, dfnet.VsockEndpointPrefix)
	}

	cid, err := strconv.ParseUint(addr[0], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to convert %q to vsock cid: %w", addr[0], err)
	}
	port, err := strconv.ParseUint(addr[1], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to convert %q to vsock port: %w", addr[1], err)
	}

	conn, err := vsock.Dial(uint32(cid), uint32(port), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to dial vsock %v:%v, address %s: %w", uint32(cid), uint32(port), address, err)
	}
	return conn, nil
}

// If `addrs` are all vsock addresses, add rpc.VsockDialer to DialOption, and return error if addrs
// have mixed vsock and other connection types.
func VsockDialerOption(addrs []dfnet.NetAddr, opts []grpc.DialOption) ([]grpc.DialOption, error) {
	var prevType dfnet.NetworkType
	hasVsock := false
	for n, a := range addrs {
		if a.Type != dfnet.VSOCK {
			prevType = a.Type
			continue
		}
		hasVsock = true
		if n > 0 && prevType != dfnet.VSOCK {
			return nil, fmt.Errorf("addrs(%v) have mixed vsock and other types", addrs)
		}
		prevType = a.Type
	}
	dialOpts := opts
	if hasVsock {
		dialOpts = append(opts, grpc.WithContextDialer(VsockDialer))
	}
	return dialOpts, nil
}
