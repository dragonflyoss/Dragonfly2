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
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/mdlayher/vsock"

	"d7y.io/dragonfly/v2/pkg/dfnet"
)

// VsockDialer is the dialer for vsock.
func VsockDialer(ctx context.Context, address string) (net.Conn, error) {
	u, err := url.Parse(address)
	if err != nil {
		return nil, err
	}

	cid, err := strconv.ParseUint(u.Hostname(), 10, 32)
	if err != nil {
		return nil, err
	}

	port, err := strconv.ParseUint(u.Port(), 10, 32)
	if err != nil {
		return nil, err
	}

	conn, err := vsock.Dial(uint32(cid), uint32(port), nil)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// IsVsock returns whether the address is vsock.
func IsVsock(target string) bool {
	return strings.HasPrefix(target, string(dfnet.VSOCK))
}
