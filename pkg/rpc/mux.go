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
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"

	"github.com/soheilhy/cmux"
	"google.golang.org/grpc/credentials"
)

const (
	// cmux's TLS matcher read at least 3 + 1 bytes
	tlsRecordPrefix = 4
)

type muxTransportCredentials struct {
	credentials credentials.TransportCredentials
	tlsMatcher  cmux.Matcher
	tlsPrefer   bool
}

func WithTLSPreferClientHandshake(prefer bool) func(m *muxTransportCredentials) {
	return func(m *muxTransportCredentials) {
		m.tlsPrefer = prefer
	}
}

func NewMuxTransportCredentials(c *tls.Config, opts ...func(m *muxTransportCredentials)) credentials.TransportCredentials {
	m := &muxTransportCredentials{
		tlsMatcher:  cmux.TLS(),
		credentials: credentials.NewTLS(c),
	}

	for _, opt := range opts {
		opt(m)
	}

	return m
}

func (m *muxTransportCredentials) ClientHandshake(ctx context.Context, s string, conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	if m.tlsPrefer {
		return m.credentials.ClientHandshake(ctx, s, conn)
	}

	return conn, info{credentials.CommonAuthInfo{SecurityLevel: credentials.NoSecurity}}, nil
}

func (m *muxTransportCredentials) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	var prefix = make([]byte, tlsRecordPrefix)

	n, err := conn.Read(prefix)
	if err != nil {
		return nil, nil, err
	}

	if n != tlsRecordPrefix {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("short read handshake")
	}

	conn = &muxConn{
		Conn: conn,
		buf:  prefix,
	}

	// tls
	if m.tlsMatcher(bytes.NewReader(prefix)) {
		return m.credentials.ServerHandshake(conn)
	}

	// non-tls
	return conn, info{credentials.CommonAuthInfo{SecurityLevel: credentials.NoSecurity}}, nil
}

func (m *muxTransportCredentials) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{
		ProtocolVersion:  "",
		SecurityProtocol: "mux",
		ServerName:       "",
	}
}

func (m *muxTransportCredentials) Clone() credentials.TransportCredentials {
	return &muxTransportCredentials{
		tlsMatcher:  cmux.TLS(),
		credentials: m.credentials.Clone(),
	}
}

func (m *muxTransportCredentials) OverrideServerName(s string) error {
	return m.credentials.OverrideServerName(s)
}

// info contains the auth information for an insecure connection.
// It implements the AuthInfo interface.
type info struct {
	credentials.CommonAuthInfo
}

// AuthType returns the type of info as a string.
func (info) AuthType() string {
	return "insecure"
}

type muxConn struct {
	net.Conn
	buf []byte
}

func (m *muxConn) Read(b []byte) (int, error) {
	if len(m.buf) == 0 {
		return m.Conn.Read(b)
	}

	wn := copy(b, m.buf)
	if wn < len(m.buf) {
		m.buf = m.buf[wn:]
		return wn, nil
	}

	m.buf = nil
	b = b[wn:]

	n, err := m.Conn.Read(b)
	return n + wn, err
}
