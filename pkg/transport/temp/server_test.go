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

package temp

import (
	"path/filepath"
	"testing"
	"time"
)

import (
	"github.com/stretchr/testify/assert"
)

func testTCPServer(t *testing.T, address string) {
	var (
		server           *server
		serverMsgHandler MessageHandler
	)

	func() {
		server = newServer(
			TCP_SERVER,
			WithLocalAddress(address),
		)
		newServerSession := func(session Session) error {
			return newSessionCallback(session, &serverMsgHandler)
		}
		server.RunEventLoop(newServerSession)
		assert.True(t, server.ID() > 0)
		assert.True(t, server.EndPointType() == TCP_SERVER)
		assert.NotNil(t, server.streamListener)
	}()
	time.Sleep(500e6)

	addr := server.streamListener.Addr().String()
	t.Logf("@address:%s, tcp server addr: %v", address, addr)
	clt := newClient(TCP_CLIENT,
		WithServerAddress(addr),
		WithReconnectInterval(5e8),
		WithConnectionNumber(1),
	)
	assert.NotNil(t, clt)
	assert.True(t, clt.ID() > 0)
	assert.Equal(t, clt.endPointType, TCP_CLIENT)

	var (
		msgHandler MessageHandler
	)
	cb := func(session Session) error {
		return newSessionCallback(session, &msgHandler)
	}

	clt.RunEventLoop(cb)
	time.Sleep(1e9)

	assert.Equal(t, 1, msgHandler.SessionNumber())
	clt.Close()
	assert.True(t, clt.IsClosed())

	server.Close()
	assert.True(t, server.IsClosed())
}
func testTCPTlsServer(t *testing.T, address string) {
	var (
		server           *server
		serverMsgHandler MessageHandler
	)
	serverPemPath, _ := filepath.Abs("./demo/hello/tls/certs/server0.pem")
	serverKeyPath, _ := filepath.Abs("./demo/hello/tls/certs/server0.key")
	caPemPath, _ := filepath.Abs("./demo/hello/tls/certs/ca.pem")

	configBuilder := &ServerTlsConfigBuilder{
		ServerKeyCertChainPath:        serverPemPath,
		ServerPrivateKeyPath:          serverKeyPath,
		ServerTrustCertCollectionPath: caPemPath,
	}

	func() {
		server = newServer(
			TCP_SERVER,
			WithLocalAddress(address),
			WithServerSslEnabled(true),
			WithServerTlsConfigBuilder(configBuilder),
		)
		newServerSession := func(session Session) error {
			return newSessionCallback(session, &serverMsgHandler)
		}
		server.RunEventLoop(newServerSession)
		assert.True(t, server.ID() > 0)
		assert.True(t, server.EndPointType() == TCP_SERVER)
		assert.NotNil(t, server.streamListener)
	}()
	time.Sleep(500e6)

	addr := server.streamListener.Addr().String()
	t.Logf("@address:%s, tcp server addr: %v", address, addr)
	keyPath, _ := filepath.Abs("./demo/hello/tls/certs/ca.key")
	clientCaPemPath, _ := filepath.Abs("./demo/hello/tls/certs/ca.pem")

	clientConfig := &ClientTlsConfigBuilder{
		ClientTrustCertCollectionPath: clientCaPemPath,
		ClientPrivateKeyPath:          keyPath,
	}

	clt := newClient(TCP_CLIENT,
		WithServerAddress(addr),
		WithReconnectInterval(5e8),
		WithConnectionNumber(1),
		WithClientTlsConfigBuilder(clientConfig),
	)
	assert.NotNil(t, clt)
	assert.True(t, clt.ID() > 0)
	assert.Equal(t, clt.endPointType, TCP_CLIENT)

	var (
		msgHandler MessageHandler
	)
	cb := func(session Session) error {
		return newSessionCallback(session, &msgHandler)
	}

	clt.RunEventLoop(cb)
	time.Sleep(1e9)

	assert.Equal(t, 1, msgHandler.SessionNumber())
	clt.Close()
	assert.True(t, clt.IsClosed())

	server.Close()
	assert.True(t, server.IsClosed())
}

func testUDPServer(t *testing.T, address string) {
	var (
		server           *server
		serverMsgHandler MessageHandler
	)
	func() {
		server = newServer(
			UDP_ENDPOINT,
			WithLocalAddress(address),
		)
		newServerSession := func(session Session) error {
			return newSessionCallback(session, &serverMsgHandler)
		}
		server.RunEventLoop(newServerSession)
		assert.True(t, server.ID() > 0)
		assert.True(t, server.EndPointType() == UDP_ENDPOINT)
		assert.NotNil(t, server.pktListener)
	}()
	time.Sleep(500e6)

	addr := server.pktListener.LocalAddr().String()
	t.Logf("@address:%s, udp server addr: %v", address, addr)
}

func TestServer(t *testing.T) {
	var addr string

	testTCPServer(t, addr)
	testUDPServer(t, addr)

	addr = "127.0.0.1:0"
	testTCPServer(t, addr)
	testUDPServer(t, addr)

	addr = "127.0.0.1"
	testTCPServer(t, addr)
	testUDPServer(t, addr)
	addr = "127.0.0.9999"
	testTCPTlsServer(t, addr)
}
