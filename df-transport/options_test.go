/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package transport

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestClientOptions(t *testing.T) {
	addr := "127.0.0.1:0"
	file := ""
	clt := newClient(TCP_CLIENT,
		WithServerAddress(addr),
		WithReconnectInterval(5e8),
		WithConnectionNumber(1),
		WithRootCertificateFile(file),
	)
	assert.NotNil(t, clt)
	assert.Equal(t, clt.endPointType, TCP_CLIENT)
	assert.True(t, clt.endPointType > 0)
	assert.NotNil(t, clt.done)
	assert.NotNil(t, clt.ssMap)
	assert.Equal(t, clt.addr, addr)
	assert.NotNil(t, clt.reconnectInterval)
	assert.NotNil(t, clt.cert)
	assert.Equal(t, clt.number, 1)
}

func TestServerOptions(t *testing.T) {
	addr := "127.0.0.1:0"
	path := "/test"
	cert := ""
	key := "test"
	srv := newServer(TCP_SERVER,
		WithLocalAddress(addr),
		WithWebsocketServerPath(path),
		WithWebsocketServerCert(cert),
		WithWebsocketServerPrivateKey(key),
		WithWebsocketServerRootCert(cert),
	)
	assert.NotNil(t, srv)
	assert.Equal(t, srv.endPointType, TCP_SERVER)
	assert.True(t, srv.endPointType > 0)
	assert.NotNil(t, srv.done)
	assert.Equal(t, srv.addr, addr)
	assert.Equal(t, srv.path, path)
	assert.Equal(t, srv.cert, cert)
	assert.Equal(t, srv.privateKey, key)
	assert.Equal(t, srv.caCert, cert)
}
