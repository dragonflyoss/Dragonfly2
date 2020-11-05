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

package tcp

import (
	"fmt"
	"net"
	"time"
)

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/transport"
)

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/transport/demo/hello"
)

var (
	pkgHandler = &hello.PackageHandler{}
	// EventListener register event callback
	EventListener = &hello.MessageHandler{}
)

func InitialSession(session transport.Session) (err error) {
	//session.SetCompressType(getty.CompressZip)

	tcpConn, ok := session.Conn().(*net.TCPConn)
	if !ok {
		panic(fmt.Sprintf("newSession: %s, session.conn{%#v} is not tcp connection", session.Stat(), session.Conn()))
	}

	if err = tcpConn.SetNoDelay(true); err != nil {
		return err
	}
	if err = tcpConn.SetKeepAlive(true); err != nil {
		return err
	}
	if err = tcpConn.SetKeepAlivePeriod(10 * time.Second); err != nil {
		return err
	}
	if err = tcpConn.SetReadBuffer(262144); err != nil {
		return err
	}
	if err = tcpConn.SetWriteBuffer(524288); err != nil {
		return err
	}

	session.SetName("hello")
	session.SetMaxMsgLen(128)
	// session.SetRQLen(1024)
	session.SetWQLen(512)
	session.SetReadTimeout(time.Second)
	session.SetWriteTimeout(5 * time.Second)
	session.SetCronPeriod(int(hello.CronPeriod / 1e6))
	session.SetWaitTime(time.Second)

	session.SetPkgHandler(pkgHandler)
	session.SetEventListener(EventListener)
	return nil
}
