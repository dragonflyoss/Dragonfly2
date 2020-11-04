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

package tcp

import (
	"crypto/tls"
	"time"
)

import (
	"github.com/dragonflyoss/Dragonfly2/df-transport"
)

import (
	"github.com/dragonflyoss/Dragonfly2/df-transport/demo/hello"
)

var (
	pkgHandler = &hello.PackageHandler{}
	// EventListener register event callback
	EventListener = &hello.MessageHandler{}
)

// InitialSession init session
func InitialSession(session transport.Session) (err error) {
	//session.SetCompressType(getty.CompressZip)
	_, ok := session.Conn().(*tls.Conn)
	if ok {
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
	}
	return nil
}
