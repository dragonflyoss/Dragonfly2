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
	"compress/flate"
	"net"
	"time"
)

import (
	gxsync "github.com/dubbogo/gost/sync"
	perrors "github.com/pkg/errors"
)

// NewSessionCallback will be invoked when server accepts a new client connection or client connects to server successfully.
// If there are too many client connections or u do not want to connect a server again, u can return non-nil error. And
// then getty will close the new session.
type NewSessionCallback func(Session) error

// Reader is used to unmarshal a complete pkg from buffer
type Reader interface {
	// Parse tcp/udp/websocket pkg from buffer and if possible return a complete pkg.
	// When receiving a tcp network streaming segment, there are 4 cases as following:
	// case 1: a error found in the streaming segment;
	// case 2: can not unmarshal a pkg header from the streaming segment;
	// case 3: unmarshal a pkg header but can not unmarshal a pkg from the streaming segment;
	// case 4: just unmarshal a pkg from the streaming segment;
	// case 5: unmarshal more than one pkg from the streaming segment;
	//
	// The return value is (nil, 0, error) as case 1.
	// The return value is (nil, 0, nil) as case 2.
	// The return value is (nil, pkgLen, nil) as case 3.
	// The return value is (pkg, pkgLen, nil) as case 4.
	// The handleTcpPackage may invoke func Read many times as case 5.
	Read(Session, []byte) (interface{}, int, error)
}

// Writer is used to marshal pkg and write to session
type Writer interface {
	// if @Session is udpGettySession, the second parameter is UDPContext.
	Write(Session, interface{}) ([]byte, error)
}

// ReadWriter interface use for handle application packages
type ReadWriter interface {
	Reader
	Writer
}

// EventListener is used to process pkg that received from remote session
type EventListener interface {
	// invoked when session opened
	// If the return error is not nil, @Session will be closed.
	OnOpen(Session) error

	// invoked when session closed.
	OnClose(Session)

	// invoked when got error.
	OnError(Session, error)

	// invoked periodically, its period can be set by (Session)SetCronPeriod
	OnCron(Session)

	// invoked when getty received a package. Pls attention that do not handle long time
	// logic processing in this func. You'd better set the package's maximum length.
	// If the message's length is greater than it, u should should return err in
	// Reader{Read} and getty will close this connection soon.
	//
	// If ur logic processing in this func will take a long time, u should start a goroutine
	// pool(like working thread pool in cpp) to handle the processing asynchronously. Or u
	// can do the logic processing in other asynchronous way.
	// !!!In short, ur OnMessage callback func should return asap.
	//
	// If this is a udp event listener, the second parameter type is UDPContext.
	OnMessage(Session, interface{})
}

/////////////////////////////////////////
// compress
/////////////////////////////////////////

type CompressType int

const (
	CompressNone            CompressType = flate.NoCompression      // 0
	CompressZip                          = flate.DefaultCompression // -1
	CompressBestSpeed                    = flate.BestSpeed          // 1
	CompressBestCompression              = flate.BestCompression    // 9
	CompressHuffman                      = flate.HuffmanOnly        // -2
	CompressSnappy                       = 10
)

/////////////////////////////////////////
// connection
/////////////////////////////////////////

type Connection interface {
	ID() uint32
	SetCompressType(CompressType)
	LocalAddr() string
	RemoteAddr() string
	incReadPkgNum()
	incWritePkgNum()
	// update session's active time
	UpdateActive()
	// get session's active time
	GetActive() time.Time
	readTimeout() time.Duration
	// SetReadTimeout sets deadline for the future read calls.
	SetReadTimeout(time.Duration)
	writeTimeout() time.Duration
	// SetWriteTimeout sets deadline for the future read calls.
	SetWriteTimeout(time.Duration)
	send(interface{}) (int, error)
	// don't distinguish between tcp connection and websocket connection. Because
	// gorilla/websocket/conn.go:(Conn)Close also invoke net.Conn.Close
	close(int)
	// set related session
	setSession(Session)
}

/////////////////////////////////////////
// Session
/////////////////////////////////////////

var (
	ErrSessionClosed  = perrors.New("session Already Closed")
	ErrSessionBlocked = perrors.New("session Full Blocked")
	ErrNullPeerAddr   = perrors.New("peer address is nil")
)

type Session interface {
	Connection
	Reset()
	Conn() net.Conn
	Stat() string
	IsClosed() bool
	// get endpoint type
	EndPoint() EndPoint

	SetMaxMsgLen(int)
	SetName(string)
	SetEventListener(EventListener)
	SetPkgHandler(ReadWriter)
	SetReader(Reader)
	SetWriter(Writer)
	SetCronPeriod(int)

	// Deprecated: don't use read queue.
	SetRQLen(int)

	SetWQLen(int)
	SetWaitTime(time.Duration)
	SetTaskPool(*gxsync.TaskPool)

	GetAttribute(interface{}) interface{}
	SetAttribute(interface{}, interface{})
	RemoveAttribute(interface{})

	// the Writer will invoke this function. Pls attention that if timeout is less than 0, WritePkg will send @pkg asap.
	// for udp session, the first parameter should be UDPContext.
	WritePkg(pkg interface{}, timeout time.Duration) error
	WriteBytes([]byte) error
	WriteBytesArray(...[]byte) error
	Close()
}

/////////////////////////////////////////
// EndPoint
/////////////////////////////////////////

type EndPoint interface {
	// get EndPoint ID
	ID() EndPointID
	// get endpoint type
	EndPointType() EndPointType
	// run event loop and serves client request.
	RunEventLoop(newSession NewSessionCallback)
	// check the endpoint has been closed
	IsClosed() bool
	// close the endpoint and free its resource
	Close()
}

type Client interface {
	EndPoint
}

// Server interface
type Server interface {
	EndPoint
}

// StreamServer is like tcp/websocket/wss server
type StreamServer interface {
	Server
	// get the network listener
	Listener() net.Listener
}

// PacketServer is like udp listen endpoint
type PacketServer interface {
	Server
	// get the network listener
	PacketConn() net.PacketConn
}
