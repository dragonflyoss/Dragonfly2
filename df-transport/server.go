/*
 *     Copyright The Dragonfly Authors
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

package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

import (
	gxnet "github.com/dubbogo/gost/net"
	"github.com/gorilla/websocket"

	perrors "github.com/pkg/errors"
)

var (
	errSelfConnect        = perrors.New("connect self!")
	serverFastFailTimeout = time.Second * 1
	serverID              = EndPointID(0)
)

type server struct {
	ServerOptions

	// endpoint ID
	endPointID EndPointID

	// net
	pktListener    net.PacketConn
	streamListener net.Listener
	lock           sync.Mutex // for server
	endPointType   EndPointType
	server         *http.Server // for ws or wss server
	sync.Once
	done chan struct{}
	wg   sync.WaitGroup
}

func (s *server) init(opts ...ServerOption) {
	for _, opt := range opts {
		opt(&(s.ServerOptions))
	}
}

func newServer(t EndPointType, opts ...ServerOption) *server {
	s := &server{
		endPointID:   atomic.AddInt32(&serverID, 1),
		endPointType: t,
		done:         make(chan struct{}),
	}

	s.init(opts...)

	return s
}

// NewTCPServer builds a tcp server.
func NewTCPServer(opts ...ServerOption) Server {
	return newServer(TCP_SERVER, opts...)
}

// NewUDPEndPoint builds a unconnected udp server.
func NewUDPPEndPoint(opts ...ServerOption) Server {
	return newServer(UDP_ENDPOINT, opts...)
}

// NewWSServer builds a websocket server.
func NewWSServer(opts ...ServerOption) Server {
	return newServer(WS_SERVER, opts...)
}

// NewWSSServer builds a secure websocket server.
func NewWSSServer(opts ...ServerOption) Server {
	s := newServer(WSS_SERVER, opts...)

	if s.addr == "" || s.cert == "" || s.privateKey == "" {
		panic(fmt.Sprintf("@addr:%s, @certs:%s, @privateKey:%s, @caCert:%s",
			s.addr, s.cert, s.privateKey, s.caCert))
	}

	return s
}

func (s server) ID() int32 {
	return s.endPointID
}

func (s server) EndPointType() EndPointType {
	return s.endPointType
}

func (s *server) stop() {
	var (
		err error
		ctx context.Context
	)

	select {
	case <-s.done:
		return
	default:
		s.Once.Do(func() {
			close(s.done)
			s.lock.Lock()
			if s.server != nil {
				ctx, _ = context.WithTimeout(context.Background(), serverFastFailTimeout)
				if err = s.server.Shutdown(ctx); err != nil {
					// if the log output is "shutdown ctx: context deadline exceeded"， it means that
					// there are still some active connections.
					log.Errorf("server shutdown ctx:%s error:%v", ctx, err)
				}
			}
			s.server = nil
			s.lock.Unlock()
			if s.streamListener != nil {
				// let the server exit asap when got error from RunEventLoop.
				s.streamListener.Close()
				s.streamListener = nil
			}
			if s.pktListener != nil {
				s.pktListener.Close()
				s.pktListener = nil
			}
		})
	}
}

func (s *server) IsClosed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// net.ipv4.tcp_max_syn_backlog
// net.ipv4.tcp_timestamps
// net.ipv4.tcp_tw_recycle
func (s *server) listenTCP() error {
	var (
		err            error
		streamListener net.Listener
	)

	if len(s.addr) == 0 || !strings.Contains(s.addr, ":") {
		streamListener, err = gxnet.ListenOnTCPRandomPort(s.addr)
		if err != nil {
			return perrors.Wrapf(err, "gxnet.ListenOnTCPRandomPort(addr:%s)", s.addr)
		}
	} else {
		if s.sslEnabled {
			if sslConfig, err := s.tlsConfigBuilder.BuildTlsConfig(); err == nil && sslConfig != nil {
				streamListener, err = tls.Listen("tcp", s.addr, sslConfig)
			}
		} else {
			streamListener, err = net.Listen("tcp", s.addr)
		}
		if err != nil {
			return perrors.Wrapf(err, "net.Listen(tcp, addr:%s)", s.addr)
		}
	}

	s.streamListener = streamListener
	s.addr = s.streamListener.Addr().String()

	return nil
}

func (s *server) listenUDP() error {
	var (
		err         error
		localAddr   *net.UDPAddr
		pktListener *net.UDPConn
	)

	if len(s.addr) == 0 || !strings.Contains(s.addr, ":") {
		pktListener, err = gxnet.ListenOnUDPRandomPort(s.addr)
		if err != nil {
			return perrors.Wrapf(err, "gxnet.ListenOnUDPRandomPort(addr:%s)", s.addr)
		}
	} else {
		localAddr, err = net.ResolveUDPAddr("udp", s.addr)
		if err != nil {
			return perrors.Wrapf(err, "net.ResolveUDPAddr(udp, addr:%s)", s.addr)
		}
		pktListener, err = net.ListenUDP("udp", localAddr)
		if err != nil {
			return perrors.Wrapf(err, "net.ListenUDP((udp, localAddr:%#v)", localAddr)
		}
	}

	s.pktListener = pktListener
	s.addr = s.pktListener.LocalAddr().String()

	return nil
}

// Listen announces on the local network address.
func (s *server) listen() error {
	switch s.endPointType {
	case TCP_SERVER, WS_SERVER, WSS_SERVER:
		return perrors.WithStack(s.listenTCP())
	case UDP_ENDPOINT:
		return perrors.WithStack(s.listenUDP())
	}

	return nil
}

func (s *server) accept(newSession NewSessionCallback) (Session, error) {
	conn, err := s.streamListener.Accept()
	if err != nil {
		return nil, perrors.WithStack(err)
	}
	if gxnet.IsSameAddr(conn.RemoteAddr(), conn.LocalAddr()) {
		log.Warnf("conn.localAddr{%s} == conn.RemoteAddr", conn.LocalAddr().String(), conn.RemoteAddr().String())
		return nil, perrors.WithStack(errSelfConnect)
	}

	ss := newTCPSession(conn, s)
	err = newSession(ss)
	if err != nil {
		conn.Close()
		return nil, perrors.WithStack(err)
	}

	return ss, nil
}

func (s *server) runTcpEventLoop(newSession NewSessionCallback) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		var (
			err    error
			client Session
			delay  time.Duration
		)
		for {
			if s.IsClosed() {
				log.Warnf("server{%s} stop accepting client connect request.", s.addr)
				return
			}
			if delay != 0 {
				<-wheel.After(delay)
			}
			client, err = s.accept(newSession)
			if err != nil {
				if netErr, ok := perrors.Cause(err).(net.Error); ok && netErr.Temporary() {
					if delay == 0 {
						delay = 5 * time.Millisecond
					} else {
						delay *= 2
					}
					if max := 1 * time.Second; delay > max {
						delay = max
					}
					continue
				}
				log.Warnf("server{%s}.Accept() = err {%+v}", s.addr, perrors.WithStack(err))
				continue
			}
			delay = 0
			client.(*session).run()
		}
	}()
}

func (s *server) runUDPEventLoop(newSession NewSessionCallback) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		var (
			err  error
			conn *net.UDPConn
			ss   Session
		)

		conn = s.pktListener.(*net.UDPConn)
		ss = newUDPSession(conn, s)
		if err = newSession(ss); err != nil {
			conn.Close()
			panic(err.Error())
		}
		ss.(*session).run()
	}()
}

type wsHandler struct {
	http.ServeMux
	server     *server
	newSession NewSessionCallback
	upgrader   websocket.Upgrader
}

func newWSHandler(server *server, newSession NewSessionCallback) *wsHandler {
	return &wsHandler{
		server:     server,
		newSession: newSession,
		upgrader: websocket.Upgrader{
			// in default, ReadBufferSize & WriteBufferSize is 4k
			// HandshakeTimeout: server.HTTPTimeout,
			CheckOrigin:       func(_ *http.Request) bool { return true }, // allow connections from any origin
			EnableCompression: true,
		},
	}
}

func (s *wsHandler) serveWSRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		// w.WriteHeader(http.StatusMethodNotAllowed)
		http.Error(w, "Method not allowed", 405)
		return
	}

	if s.server.IsClosed() {
		http.Error(w, "HTTP server is closed(code:500-11).", 500)
		log.Warnf("server{%s} stop acceptting client connect request.", s.server.addr)
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Warnf("upgrader.Upgrader(http.Request{%#v}) = error:%+v", r, err)
		return
	}
	if conn.RemoteAddr().String() == conn.LocalAddr().String() {
		log.Warnf("conn.localAddr{%s} == conn.RemoteAddr", conn.LocalAddr().String(), conn.RemoteAddr().String())
		return
	}
	// conn.SetReadLimit(int64(handler.maxMsgLen))
	ss := newWSSession(conn, s.server)
	err = s.newSession(ss)
	if err != nil {
		conn.Close()
		log.Warnf("server{%s}.newSession(ss{%#v}) = err {%s}", s.server.addr, ss, err)
		return
	}
	if ss.(*session).maxMsgLen > 0 {
		conn.SetReadLimit(int64(ss.(*session).maxMsgLen))
	}
	ss.(*session).run()
}

// runWSEventLoop serve websocket client request
// @newSession: new websocket connection callback
func (s *server) runWSEventLoop(newSession NewSessionCallback) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		var (
			err     error
			handler *wsHandler
			server  *http.Server
		)
		handler = newWSHandler(s, newSession)
		handler.HandleFunc(s.path, handler.serveWSRequest)
		server = &http.Server{
			Addr:    s.addr,
			Handler: handler,
			// ReadTimeout:    server.HTTPTimeout,
			// WriteTimeout:   server.HTTPTimeout,
		}
		s.lock.Lock()
		s.server = server
		s.lock.Unlock()
		err = server.Serve(s.streamListener)
		if err != nil {
			log.Errorf("http.server.Serve(addr{%s}) = err:%+v", s.addr, perrors.WithStack(err))
		}
	}()
}

// serve websocket client request
// RunWSSEventLoop serve websocket client request
func (s *server) runWSSEventLoop(newSession NewSessionCallback) {
	s.wg.Add(1)
	go func() {
		var (
			err         error
			certPem     []byte
			certificate tls.Certificate
			certPool    *x509.CertPool
			config      *tls.Config
			handler     *wsHandler
			server      *http.Server
		)
		defer s.wg.Done()

		if certificate, err = tls.LoadX509KeyPair(s.cert, s.privateKey); err != nil {
			panic(fmt.Sprintf("tls.LoadX509KeyPair(certs{%s}, privateKey{%s}) = err:%+v",
				s.cert, s.privateKey, perrors.WithStack(err)))
			return
		}
		config = &tls.Config{
			InsecureSkipVerify: true, // do not verify peer certs
			ClientAuth:         tls.NoClientCert,
			NextProtos:         []string{"http/1.1"},
			Certificates:       []tls.Certificate{certificate},
		}

		if s.caCert != "" {
			certPem, err = ioutil.ReadFile(s.caCert)
			if err != nil {
				panic(fmt.Errorf("ioutil.ReadFile(certFile{%s}) = err:%+v", s.caCert, perrors.WithStack(err)))
			}
			certPool = x509.NewCertPool()
			if ok := certPool.AppendCertsFromPEM(certPem); !ok {
				panic("failed to parse root certificate file")
			}
			config.ClientCAs = certPool
			config.ClientAuth = tls.RequireAndVerifyClientCert
			config.InsecureSkipVerify = false
		}

		handler = newWSHandler(s, newSession)
		handler.HandleFunc(s.path, handler.serveWSRequest)
		server = &http.Server{
			Addr:    s.addr,
			Handler: handler,
			// ReadTimeout:    server.HTTPTimeout,
			// WriteTimeout:   server.HTTPTimeout,
		}
		server.SetKeepAlivesEnabled(true)
		s.lock.Lock()
		s.server = server
		s.lock.Unlock()
		err = server.Serve(tls.NewListener(s.streamListener, config))
		if err != nil {
			log.Errorf("http.server.Serve(addr{%s}) = err:%+v", s.addr, perrors.WithStack(err))
			panic(err)
		}
	}()
}

// RunEventLoop serves client request.
// @newSession: new connection callback
func (s *server) RunEventLoop(newSession NewSessionCallback) {
	if err := s.listen(); err != nil {
		panic(fmt.Errorf("server.listen() = error:%+v", perrors.WithStack(err)))
	}

	switch s.endPointType {
	case TCP_SERVER:
		s.runTcpEventLoop(newSession)
	case UDP_ENDPOINT:
		s.runUDPEventLoop(newSession)
	case WS_SERVER:
		s.runWSEventLoop(newSession)
	case WSS_SERVER:
		s.runWSSEventLoop(newSession)
	default:
		panic(fmt.Sprintf("illegal server type %s", s.endPointType.String()))
	}
}

func (s *server) Listener() net.Listener {
	return s.streamListener
}

func (s *server) PacketConn() net.PacketConn {
	return s.pktListener
}

func (s *server) Close() {
	s.stop()
	s.wg.Wait()
}
