/*
 * Copyright The Dragonfly Authors.
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

package proxy

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"sync"
	"time"

	"github.com/golang/groupcache/lru"
	"github.com/pkg/errors"

	"github.com/dragonflyoss/Dragonfly2/client/config"
	"github.com/dragonflyoss/Dragonfly2/client/daemon/peer"
	"github.com/dragonflyoss/Dragonfly2/client/daemon/transport"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
)

var okHeader = []byte("HTTP/1.1 200 OK\r\n\r\n")

// Proxy is an http proxy handler. It proxies requests with dragonfly
// if any defined proxy rules is matched
type Proxy struct {
	// reverse proxy upstream url for the default registry
	registry *config.RegistryMirror

	// proxy rules
	rules []*config.Proxy

	// httpsHosts is the list of hosts whose https requests will be hijacked
	httpsHosts []*config.HijackHost

	// cert is the certificate used to hijack https proxy requests
	cert *tls.Certificate

	// certCache is a in-memory cache store for TLS certs used in HTTPS hijack. Lazy init.
	certCache *lru.Cache

	// directHandler are used to handle non proxy requests
	directHandler http.Handler

	// peerTaskManager is the peer task manager
	peerTaskManager peer.PeerTaskManager

	// peerHost is the peer host info
	peerHost *scheduler.PeerHost
}

// Option is a functional option for configuring the proxy
type Option func(p *Proxy) *Proxy

// WithHTTPSHosts sets the rules for hijacking https requests
func WithPeerHost(peerHost *scheduler.PeerHost) Option {
	return func(p *Proxy) *Proxy {
		p.peerHost = peerHost
		return p
	}
}

// WithHTTPSHosts sets the rules for hijacking https requests
func WithPeerTaskManager(peerTaskManager peer.PeerTaskManager) Option {
	return func(p *Proxy) *Proxy {
		p.peerTaskManager = peerTaskManager
		return p
	}
}

// WithHTTPSHosts sets the rules for hijacking https requests
func WithHTTPSHosts(hosts ...*config.HijackHost) Option {
	return func(p *Proxy) *Proxy {
		p.httpsHosts = hosts
		return p
	}
}

// WithRegistryMirror sets the registry mirror for the proxy
func WithRegistryMirror(r *config.RegistryMirror) Option {
	return func(p *Proxy) *Proxy {
		p.registry = r
		return p
	}
}

// WithCertFromFile is a convenient wrapper for WithCert, to read certificate from
// the given file
func WithCert(cert *tls.Certificate) Option {
	return func(p *Proxy) *Proxy {
		p.cert = cert
		return p
	}
}

// WithDirectHandler sets the handler for non-proxy requests
func WithDirectHandler(h *http.ServeMux) Option {
	return func(p *Proxy) *Proxy {
		// Make sure the root handler of the given server mux is the
		// registry mirror reverse proxy
		h.HandleFunc("/", p.mirrorRegistry)
		p.directHandler = h
		return p
	}
}

// WithRules sets the proxy rules
func WithRules(rules []*config.Proxy) Option {
	return func(p *Proxy) *Proxy {
		p.rules = rules
		return p
	}
}

// NewFromConfig returns a new transparent proxy from the given properties
func NewProxy(options ...Option) (*Proxy, error) {
	return NewProxyWithOptions(options...)
}

// NewProxyWithOptions constructs a new instance of a Proxy with additional options.
func NewProxyWithOptions(options ...Option) (*Proxy, error) {
	proxy := &Proxy{
		directHandler: http.NewServeMux(),
	}
	for _, opt := range options {
		opt(proxy)
	}

	return proxy, nil
}

// ServeHTTP implements http.Handler.ServeHTTP
func (proxy *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect {
		// handle https proxy requests
		proxy.handleHTTPS(w, r)
	} else if r.URL.Scheme == "" {
		// handle direct requests
		proxy.directHandler.ServeHTTP(w, r)
	} else {
		// handle http proxy requests
		proxy.handleHTTP(w, r)
	}
}

func (proxy *Proxy) handleHTTP(w http.ResponseWriter, req *http.Request) {
	resp, err := proxy.newTransport(nil).RoundTrip(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()
	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		logger.Errorf("failed to write http body: %v", err)
	}
}

func (proxy *Proxy) handleHTTPS(w http.ResponseWriter, r *http.Request) {
	if proxy.cert == nil {
		tunnelHTTPS(w, r)
		return
	}

	cConfig := proxy.remoteConfig(r.Host)
	if cConfig == nil {
		tunnelHTTPS(w, r)
		return
	}

	logger.Debugf("hijack https request to %s", r.Host)

	sConfig := new(tls.Config)
	if proxy.cert.Leaf != nil && proxy.cert.Leaf.IsCA {
		if proxy.certCache == nil { // Initialize proxy.certCache on first access. (Lazy init)
			proxy.certCache = lru.New(100) // Default max entries size = 100
		}
		logger.Debugf("hijack https request with CA <%s>", proxy.cert.Leaf.Subject.CommonName)
		leafCertSpec := LeafCertSpec{
			proxy.cert.Leaf.PublicKey,
			proxy.cert.PrivateKey,
			proxy.cert.Leaf.SignatureAlgorithm}
		host, _, _ := net.SplitHostPort(r.Host)
		sConfig.GetCertificate = func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			cConfig.ServerName = host
			logger.Debugf("Generate temporal leaf TLS cert for ServerName <%s>, host <%s>", hello.ServerName, host)
			// It's assumed that `hello.ServerName` is always same as `host`, in practice.
			cacheKey := host
			cached, hit := proxy.certCache.Get(cacheKey)
			if hit && time.Now().Before(cached.(*tls.Certificate).Leaf.NotAfter) { // If cache hit and the cert is not expired
				logger.Debugf("TLS Cache hit, cacheKey = <%s>", cacheKey)
				return cached.(*tls.Certificate), nil
			}
			cert, err := genLeafCert(proxy.cert, &leafCertSpec, host)
			if err == nil {
				// Put cert in cache only if there is no error. So all certs in cache are always valid.
				// But certs in cache maybe expired (After 24 hours, see the default duration of generated certs)
				proxy.certCache.Add(cacheKey, cert)
			}
			// If err != nil, means unrecoverable error happened in genLeafCert(...)
			return cert, err
		}
	} else {
		sConfig.Certificates = []tls.Certificate{*proxy.cert}
	}

	sConn, err := handshake(w, sConfig)
	if err != nil {
		logger.Errorf("handshake failed for %s: %v", r.Host, err)
		return
	}
	defer sConn.Close()

	cConn, err := tls.Dial("tcp", r.Host, cConfig)
	if err != nil {
		logger.Errorf("dial failed for %s: %v", r.Host, err)
		return
	}
	cConn.Close()

	rp := &httputil.ReverseProxy{
		Director: func(r *http.Request) {
			r.URL.Host = r.Host
			r.URL.Scheme = "https"
		},
		Transport: proxy.newTransport(cConfig),
	}

	// We have to wait until the connection is closed
	wg := sync.WaitGroup{}
	wg.Add(1)
	// NOTE: http.Serve always returns a non-nil error
	if err := http.Serve(&singleUseListener{&customCloseConn{sConn, wg.Done}}, rp); err != errServerClosed && err != http.ErrServerClosed {
		logger.Errorf("failed to accept incoming HTTP connections: %v", err)
	}
	wg.Wait()
}

func (proxy *Proxy) newTransport(tlsConfig *tls.Config) http.RoundTripper {
	rt, _ := transport.New(
		transport.WithPeerHost(proxy.peerHost),
		transport.WithPeerTaskManager(proxy.peerTaskManager),
		transport.WithTLS(tlsConfig),
		transport.WithCondition(proxy.shouldUseDragonfly),
	)
	return rt
}

func (proxy *Proxy) mirrorRegistry(w http.ResponseWriter, r *http.Request) {
	reverseProxy := httputil.NewSingleHostReverseProxy(proxy.registry.Remote.URL)
	t, err := transport.New(
		transport.WithPeerHost(proxy.peerHost),
		transport.WithPeerTaskManager(proxy.peerTaskManager),
		transport.WithTLS(proxy.registry.TLSConfig()),
		transport.WithCondition(proxy.shouldUseDragonflyForMirror),
	)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to get transport: %v", err), http.StatusInternalServerError)
	}

	reverseProxy.Transport = t
	reverseProxy.ServeHTTP(w, r)
}

// remoteConfig returns the tls.Config used to connect to the given remote host.
// If the host should not be hijacked, and it will return nil.
func (proxy *Proxy) remoteConfig(host string) *tls.Config {
	for _, h := range proxy.httpsHosts {
		if h.Regx.MatchString(host) {
			config := &tls.Config{InsecureSkipVerify: h.Insecure}
			if h.Certs != nil {
				config.RootCAs = h.Certs.CertPool
			}
			return config
		}
	}
	return nil
}

// setRules changes the rule lists of the proxy to the given rules.
func (proxy *Proxy) setRules(rules []*config.Proxy) error {
	proxy.rules = rules
	return nil
}

// shouldUseDragonfly returns whether we should use dragonfly to proxy a request. It
// also change the scheme of the given request if the matched rule has
// UseHTTPS = true
func (proxy *Proxy) shouldUseDragonfly(req *http.Request) bool {
	if req.Method != http.MethodGet {
		return false
	}

	for _, rule := range proxy.rules {
		if rule.Match(req.URL.String()) {
			if rule.UseHTTPS {
				req.URL.Scheme = "https"
			}
			if rule.Redirect != "" {
				req.URL.Host = rule.Redirect
				req.Host = rule.Redirect
			}
			return !rule.Direct
		}
	}
	return false
}

// shouldUseDragonflyForMirror returns whether we should use dragonfly to proxy a request
// when we use registry mirror.
func (proxy *Proxy) shouldUseDragonflyForMirror(req *http.Request) bool {
	return proxy.registry != nil && !proxy.registry.Direct && transport.NeedUseDragonfly(req)
}

// tunnelHTTPS handles a CONNECT request and proxy an https request through an
// http tunnel.
func tunnelHTTPS(w http.ResponseWriter, r *http.Request) {
	logger.Debugf("Tunneling https request for %s", r.Host)
	dst, err := net.DialTimeout("tcp", r.Host, 10*time.Second)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "Hijacking not supported", http.StatusInternalServerError)
		return
	}
	clientConn, _, err := hijacker.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	}

	go copyAndClose(dst, clientConn)
	go copyAndClose(clientConn, dst)
}

func copyAndClose(dst io.WriteCloser, src io.ReadCloser) error {
	defer src.Close()
	defer dst.Close()
	if _, err := io.Copy(dst, src); err != nil {
		return err
	}
	return nil
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

// handshake hijacks w's underlying net.Conn, responds to the CONNECT request
// and manually performs the TLS handshake.
func handshake(w http.ResponseWriter, config *tls.Config) (net.Conn, error) {
	raw, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		http.Error(w, "no upstream", http.StatusServiceUnavailable)
		return nil, err
	}
	if _, err = raw.Write(okHeader); err != nil {
		raw.Close()
		return nil, err
	}
	conn := tls.Server(raw, config)
	if err = conn.Handshake(); err != nil {
		conn.Close()
		raw.Close()
		return nil, err
	}
	return conn, nil
}

// A singleUseListener implements a net.Listener that returns the net.Conn specified
// in c for the first Accept call, and returns errors for the subsequent calls.
type singleUseListener struct {
	c net.Conn
}

// errServerClosed is returned by the singleUseListener's Accept method
// when it receives the subsequent calls after the first Accept call
var errServerClosed = errors.New("singleUseListener: Server closed")

func (l *singleUseListener) Accept() (net.Conn, error) {
	if l.c == nil {
		return nil, errServerClosed
	}
	c := l.c
	l.c = nil
	return c, nil
}

func (l *singleUseListener) Close() error { return nil }

func (l *singleUseListener) Addr() net.Addr { return l.c.LocalAddr() }

// A customCloseConn implements net.Conn and calls f before closing the underlying net.Conn.
type customCloseConn struct {
	net.Conn
	f func()
}

func (c *customCloseConn) Close() error {
	if c.f != nil {
		c.f()
		c.f = nil
	}
	return c.Conn.Close()
}
