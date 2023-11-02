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

package proxy

import (
	"crypto/subtle"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/go-http-utils/headers"
	"github.com/golang/groupcache/lru"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"
	"golang.org/x/sync/semaphore"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/metrics"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	"d7y.io/dragonfly/v2/client/daemon/transport"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	pkgstrings "d7y.io/dragonfly/v2/pkg/strings"
)

var (
	okHeader = []byte("HTTP/1.1 200 OK\r\n\r\n")

	schemaHTTPS = "https"

	portHTTPS = 443
)

// Proxy is a http proxy handler. It proxies requests with dragonfly
// if any defined proxy rules is matched
type Proxy struct {
	// reverse proxy upstream url for the default registry
	registry *config.RegistryMirror

	// proxy rules
	rules atomic.Value

	// httpsHosts is the list of hosts whose https requests will be hijacked
	httpsHosts []*config.HijackHost

	// cert is the certificate used to hijack https proxy requests
	cert *tls.Certificate

	// certCache is an in-memory cache store for TLS certs used in HTTPS hijack
	certCache    *lru.Cache
	cacheRWMutex sync.RWMutex

	// directHandler are used to handle non-proxy requests
	directHandler http.Handler

	// transport is used to handle http proxy requests
	transport http.RoundTripper

	// peerTaskManager is the peer task manager
	peerTaskManager peer.TaskManager

	// peerHost is the peer host info
	peerHost *schedulerv1.PeerHost

	// whiteList is the proxy white list
	whiteList []*config.WhiteList

	// semaphore is used to limit max concurrency when process http request
	semaphore *semaphore.Weighted

	// defaultFilter is used when http request without X-Dragonfly-Filter Header
	defaultFilter string

	// defaultTag is used when http request without X-Dragonfly-Tag Header
	defaultTag string

	// defaultApplication is used when http request without X-Dragonfly-Application Header
	defaultApplication string

	// defaultPriority is used for scheduling
	defaultPriority commonv1.Priority

	// tracer is used for telemetry
	tracer trace.Tracer

	basicAuth *config.BasicAuth

	// dumpHTTPContent indicates to dump http request header and response header
	dumpHTTPContent bool

	peerIDGenerator peer.IDGenerator

	wg *sync.WaitGroup
}

// Option is a functional option for configuring the proxy
type Option func(p *Proxy) *Proxy

// WithPeerHost sets the *schedulerv1.PeerHost
func WithPeerHost(peerHost *schedulerv1.PeerHost) Option {
	return func(p *Proxy) *Proxy {
		p.peerHost = peerHost
		return p
	}
}

// WithPeerIDGenerator sets the *transport.PeerIDGenerator
func WithPeerIDGenerator(peerIDGenerator peer.IDGenerator) Option {
	return func(p *Proxy) *Proxy {
		p.peerIDGenerator = peerIDGenerator
		return p
	}
}

// WithPeerTaskManager sets the peer.PeerTaskManager
func WithPeerTaskManager(peerTaskManager peer.TaskManager) Option {
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

// WithCert sets the certificate
func WithCert(cert *tls.Certificate) Option {
	return func(p *Proxy) *Proxy {
		p.cert = cert
		return p
	}
}

// WithDirectHandler sets the handler for non-proxy requests
func WithDirectHandler(h *http.ServeMux) Option {
	return func(p *Proxy) *Proxy {
		if p.registry == nil || p.registry.Remote == nil || p.registry.Remote.URL == nil {
			logger.Warnf("registry mirror url is empty, registry mirror feature is disabled")
			h.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, fmt.Sprintf("registry mirror feature is disabled"), http.StatusNotFound)
			})
			p.directHandler = h
			return p
		}
		// Make sure the root handler of the given server mux is the
		// registry mirror reverse proxy
		h.HandleFunc("/", p.mirrorRegistry)
		p.directHandler = h
		return p
	}
}

// WithRules sets the proxy rules
func WithRules(rules []*config.ProxyRule) Option {
	return func(p *Proxy) *Proxy {
		p.rules.Store(rules)
		return p
	}
}

// WithWhiteList sets the proxy whitelist
func WithWhiteList(whiteList []*config.WhiteList) Option {
	return func(p *Proxy) *Proxy {
		p.whiteList = whiteList
		return p
	}
}

// WithMaxConcurrency sets max concurrent for process http request
func WithMaxConcurrency(con int64) Option {
	return func(p *Proxy) *Proxy {
		if con > 0 {
			p.semaphore = semaphore.NewWeighted(con)
		}
		return p
	}
}

// WithDefaultFilter sets default filter for http requests without X-Dragonfly-Filter Header
func WithDefaultFilter(f string) Option {
	return func(p *Proxy) *Proxy {
		p.defaultFilter = f
		return p
	}
}

// WithDefaultTag sets default tag for http requests without X-Dragonfly-Tag Header
func WithDefaultTag(t string) Option {
	return func(p *Proxy) *Proxy {
		p.defaultTag = t
		return p
	}
}

// WithDefaultApplication sets default tag for http requests without X-Dragonfly-Application Header
func WithDefaultApplication(t string) Option {
	return func(p *Proxy) *Proxy {
		p.defaultApplication = t
		return p
	}
}

// WithDefaultPriority sets default priority for http requests without X-Dragonfly-Priority Header
func WithDefaultPriority(priority commonv1.Priority) Option {
	return func(p *Proxy) *Proxy {
		p.defaultPriority = priority
		return p
	}
}

// WithBasicAuth sets basic auth info for proxy
func WithBasicAuth(auth *config.BasicAuth) Option {
	return func(p *Proxy) *Proxy {
		p.basicAuth = auth
		return p
	}
}

func WithDumpHTTPContent(dump bool) Option {
	return func(p *Proxy) *Proxy {
		p.dumpHTTPContent = dump
		return p
	}
}

// NewProxy returns a new transparent proxy from the given options
func NewProxy(options ...Option) (*Proxy, error) {
	return NewProxyWithOptions(options...)
}

// NewProxyWithOptions constructs a new instance of a Proxy with additional options.
func NewProxyWithOptions(options ...Option) (*Proxy, error) {
	proxy := &Proxy{
		directHandler: http.NewServeMux(),
		tracer:        otel.Tracer("dfget-daemon-proxy"),
		certCache:     lru.New(100),
		wg:            &sync.WaitGroup{},
	}

	for _, opt := range options {
		opt(proxy)
	}

	if proxy.transport == nil {
		proxy.transport = proxy.newTransport(nil)
	}
	return proxy, nil
}

func isBasicAuthMatch(basicAuth *config.BasicAuth, user, pass string) bool {
	usernameOK := subtle.ConstantTimeCompare([]byte(basicAuth.Username), []byte(user)) == 1
	passwordOK := subtle.ConstantTimeCompare([]byte(basicAuth.Password), []byte(pass)) == 1
	return usernameOK && passwordOK
}

// ServeHTTP implements http.Handler.ServeHTTP
func (proxy *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	proxy.wg.Add(1)
	defer proxy.wg.Done()

	metrics.ProxyRequestCount.WithLabelValues(r.Method).Add(1)
	metrics.ProxyRequestRunningCount.WithLabelValues(r.Method).Add(1)
	defer metrics.ProxyRequestRunningCount.WithLabelValues(r.Method).Sub(1)

	ctx, span := proxy.tracer.Start(r.Context(), config.SpanProxy)
	span.SetAttributes(config.AttributePeerHost.String(proxy.peerHost.Id))
	span.SetAttributes(semconv.NetHostIPKey.String(proxy.peerHost.Ip))
	span.SetAttributes(semconv.HTTPSchemeKey.String(r.URL.Scheme))
	span.SetAttributes(semconv.HTTPHostKey.String(r.Host))
	span.SetAttributes(semconv.HTTPURLKey.String(r.URL.String()))
	span.SetAttributes(semconv.HTTPMethodKey.String(r.Method))

	defer span.End()

	// update ctx to transfer trace id
	r = r.WithContext(ctx)

	// check authenticity
	if proxy.basicAuth != nil {
		user, pass, ok := proxyBasicAuth(r)
		if !ok {
			status := http.StatusProxyAuthRequired
			span.SetAttributes(semconv.HTTPStatusCodeKey.Int(status))
			http.Error(w, http.StatusText(status), status)
			logger.Debugf("empty auth info: %s, url：%s", r.Host, r.URL.String())
			return
		}
		// TODO dynamic auth config via manager
		if !isBasicAuthMatch(proxy.basicAuth, user, pass) {
			status := http.StatusUnauthorized
			span.SetAttributes(semconv.HTTPStatusCodeKey.Int(status))
			http.Error(w, http.StatusText(status), status)
			logger.Debugf("mismatch auth info (%s/%s): %s, url：%s", user, pass, r.Host, r.URL.String())
			return
		}
	}

	// check direct request
	directRequest := r.Method != http.MethodConnect && r.URL.Scheme == ""

	// check whiteList
	if !directRequest && !proxy.checkWhiteList(r) {
		status := http.StatusUnauthorized
		span.SetAttributes(semconv.HTTPStatusCodeKey.Int(status))
		http.Error(w, http.StatusText(status), status)
		logger.Debugf("not in whitelist: %s, url：%s", r.Host, r.URL.String())
		return
	}

	// limit max concurrency
	if proxy.semaphore != nil {
		err := proxy.semaphore.Acquire(r.Context(), 1)
		if err != nil {
			logger.Errorf("acquire semaphore error: %v", err)
			http.Error(w, err.Error(), http.StatusTooManyRequests)
			return
		}
		defer proxy.semaphore.Release(1)
	}

	if r.Method == http.MethodConnect {
		// handle https proxy requests
		proxy.handleHTTPS(w, r)
	} else if r.URL.Scheme == "" {
		// handle direct requests
		proxy.directHandler.ServeHTTP(w, r)
	} else {
		// handle http proxy requests
		proxy.handleHTTP(span, w, r)
	}
}

func proxyBasicAuth(r *http.Request) (username, password string, ok bool) {
	auth := r.Header.Get(headers.ProxyAuthorization)
	if auth == "" {
		return
	}
	return parseBasicAuth(auth)
}

// parseBasicAuth parses an HTTP Basic Authentication string.
// "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==" returns ("Aladdin", "open sesame", true).
func parseBasicAuth(auth string) (username, password string, ok bool) {
	const prefix = "Basic "
	// Case-insensitive prefix match. See Issue 22736.
	if len(auth) < len(prefix) || !strings.EqualFold(auth[:len(prefix)], prefix) {
		return
	}
	c, err := base64.StdEncoding.DecodeString(auth[len(prefix):])
	if err != nil {
		return
	}
	cs := string(c)
	s := strings.IndexByte(cs, ':')
	if s < 0 {
		return
	}
	return cs[:s], cs[s+1:], true
}

func (proxy *Proxy) handleHTTP(span trace.Span, w http.ResponseWriter, req *http.Request) {
	resp, err := proxy.transport.RoundTrip(req)
	if err != nil {
		span.RecordError(err)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()
	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	span.SetAttributes(semconv.HTTPStatusCodeKey.Int(resp.StatusCode))
	if n, err := io.Copy(w, resp.Body); err != nil && err != io.EOF {
		if peerID := resp.Header.Get(config.HeaderDragonflyPeer); peerID != "" {
			logger.Errorf("failed to write http body: %v, peer: %s, task: %s, written bytes: %d",
				err, peerID, resp.Header.Get(config.HeaderDragonflyTask), n)
		} else {
			logger.Errorf("failed to write http body: %v, written bytes: %d", err, n)
		}
		span.RecordError(err)
	} else {
		span.SetAttributes(semconv.HTTPResponseContentLengthKey.Int64(n))
		// when resp.ContentLength == -1 or 0, byte count can not be updated by transport
		if resp.ContentLength == -1 {
			metrics.ProxyRequestBytesCount.WithLabelValues(req.Method).Add(float64(n))
		}
	}
}

func (proxy *Proxy) handleHTTPS(w http.ResponseWriter, r *http.Request) {
	if proxy.cert == nil {
		logger.Debugf("proxy cert is not configured, tunneling https request for %s", r.Host)
		tunnelHTTPS(w, r)
		return
	}

	cConfig := proxy.remoteConfig(r.Host)
	if cConfig == nil {
		logger.Debugf("hijackHTTPS hosts not match, tunneling https request for %s", r.Host)
		tunnelHTTPS(w, r)
		return
	}

	logger.Debugf("hijack https request to %s", r.Host)

	sConfig := new(tls.Config)
	if proxy.cert.Leaf != nil && proxy.cert.Leaf.IsCA {
		leafCertSpec := LeafCertSpec{
			proxy.cert.Leaf.PublicKey,
			proxy.cert.PrivateKey,
			proxy.cert.Leaf.SignatureAlgorithm}
		host, _, _ := net.SplitHostPort(r.Host)
		sConfig.GetCertificate = func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			cConfig.ServerName = host
			// It's assumed that `hello.ServerName` is always same as `host`, in practice.
			cacheKey := host
			proxy.cacheRWMutex.RLock()
			cached, hit := proxy.certCache.Get(cacheKey)
			proxy.cacheRWMutex.RUnlock()
			if hit && time.Now().Before(cached.(*tls.Certificate).Leaf.NotAfter) { // If cache hit and the cert is not expired
				logger.Debugf("TLS cert cache hit, cacheKey = <%s>", cacheKey)
				return cached.(*tls.Certificate), nil
			}
			logger.Debugf("Generate temporal leaf TLS cert for ServerName <%s>, host <%s>", hello.ServerName, host)
			cert, err := genLeafCert(proxy.cert, &leafCertSpec, host)
			if err == nil {
				// Put cert in cache only if there is no error. So all certs in cache are always valid.
				// But certs in cache maybe expired (After 24 hours, see the default duration of generated certs)
				proxy.cacheRWMutex.Lock()
				proxy.certCache.Add(cacheKey, cert)
				proxy.cacheRWMutex.Unlock()
			}
			// If err != nil, means unrecoverable error happened in genLeafCert(...)
			return cert, err
		}
	} else {
		sConfig.Certificates = []tls.Certificate{*proxy.cert}
	}

	// TODO support http2 by set sConfig.NextProtos = []string{"http/1.1", "h2"}
	// then check conn.ConnectionState().NegotiatedProtocol in handshake(w, sConfig)
	// example https://github.com/google/martian/blob/v3.2.1/proxy.go#L337
	sConn, err := handshake(w, sConfig)
	if err != nil {
		logger.Errorf("handshake failed for %s: %v", r.Host, err)
		return
	}
	defer sConn.Close()

	// confirm remote is valid
	cConn, err := tls.Dial("tcp", r.Host, cConfig)
	if err != nil {
		logger.Errorf("dial failed for %s: %v", r.Host, err)
		return
	}
	cConn.Close()

	rp := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			// we can not change req.ctx in Director, so inject trace with header
			propagation.TraceContext{}.Inject(r.Context(), propagation.HeaderCarrier(req.Header))
			req.URL.Host = req.Host
			req.URL.Scheme = schemaHTTPS
			if proxy.dumpHTTPContent {
				if out, e := httputil.DumpRequest(req, false); e == nil {
					logger.Debugf("dump request in ReverseProxy: %s", string(out))
				} else {
					logger.Errorf("dump request in ReverseProxy error: %s", e)
				}
			}
		},
		Transport: proxy.newTransport(cConfig),
	}

	// We have to wait until the connection is closed
	wg := sync.WaitGroup{}
	wg.Add(1)
	// NOTE: http.Serve always returns a non-nil error
	err = http.Serve(&singleUseListener{&customCloseConn{sConn, wg.Done}}, rp)
	if err != errServerClosed && err != http.ErrServerClosed {
		logger.Errorf("failed to accept incoming HTTP connections: %v", err)
	}
	wg.Wait()
}

func (proxy *Proxy) newTransport(tlsConfig *tls.Config) http.RoundTripper {
	rt, _ := transport.New(
		transport.WithPeerIDGenerator(proxy.peerIDGenerator),
		transport.WithPeerTaskManager(proxy.peerTaskManager),
		transport.WithTLS(tlsConfig),
		transport.WithCondition(proxy.shouldUseDragonfly),
		transport.WithDefaultFilter(proxy.defaultFilter),
		transport.WithDefaultTag(proxy.defaultTag),
		transport.WithDefaultApplication(proxy.defaultApplication),
		transport.WithDefaultPriority(proxy.defaultPriority),
		transport.WithDumpHTTPContent(proxy.dumpHTTPContent),
	)
	return rt
}

func (proxy *Proxy) mirrorRegistry(w http.ResponseWriter, r *http.Request) {
	reverseProxy := newReverseProxy(proxy.registry)
	t, err := transport.New(
		transport.WithPeerIDGenerator(proxy.peerIDGenerator),
		transport.WithPeerTaskManager(proxy.peerTaskManager),
		transport.WithTLS(proxy.registry.TLSConfig()),
		transport.WithCondition(proxy.shouldUseDragonflyForMirror),
		transport.WithDefaultFilter(proxy.defaultFilter),
		transport.WithDefaultTag(proxy.defaultTag),
		transport.WithDefaultApplication(proxy.defaultApplication),
		transport.WithDefaultPriority(proxy.defaultPriority),
		transport.WithDumpHTTPContent(proxy.dumpHTTPContent),
	)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to get transport: %v", err), http.StatusInternalServerError)
	}

	reverseProxy.Transport = t
	reverseProxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		rw.WriteHeader(http.StatusInternalServerError)
		// write error string to response body
		if _, err := rw.Write([]byte(err.Error())); err != nil {
			logger.Errorf("write error string to response body failed %s", err)
		}
	}
	reverseProxy.ServeHTTP(w, r)
}

// remoteConfig returns the tls.Config used to connect to the given remote host.
// If the host should not be hijacked, and it will return nil.
func (proxy *Proxy) remoteConfig(host string) *tls.Config {
	for _, h := range proxy.httpsHosts {
		if h.Regx.MatchString(host) {
			tlsConfig := &tls.Config{InsecureSkipVerify: h.Insecure}
			if h.Certs != nil {
				tlsConfig.RootCAs = h.Certs.CertPool
			}
			return tlsConfig
		}
	}
	return nil
}

// checkWhiteList check proxy white list.
func (proxy *Proxy) checkWhiteList(r *http.Request) bool {
	whiteList := proxy.whiteList
	host := r.URL.Hostname()
	port := r.URL.Port()

	// No whitelist
	if len(whiteList) <= 0 {
		return true
	}

	for _, v := range whiteList {
		if (v.Host != "" && v.Host == host) || (v.Regx != nil && v.Regx.MatchString(host)) {
			// No ports
			if len(v.Ports) <= 0 {
				return true
			}

			// Hit ports
			if pkgstrings.Contains(v.Ports, port) {
				return true
			}

			return false
		}
	}

	return false
}

// shouldUseDragonfly returns whether we should use dragonfly to proxy a request. It
// also changes the scheme of the given request if the matched rule has
// UseHTTPS = true
func (proxy *Proxy) shouldUseDragonfly(req *http.Request) bool {
	for _, rule := range proxy.rules.Load().([]*config.ProxyRule) {
		if rule.Match(req.URL.String()) {
			if rule.UseHTTPS {
				req.URL.Scheme = schemaHTTPS
			}
			if strings.Contains(rule.Redirect, "/") {
				u, err := url.Parse(rule.Regx.ReplaceAllString(req.URL.String(), rule.Redirect))
				if err != nil {
					logger.Errorf("failed to rewrite url", err)
					return false
				}
				req.URL = u
				req.Host = req.URL.Host
				req.RequestURI = req.URL.RequestURI()
			} else if rule.Redirect != "" {
				req.URL.Host = rule.Redirect
				req.Host = rule.Redirect
			}

			if req.Method != http.MethodGet {
				return false
			}
			return !rule.Direct
		}
	}
	return false
}

// shouldUseDragonflyForMirror returns whether we should use dragonfly to proxy a request
// when we use registry mirror.
func (proxy *Proxy) shouldUseDragonflyForMirror(req *http.Request) bool {
	if proxy.registry == nil || proxy.registry.Direct {
		return false
	}
	if proxy.registry.UseProxies {
		return proxy.shouldUseDragonfly(req)
	}
	return transport.NeedUseDragonfly(req)
}

// tunnelHTTPS handles the CONNECT request and proxy the https request through http tunnel.
func tunnelHTTPS(w http.ResponseWriter, r *http.Request) {
	metrics.ProxyRequestNotViaDragonflyCount.Add(1)
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

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		if _, err := io.Copy(dst, clientConn); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			logger.Errorf("copy hijacked stream from client to destination error: %s", err)
		}
		wg.Done()
	}()

	if _, err := io.Copy(clientConn, dst); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		logger.Errorf("copy hijacked stream from destination to client error: %s", err)
	}
	wg.Wait()

	// Close() will close both read and write, we need wait all stream is done, then close connections
	if err = dst.Close(); err != nil {
		logger.Errorf("close hijacked destination error: %s", err)
	}
	if err = clientConn.Close(); err != nil {
		logger.Errorf("close hijacked client error: %s", err)
	}
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
