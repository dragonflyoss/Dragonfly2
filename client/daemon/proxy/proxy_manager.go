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
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"reflect"

	"github.com/pkg/errors"
	"github.com/spf13/viper"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

type Manager interface {
	Serve(net.Listener) error
	ServeSNI(net.Listener) error
	Stop() error
	IsEnabled() bool
}

type proxyManager struct {
	*http.Server
	*Proxy
	config.ListenOption
}

var _ Manager = (*proxyManager)(nil)

func NewProxyManager(peerHost *scheduler.PeerHost, peerTaskManager peer.TaskManager, defaultPattern base.Pattern, opts *config.ProxyOption) (Manager, error) {
	// proxy is option, when nil, just disable it
	if opts == nil {
		logger.Infof("proxy config is empty, disabled")
		return &proxyManager{}, nil
	}
	registry := opts.RegistryMirror
	proxies := opts.Proxies
	hijackHTTPS := opts.HijackHTTPS
	whiteList := opts.WhiteList

	options := []Option{
		WithPeerHost(peerHost),
		WithPeerIDGenerator(peer.NewPeerIDGenerator(peerHost.Ip)),
		WithPeerTaskManager(peerTaskManager),
		WithRules(proxies),
		WithWhiteList(whiteList),
		WithMaxConcurrency(opts.MaxConcurrency),
		WithDefaultFilter(opts.DefaultFilter),
		WithDefaultPattern(defaultPattern),
		WithBasicAuth(opts.BasicAuth),
		WithDumpHTTPContent(opts.DumpHTTPContent),
	}

	if registry != nil {
		logger.Infof("registry mirror: %s", registry.Remote)
		options = append(options, WithRegistryMirror(registry))
	}

	if len(proxies) > 0 {
		logger.Infof("load %d proxy rules", len(proxies))
		for i, r := range proxies {
			method := "with dragonfly"
			if r.Direct {
				method = "directly"
			}
			scheme := ""
			if r.UseHTTPS {
				scheme = "and force https"
			}
			logger.Infof("[%d] proxy %s %s %s", i+1, r.Regx, method, scheme)
		}
	}

	if hijackHTTPS != nil {
		options = append(options, WithHTTPSHosts(hijackHTTPS.Hosts...))
		if hijackHTTPS.Cert != "" && hijackHTTPS.Key != "" {
			cert, err := certFromFile(hijackHTTPS.Cert, hijackHTTPS.Key)
			if err != nil {
				return nil, errors.Wrap(err, "cert from file")
			}
			if cert.Leaf != nil && cert.Leaf.IsCA {
				logger.Debugf("hijack https request with CA <%s>", cert.Leaf.Subject.CommonName)
			}
			options = append(options, WithCert(cert))
		}
	}

	p, err := NewProxy(options...)
	if err != nil {
		return nil, errors.Wrap(err, "create proxy")
	}

	return &proxyManager{
		Server:       &http.Server{},
		Proxy:        p,
		ListenOption: opts.ListenOption,
	}, nil
}

func (pm *proxyManager) Serve(listener net.Listener) error {
	_ = WithDirectHandler(newDirectHandler())(pm.Proxy)
	pm.Server.Handler = pm.Proxy
	return pm.Server.Serve(listener)
}

func (pm *proxyManager) ServeSNI(listener net.Listener) error {
	return pm.Proxy.ServeSNI(listener)
}

func (pm *proxyManager) Stop() error {
	return pm.Server.Shutdown(context.Background())
}

func (pm *proxyManager) IsEnabled() bool {
	return pm.ListenOption.TCPListen != nil && pm.ListenOption.TCPListen.PortRange.Start != 0
}

func newDirectHandler() *http.ServeMux {
	s := http.DefaultServeMux
	s.HandleFunc("/args", getArgs)
	s.HandleFunc("/env", getEnv)
	return s
}

// getEnv returns the environments of dfdaemon.
func getEnv(w http.ResponseWriter, r *http.Request) {
	logger.Debugf("access: %s", r.URL.String())
	if err := json.NewEncoder(w).Encode(ensureStringKey(viper.AllSettings())); err != nil {
		logger.Errorf("failed to encode env json: %v", err)
	}
}

// ensureStringKey recursively ensures all maps in the given interface are string,
// to make the result marshalable by json. This is meant to be used with viper
// settings, so only maps and slices are handled.
func ensureStringKey(obj interface{}) interface{} {
	rt, rv := reflect.TypeOf(obj), reflect.ValueOf(obj)
	switch rt.Kind() {
	case reflect.Map:
		res := make(map[string]interface{})
		for _, k := range rv.MapKeys() {
			res[fmt.Sprintf("%v", k.Interface())] = ensureStringKey(rv.MapIndex(k).Interface())
		}
		return res
	case reflect.Slice:
		res := make([]interface{}, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			res[i] = ensureStringKey(rv.Index(i).Interface())
		}
		return res
	}
	return obj
}

// getArgs returns all the arguments of command-line except the program name.
func getArgs(w http.ResponseWriter, r *http.Request) {
	logger.Debugf("access: %s", r.URL.String())

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/plain;charset=utf-8")
	for index, value := range os.Args {
		if index > 0 {
			if _, err := w.Write([]byte(value + " ")); err != nil {
				logger.Errorf("failed to respond information: %v", err)
			}
		}

	}
}

func certFromFile(certFile string, keyFile string) (*tls.Certificate, error) {

	// cert.Certificate is a chain of one or more certificates, leaf first.
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, errors.Wrap(err, "load cert")
	}
	logger.Infof("use self-signed certificate (%s, %s) for https hijacking", certFile, keyFile)

	// leaf is CA cert or server cert
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, errors.Wrap(err, "load leaf cert")
	}

	cert.Leaf = leaf
	return &cert, nil
}
