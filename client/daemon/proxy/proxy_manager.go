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

//go:generate mockgen -destination mocks/proxy_manager_mock.go -source proxy_manager.go -package mocks

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

	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"

	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	logger "d7y.io/dragonfly/v2/internal/dflog"
)

type Manager interface {
	ConfigWatcher
	Serve(net.Listener) error
	ServeSNI(net.Listener) error
	Stop() error
	IsEnabled() bool
}

type ConfigWatcher interface {
	Watch(*config.ProxyOption)
}

type proxyManager struct {
	*http.Server
	*Proxy
	config.ListenOption
}

var _ Manager = (*proxyManager)(nil)

func NewProxyManager(peerHost *schedulerv1.PeerHost, peerTaskManager peer.TaskManager, proxyOption *config.ProxyOption) (Manager, error) {
	// proxy is option, when nil, just disable it
	if proxyOption == nil {
		logger.Infof("proxy config is empty, disabled")
		return &proxyManager{}, nil
	}
	registry := proxyOption.RegistryMirror
	proxyRules := proxyOption.ProxyRules
	hijackHTTPS := proxyOption.HijackHTTPS
	whiteList := proxyOption.WhiteList

	options := []Option{
		WithPeerHost(peerHost),
		WithPeerIDGenerator(peer.NewPeerIDGenerator(peerHost.Ip)),
		WithPeerTaskManager(peerTaskManager),
		WithRules(proxyRules),
		WithWhiteList(whiteList),
		WithMaxConcurrency(proxyOption.MaxConcurrency),
		WithDefaultFilter(proxyOption.DefaultFilter),
		WithDefaultTag(proxyOption.DefaultTag),
		WithDefaultApplication(proxyOption.DefaultApplication),
		WithDefaultPriority(proxyOption.DefaultPriority),
		WithBasicAuth(proxyOption.BasicAuth),
		WithDumpHTTPContent(proxyOption.DumpHTTPContent),
	}

	if registry != nil {
		logger.Infof("registry mirror: %s", registry.Remote)
		options = append(options, WithRegistryMirror(registry))
	}

	if len(proxyRules) > 0 {
		logger.Infof("load %d proxy rules", len(proxyRules))
		for i, r := range proxyRules {
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
				return nil, fmt.Errorf("cert from file: %w", err)
			}
			if cert.Leaf != nil && cert.Leaf.IsCA {
				logger.Debugf("hijack https request with CA <%s>", cert.Leaf.Subject.CommonName)
			}
			options = append(options, WithCert(cert))
		}
	}

	p, err := NewProxy(options...)
	if err != nil {
		return nil, fmt.Errorf("create proxy: %w", err)
	}

	return &proxyManager{
		Server:       &http.Server{},
		Proxy:        p,
		ListenOption: proxyOption.ListenOption,
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
	err := pm.Server.Shutdown(context.Background())
	if err != nil {
		logger.Errorf("proxy shut down error: %s", err)
	}
	pm.Proxy.wg.Wait()
	return err
}

func (pm *proxyManager) IsEnabled() bool {
	return pm.ListenOption.TCPListen != nil && pm.ListenOption.TCPListen.PortRange.Start != 0
}

func (pm *proxyManager) Watch(opt *config.ProxyOption) {
	old, err := yaml.Marshal(pm.Proxy.rules.Load().([]*config.ProxyRule))
	if err != nil {
		logger.Errorf("yaml marshal proxy rules error: %s", err.Error())
		return
	}

	fresh, err := yaml.Marshal(opt.ProxyRules)
	if err != nil {
		logger.Errorf("yaml marshal proxy rules error: %s", err.Error())
		return
	}
	logger.Infof("previous rules: %s", string(old))
	logger.Infof("current rules: %s", string(fresh))
	if string(old) != string(fresh) {
		logger.Infof("update proxy rules")
		pm.Proxy.rules.Store(opt.ProxyRules)
	}
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
func ensureStringKey(obj any) any {
	rt, rv := reflect.TypeOf(obj), reflect.ValueOf(obj)
	switch rt.Kind() {
	case reflect.Map:
		res := make(map[string]any)
		for _, k := range rv.MapKeys() {
			res[fmt.Sprintf("%v", k.Interface())] = ensureStringKey(rv.MapIndex(k).Interface())
		}
		return res
	case reflect.Slice:
		res := make([]any, rv.Len())
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
		return nil, fmt.Errorf("load cert: %w", err)
	}
	logger.Infof("use self-signed certificate (%s, %s) for https hijacking", certFile, keyFile)

	// leaf is CA cert or server cert
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("load leaf cert: %w", err)
	}

	cert.Leaf = leaf
	return &cert, nil
}
