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
	"fmt"
	"net"
	"net/http"

	"gopkg.in/yaml.v3"

	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	"d7y.io/dragonfly/v2/client/daemon/pex"
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

func NewProxyManager(peerHost *schedulerv1.PeerHost, peerTaskManager peer.TaskManager, peerExchange pex.PeerExchangeServer, proxyOption *config.ProxyOption) (Manager, error) {
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

	if peerExchange != nil {
		options = append(options, WithPeerSearcher(peerExchange.PeerSearchBroadcaster()))
	}

	if len(proxyRules) > 0 {
		logger.Infof("load %d proxy rules", len(proxyRules))
		for i, r := range proxyRules {
			method := "with dragonfly"
			if r.Direct {
				method = "directly"
			}
			prompt := ""
			if r.UseHTTPS {
				prompt = " and force https"
			}
			logger.Infof("[%d] proxy %s %s%s", i+1, r.Regx, method, prompt)
		}
	}

	if hijackHTTPS != nil {
		options = append(options, WithHTTPSHosts(hijackHTTPS.Hosts...))
		if hijackHTTPS.Cert != "" && hijackHTTPS.Key != "" {
			cert, err := certFromFile(string(hijackHTTPS.Cert), string(hijackHTTPS.Key))
			if err != nil {
				return nil, fmt.Errorf("load cert error: %w", err)
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

func certFromFile(certPEM string, keyPEM string) (*tls.Certificate, error) {
	// cert.Certificate is a chain of one or more certificates, leaf first.
	cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	if err != nil {
		return nil, fmt.Errorf("load cert: %w", err)
	}

	logger.Infof("use self-signed certificate for https hijacking")

	// leaf is CA cert or server cert
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("load leaf cert: %w", err)
	}

	cert.Leaf = leaf
	return &cert, nil
}
