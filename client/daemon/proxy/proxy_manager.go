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
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"reflect"

	"github.com/pkg/errors"
	"github.com/spf13/viper"

	"github.com/dragonflyoss/Dragonfly2/client/config"
	"github.com/dragonflyoss/Dragonfly2/client/daemon/peer"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
)

type Manager interface {
	Serve(lis net.Listener) error
	Stop() error
}

type proxyManager struct {
	*http.Server
	*Proxy
}

func NewProxyManager(registry *config.RegistryMirror, proxies []*config.Proxy, hijackHTTPS *config.HijackConfig, peerTaskManager peer.PeerTaskManager) (Manager, error) {
	opts := []ProxyOption{
		WithRules(proxies),
		WithRegistryMirror(registry),
	}

	logger.Infof("registry mirror: %s", registry.Remote)

	if len(proxies) > 0 {
		logger.Infof("%d proxy rules loaded", len(proxies))
		for i, r := range proxies {
			method := "with dfget"
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
		opts = append(opts, WithHTTPSHosts(hijackHTTPS.Hosts...))
		if hijackHTTPS.Cert != "" && hijackHTTPS.Key != "" {
			opts = append(opts, WithCertFromFile(hijackHTTPS.Cert, hijackHTTPS.Key))
		}
	}

	p, err := NewProxy(peerTaskManager, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "create proxy")
	}

	return &proxyManager{
		Server: &http.Server{},
		Proxy:  p,
	}, nil
}

func (pm *proxyManager) Serve(lis net.Listener) error {
	_ = WithDirectHandler(newDirectHandler())(pm.Proxy)
	pm.Server.Handler = pm.Proxy
	return pm.Server.Serve(lis)
}

func (pm *proxyManager) Stop() error {
	logger.Warnf("TODO not implement")
	return nil
}

func newDirectHandler() *http.ServeMux {
	s := http.DefaultServeMux
	s.HandleFunc("/args", getArgs)
	s.HandleFunc("/env", getEnv)
	return s
}

// getEnv returns the environments of dfdaemon.
func getEnv(w http.ResponseWriter, r *http.Request) {
	logger.Debugf("access:%s", r.URL.String())
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
	logger.Debugf("access:%s", r.URL.String())

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
