/*
 *     Copyright 2022 The Dragonfly Authors
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

package http

import (
	"fmt"
	"net"
	"net/http"
	"syscall"
	"time"
)

const (
	// DefaultDialTimeout is the default timeout for dialing a http connection.
	DefaultDialTimeout = 30 * time.Second
)

// HeaderToMap coverts request headers to map[string]string.
func HeaderToMap(header http.Header) map[string]string {
	m := make(map[string]string)
	for k, v := range header {
		m[k] = v[0]
	}
	return m
}

// MapToHeader coverts map[string]string to request headers.
func MapToHeader(m map[string]string) http.Header {
	var h = http.Header{}
	for k, v := range m {
		h.Set(k, v)
	}
	return h
}

// PickHeader pick header with key.
func PickHeader(header http.Header, key, defaultValue string) string {
	v := header.Get(key)
	if v != "" {
		header.Del(key)
		return v
	}

	return defaultValue
}

// NewSafeDialer returns a new net.Dialer with safe socket control.
func NewSafeDialer() *net.Dialer {
	return &net.Dialer{
		Timeout:   DefaultDialTimeout,
		DualStack: true,
		Control:   safeSocketControl,
	}
}

// safeSocketControl restricts the socket to only connect to valid addresses.
func safeSocketControl(network string, address string, conn syscall.RawConn) error {
	if !(network == "tcp4" || network == "tcp6") {
		return fmt.Errorf("network type %s is invalid", network)
	}

	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("host %s is invalid", host)
	}

	if !ip.IsGlobalUnicast() {
		return fmt.Errorf("ip %s is invalid", ip.String())
	}

	return nil
}
