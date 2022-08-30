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

package config

import (
	"crypto/tls"

	"github.com/johanbrandhorst/certify"

	"d7y.io/dragonfly/v2/pkg/net/ip"
)

func GetCertificate(certifyClient *certify.Certify) func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	return func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		// FIXME peers need pure ip cert, certify checks the ServerName, so workaround here
		hello.ServerName = ip.IPv4
		return certifyClient.GetCertificate(hello)
	}
}
