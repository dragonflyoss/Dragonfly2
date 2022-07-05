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

package fqdn

import (
	"os"

	"github.com/Showmax/go-fqdn"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

var FQDNHostname string

func init() {
	FQDNHostname = fqdnHostname()
}

// Get FQDN hostname
func fqdnHostname() string {
	fqdn, err := fqdn.FqdnHostname()
	if err != nil {
		logger.Warnf("can not found fqdn: %s", err.Error())
		hostname, err := os.Hostname()
		if err != nil {
			panic(err)
		}

		return hostname
	}

	return fqdn
}
