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

package ip

import (
	"fmt"
	"net"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

var (
	// IPv6 is external address.
	IPv6 string

	// IPv6lookback is lookback address.
	IPv6lookback = net.IPv6loopback.String()
)

func init() {
	ip, err := externalIPv6()
	if err != nil {
		logger.Warnf("%s, use %s as IPv6 address", err.Error(), IPv6lookback)
		IPv6 = IPv6lookback
	} else {
		IPv6 = ip
	}
}

// externalIPv6 returns the available IPv6.
func externalIPv6() (string, error) {
	ips, err := ipAddrs()
	if err != nil {
		return "", err
	}

	var externalIPs []string
	for _, ip := range ips {
		ipv4 := ip.To4()
		if ipv4 != nil {
			continue // skip all ipv4 address
		}

		ipv6 := ip.To16()
		externalIPs = append(externalIPs, ipv6.String())
	}

	if len(externalIPs) == 0 {
		return "", fmt.Errorf("can not found external ipv6")
	}

	return externalIPs[0], nil
}
