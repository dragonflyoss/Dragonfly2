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
	"os"
)

var (
	// IPv4 is external address.
	IPv4 net.IP

	// IPv4lookback is lookback address.
	IPv4lookback = net.ParseIP("127.0.0.1")
)

func init() {
	ip, err := externalIPv4()
	if err != nil {
		IPv4 = IPv4lookback
	} else {
		IPv4 = ip
	}
}

// externalIPv4 returns the available IPv4.
func externalIPv4() (net.IP, error) {
	ips, err := ipAddrs()
	if err != nil {
		return nil, err
	}

	var externalIPs []net.IP
	for _, ip := range ips {
		ip = ip.To4()
		if ip == nil {
			continue // not an ipv4 address
		}
		externalIPs = append(externalIPs, ip)
	}

	if len(externalIPs) == 0 {
		return nil, fmt.Errorf("can not found external ipv4")
	}

	return externalIPs[0], nil
}

// ipAddrs returns all the valid IPs available.
// refer to https://github.com/dragonflyoss/Dragonfly2/pull/652
func ipAddrs() ([]net.IP, error) {
	// use prefer ip possible
	var prefers []net.IP
	host, err := os.Hostname()
	if err == nil {
		ips, _ := net.LookupIP(host)
		for _, ip := range ips {
			if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				continue
			}
			prefers = append(prefers, ip)
		}
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	var preferIPs []net.IP
	var normalIPs []net.IP
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}

		addrs, err := iface.Addrs()
		if err != nil {
			return nil, err
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				continue
			}

			var isPrefer bool
			for _, preferAddr := range prefers {
				if preferAddr.Equal(ip) {
					isPrefer = true
				}
			}

			if isPrefer {
				preferIPs = append(preferIPs, ip)
			} else {
				normalIPs = append(normalIPs, ip)
			}
		}
	}

	return append(preferIPs, normalIPs...), nil
}
