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

package iputils

import (
	"net"
	"os"
	"sort"

	"github.com/pkg/errors"
)

var IPv4 string

func init() {
	ip, err := externalIPv4()
	if err != nil {
		panic(err)
	}

	IPv4 = ip
}

// IsIPv4 returns whether the ip is a valid IPv4 Address.
func IsIPv4(ip string) bool {
	addr := net.ParseIP(ip)
	if addr != nil {
		return addr.To4() != nil
	}

	return false
}

// externalIPv4 returns the first IPv4 available.
func externalIPv4() (string, error) {
	ips, err := ipAddrs()

	if err != nil {
		return "", err
	}

	if len(ips) == 0 {
		return "", errors.Errorf("not found any ip")
	}

	var values []string
	for _, ip := range ips {
		ip = ip.To4()
		if ip == nil {
			continue // not an ipv4 address
		}
		values = append(values, ip.String())
	}

	if len(values) > 0 {
		sort.Strings(values)
		return values[0], nil
	}

	return "", errors.Errorf("not found any ipv4")
}

// ipAddrs returns all the valid IPs available.
// refer to https://github.com/dragonflyoss/Dragonfly2/pull/652
func ipAddrs() ([]net.IP, error) {
	// use prefer ip possible
	var prefer []net.IP
	host, err := os.Hostname()
	if err == nil {
		addrs, _ := net.LookupIP(host)
		for _, addr := range addrs {
			if addr == nil || addr.IsLoopback() || addr.IsLinkLocalUnicast() {
				continue
			}
			prefer = append(prefer, addr)
		}
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	var ipAddrs []net.IP
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
			for _, a := range prefer {
				if a.Equal(ip) {
					isPrefer = true
				}
			}
			if isPrefer {
				ipAddrs = append([]net.IP{ip}, ipAddrs...)
			} else {
				ipAddrs = append(ipAddrs, ip)
			}
		}
	}

	return ipAddrs, nil
}
