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

package netutils

import (
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"d7y.io/dragonfly/v2/pkg/ratelimiter"

	"d7y.io/dragonfly/v2/pkg/util/stringutils"
)

const (
	separator = "&"
	layoutGMT = "GMT"
)


// FilterURLParam filters request queries in URL.
// Eg:
// If you pass parameters as follows:
//     url: http://a.b.com/locate?key1=value1&key2=value2&key3=value3
//     filter: key2
// and then you will get the following value as the return:
//     http://a.b.com/locate?key1=value1&key3=value3
func FilterURLParam(url string, filters []string) string {
	rawUrls := strings.SplitN(url, "?", 2)
	if len(filters) <= 0 || len(rawUrls) != 2 || strings.TrimSpace(rawUrls[1]) == "" {
		return url
	}
	filtersMap := slice2Map(filters)

	var params []string
	for _, param := range strings.Split(rawUrls[1], separator) {
		kv := strings.SplitN(param, "=", 2)
		if !(len(kv) >= 1 && isExist(filtersMap, kv[0])) {
			params = append(params, param)
		}
	}
	if len(params) > 0 {
		return rawUrls[0] + "?" + strings.Join(params, separator)
	}
	return rawUrls[0]
}

// IsValidURL returns whether the string url is a valid HTTP URL.
func IsValidURL(urlStr string) bool {
	u, err := url.Parse(urlStr)
	if err != nil {
		return false
	}
	if len(u.Host) == 0 || len(u.Scheme) == 0 {
		return false
	}

	// with custom schemas, url like "x://y/z" is valid
	reg := regexp.MustCompile(`(` +
		"https?|HTTPS?" +
		`)://([\w_]+:[\w_]+@)?([\w-]+\.)*[\w-]+(/[\w- ./?%&=]*)?`)
	if result := reg.FindString(urlStr); stringutils.IsBlank(result) {
		return false
	}
	return true
}

// IsValidIP returns whether the string ip is a valid IP Address.
func IsValidIP(ip string) bool {
	if strings.TrimSpace(ip) == "" {
		return false
	}

	// str is a regex which matches a digital
	// greater than or equal to 0 and less than or equal to 255
	str := "(?:25[0-5]|2[0-4]\\d|[01]?\\d?\\d)"
	result, err := regexp.MatchString("^(?:"+str+"\\.){3}"+str+"$", ip)
	if err != nil {
		return false
	}

	return result
}

// GetAllIPs returns all non-loopback IPV4 addresses.
func GetAllIPs() (ipList []string, err error) {
	// get all system's unicast interface addresses.
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	// filter all loopback addresses.
	for _, v := range addrs {
		if ipNet, ok := v.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				ipList = append(ipList, ipNet.IP.String())
			}
		}
	}
	return
}

// slice2Map translates a slice to a map with
// the value in slice as the key and true as the value.
func slice2Map(value []string) map[string]bool {
	mmap := make(map[string]bool)
	for _, v := range value {
		mmap[v] = true
	}
	return mmap
}
