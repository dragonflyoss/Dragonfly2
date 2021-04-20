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

package config

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// CdnValue implements the pflag.Value interface.
type CdnValue struct {
	cc *CDNConfig
}

func NewCDNValue(cc *CDNConfig) *CdnValue {
	return &CdnValue{cc: cc}
}

func (cv *CdnValue) String() string {
	var result []string
	for _, cdn := range cv.cc.Servers {
		result = append(result, fmt.Sprintf("%s:%s:%d:%d", cdn.Name, cdn.IP, cdn.RpcPort, cdn.DownloadPort))
	}
	return strings.Join(result, ",")
}

func (cv *CdnValue) Set(value string) error {
	cv.cc.Servers = cv.cc.Servers[:0]
	cdnList := strings.Split(value, ",")

	for _, address := range cdnList {
		vv := strings.Split(address, ":")
		if len(vv) != 4 {
			return errors.New("invalid cdn address")
		}
		rpcPort, _ := strconv.Atoi(vv[2])
		downloadPort, _ := strconv.Atoi(vv[3])
		cv.cc.Servers = append(cv.cc.Servers, CDNServerConfig{
			Name:         vv[0],
			IP:           vv[1],
			RpcPort:      rpcPort,
			DownloadPort: downloadPort,
		})
	}
	return nil
}

func (cv *CdnValue) Type() string {
	return "cdn-servers"
}
