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
	cc *cdnConfig
}

func NewCdnValue(cc *cdnConfig) *CdnValue {
	return &CdnValue{cc: cc}
}

func (cv *CdnValue) String() string {
	var result []string
	for _, group := range cv.cc.List {
		var subResult []string
		for _, v := range group {
			subResult = append(subResult, fmt.Sprintf("%s:%s:%d:%d", v.CdnName, v.IP, v.RpcPort, v.DownloadPort))
		}
		result = append(result, strings.Join(subResult, ","))
	}
	return strings.Join(result, "|")
}

func (cv *CdnValue) Set(value string) error {
	cv.cc.List = cv.cc.List[:0]
	cdnList := strings.Split(value, "|")
	for _, addresses := range cdnList {
		addrs := strings.Split(addresses, ",")
		var cdnList []CdnServerConfig
		for _, address := range addrs {
			vv := strings.Split(address, ":")
			if len(vv) != 4 {
				return errors.New("invalid cdn address")
			}
			rpcPort, _ := strconv.Atoi(vv[2])
			downloadPort, _ := strconv.Atoi(vv[3])
			cdnList = append(cdnList, CdnServerConfig{
				CdnName:      vv[0],
				IP:           vv[1],
				RpcPort:      rpcPort,
				DownloadPort: downloadPort,
			})
		}
		cv.cc.List = append(cv.cc.List, cdnList)
	}
	return nil
}

func (cv *CdnValue) Type() string {
	return "cdn-list"
}
