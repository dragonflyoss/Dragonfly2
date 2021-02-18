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
