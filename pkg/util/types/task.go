package types

import (
	"crypto/sha1"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"fmt"
	"net/url"
	"strings"
)

func GenerateTaskId(rawUrl string, filter string, meta *base.UrlMeta, bizId string) (taskId string) {
	taskUrl, err := url.Parse(rawUrl)
	if err != nil {
		logger.Warnf("GenerateTaskId rawUrl[%s] is invalid url", rawUrl)
		taskId = fmt.Sprintf("%x", sha1.Sum([]byte(rawUrl)))
		return
	}
	if filter != "" {
		queries := taskUrl.Query()
		fields := strings.Split(filter, "&")
		if len(fields) > 0 {
			for _, key := range fields {
				queries.Del(key)
			}
		}
		taskUrl.RawQuery = queries.Encode()
	}
	taskRawString := taskUrl.String()
	if meta != nil {
		taskRawString += "_" + meta.Range + "_" + meta.Md5 + "_" + bizId
	}
	taskId = fmt.Sprintf("%x", sha1.Sum([]byte(taskRawString)))
	return
}

func GenerateABTestTaskId(rawUrl string, filter string, meta *base.UrlMeta, bizId string, peerId string) (taskId string) {
	taskId = GenerateTaskId(rawUrl, filter, meta, bizId)
	b := sha1.Sum([]byte(peerId))[sha1.Size-1]
	if b & 1 == 1 {
		taskId += "TB"
	}
	return
}
