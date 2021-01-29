package types

import (
	"crypto/sha1"
	"fmt"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"net/url"
	"strings"
)

func GenerateTaskId(rawUrl string, filter string) (taskId string) {
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
			queries = url.Values{}
			for _, key := range fields {
				queries.Add(key, taskUrl.Query().Get(key))
			}
		}
		taskUrl.RawQuery = queries.Encode()
	}
	taskId = fmt.Sprintf("%x", sha1.Sum([]byte(taskUrl.String())))
	return
}
