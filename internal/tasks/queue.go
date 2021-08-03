package tasks

import (
	"fmt"

	"github.com/pkg/errors"
)

type Queue string

func GetSchedulerQueue(hostname string) (Queue, error) {
	if hostname == "" {
		return Queue(""), errors.New("empty hostname config is not specified")
	}

	return Queue(fmt.Sprintf("scheduler_%s", hostname)), nil
}

func GetCDNQueue(hostname string) (Queue, error) {
	if hostname == "" {
		return Queue(""), errors.New("empty hostname config is not specified")
	}

	return Queue(fmt.Sprintf("cdn_%s", hostname)), nil
}

func (q Queue) String() string {
	return string(q)
}
