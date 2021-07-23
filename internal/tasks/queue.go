package tasks

import "fmt"

type Queue string

func GetSchedulerQueue(hostname string) Queue {
	return Queue(fmt.Sprintf("scheduler_%s", hostname))
}

func GetCDNQueue(hostname string) Queue {
	return Queue(fmt.Sprintf("cdn_%s", hostname))
}

func (q Queue) String() string {
	return string(q)
}
