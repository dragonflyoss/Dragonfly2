package tasks

import "fmt"

type Queue string

const (
	GlobalQueue     Queue = "global"
	SchedulersQueue Queue = "schedulers"
	CDNsQueue       Queue = "cdns"
)

func GetSchedulerQueue(hostname string) Queue {
	return Queue(fmt.Sprintf("scheduler_%s", hostname))
}

func GetCDNQueue(hostname string) Queue {
	return Queue(fmt.Sprintf("cdn_%s", hostname))
}

func (q Queue) String() string {
	return string(q)
}
