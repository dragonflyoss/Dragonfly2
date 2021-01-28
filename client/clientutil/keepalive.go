package clientutil

import (
	"time"

	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
)

type KeepAlive interface {
	Keep()
	Alive(alive time.Duration) bool
}

type keepAlive struct {
	name   string
	access time.Time
}

func NewKeepAlive(name string) KeepAlive {
	return &keepAlive{
		name:   name,
		access: time.Now(),
	}
}

func (k keepAlive) Keep() {
	k.access = time.Now()
	logger.Debugf("update %s keepalive access time: %s", k.access.Format(time.RFC3339))
}

func (k keepAlive) Alive(alive time.Duration) bool {
	var now = time.Now()
	logger.Debugf("%s keepalive check, last access: %s, alive time: %f seconds, current time: %s",
		k.name, k.access.Format(time.RFC3339), alive.Seconds(), now)
	return k.access.Add(alive).After(now)
}
