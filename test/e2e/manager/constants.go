package manager

import "fmt"

const (
	CDNCachePath = "/tmp/cdn/download"
	Service      = "dragonfly-manager.dragonfly-system.svc"
	Port         = "8080"
	PreheatPath  = "api/v1/preheats"
)

var (
	PreheatURL = fmt.Sprintf("http://%s:%s/%s", Service, Port, PreheatPath)
)
