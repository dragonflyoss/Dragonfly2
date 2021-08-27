package e2e

const (
	CDNCachePath = "/tmp/cdn/download"

	ManagerService = "dragonfly-manager.dragonfly-system.svc"
	ManagerPort    = "8080"
	PreheatPath    = "api/v1/preheats"

	DragonflyNamespace = "dragonfly-system"
	E2ENamespace       = "dragonfly-e2e"

	Proxy            = "localhost:65001"
	HostnameFilePath = "/etc/hostname"
)
