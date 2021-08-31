package manager

const (
	cdnCachePath = "/tmp/cdn/download"

	managerService = "dragonfly-manager.dragonfly-system.svc"
	managerPort    = "8080"
	preheatPath    = "api/v1/preheats"
	managerTag     = "d7y/manager"

	dragonflyNamespace = "dragonfly-system"
	e2eNamespace       = "dragonfly-e2e"

	proxy            = "localhost:65001"
	hostnameFilePath = "/etc/hostname"
)
