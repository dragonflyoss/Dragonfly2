package host

import "d7y.io/dragonfly/v2/manager/config"

type HostInfo struct {
	HostName       string
	Ip             string
	SecurityDomain string
	Location       string
	Idc            string
	NetTopology    string
}

type HostManager interface {
	GetHostInfo(sn, ip, hostName, alias string) (*HostInfo, error)
}

func NewHostManager(config *config.HostService) (HostManager, error) {
	if config.Skyline != nil {
		return NewSkyline(config.Skyline)
	}

	return NewEmptyHostManager()
}
