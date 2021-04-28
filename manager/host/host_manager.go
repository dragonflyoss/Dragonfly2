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

func NewEmptyHostInfo(ip, hostName string) *HostInfo {
	return &HostInfo{
		HostName:       hostName,
		Ip:             ip,
		SecurityDomain: "",
		Location:       "",
		Idc:            "",
		NetTopology:    "",
	}
}

func (host *HostInfo) IsEmpty() bool {
	return len(host.SecurityDomain) == 0 && len(host.Idc) == 0
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
