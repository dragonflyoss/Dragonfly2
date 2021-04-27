package host

type empty struct {
}

func (empty *empty) GetHostInfo(sn, ip, hostName, alias string) (*HostInfo, error) {
	return &HostInfo{
		HostName:       hostName,
		Ip:             ip,
		SecurityDomain: "",
		Location:       "",
		Idc:            "",
		NetTopology:    "",
	}, nil
}

func NewEmptyHostManager() (HostManager, error) {
	return &empty{}, nil
}
