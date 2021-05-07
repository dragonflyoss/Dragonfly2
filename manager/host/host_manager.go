package host

import (
	"context"

	"d7y.io/dragonfly/v2/manager/config"
)

type HostInfo struct {
	HostName       string
	Ip             string
	SecurityDomain string
	Location       string
	Idc            string
	NetTopology    string
}

func NewDefaultHostInfo(ip, hostName string) *HostInfo {
	return &HostInfo{
		HostName:       hostName,
		Ip:             ip,
		SecurityDomain: "",
		Location:       "",
		Idc:            "",
		NetTopology:    "",
	}
}

func (host *HostInfo) IsDefault() bool {
	return len(host.SecurityDomain) == 0 && len(host.Idc) == 0
}

type Op struct {
	sn       string
	ip       string
	hostName string
}

type OpOption func(*Op)

func (op *Op) ApplyOpts(opts []OpOption) {
	for _, opt := range opts {
		opt(op)
	}
}

func WithSn(sn string) OpOption {
	return func(op *Op) {
		op.sn = sn
	}
}

func WithIp(ip string) OpOption {
	return func(op *Op) {
		op.ip = ip
	}
}

func WithHostName(hostName string) OpOption {
	return func(op *Op) {
		op.hostName = hostName
	}
}

type HostManager interface {
	GetHostInfo(ctx context.Context, opts ...OpOption) (*HostInfo, error)
}

func NewHostManager(config *config.HostService) (HostManager, error) {
	if config.Skyline != nil {
		return NewSkyline(config.Skyline)
	}

	return NewDefaultHostManager()
}
