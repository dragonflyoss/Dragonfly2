package host

import (
	"context"

	"d7y.io/dragonfly/v2/manager/config"
)

type Info struct {
	HostName       string
	IP             string
	SecurityDomain string
	Location       string
	IDC            string
	NetTopology    string
}

func NewDefaultHostInfo(ip, hostName string) *Info {
	return &Info{
		HostName:       hostName,
		IP:             ip,
		SecurityDomain: "",
		Location:       "",
		IDC:            "",
		NetTopology:    "",
	}
}

func (host *Info) IsDefault() bool {
	return len(host.SecurityDomain) == 0 && len(host.IDC) == 0
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

func WithIP(ip string) OpOption {
	return func(op *Op) {
		op.ip = ip
	}
}

func WithHostName(hostName string) OpOption {
	return func(op *Op) {
		op.hostName = hostName
	}
}

type Manager interface {
	GetHostInfo(ctx context.Context, opts ...OpOption) (*Info, error)
}

func NewManager(config *config.HostService) (Manager, error) {
	return NewDefaultHostManager()
}
