package config

import (
	"fmt"
	"strings"

	"github.com/docker/go-units"
	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
)

type NetAddrsValue struct {
	n *[]dfnet.NetAddr
}

// NetAddrsValue implements the pflag.Value interface.
func NewNetAddrsValue(n *[]dfnet.NetAddr) *NetAddrsValue {
	return &NetAddrsValue{n: n}
}

func (nv *NetAddrsValue) String() string {
	var result []string
	for _, v := range *nv.n {
		result = append(result, v.Addr)
	}
	return strings.Join(result, ",")
}

func (nv *NetAddrsValue) Set(value string) error {
	*nv.n = append(*nv.n, dfnet.NetAddr{
		Type: dfnet.TCP,
		Addr: value,
	})
	return nil
}

func (nv *NetAddrsValue) Type() string {
	return "netaddrs"
}

type RateLimitValue struct {
	rate *clientutil.RateLimit
}

func NewLimitRateValue(rate *clientutil.RateLimit) *RateLimitValue {
	return &RateLimitValue{rate: rate}
}

func (r *RateLimitValue) String() string {
	return fmt.Sprintf("%f", r.rate.Limit)
}

func (r *RateLimitValue) Set(s string) error {
	bs, err := units.RAMInBytes(s)
	if err != nil {
		return err
	}
	r.rate.Limit = rate.Limit(bs)
	return nil
}

func (r *RateLimitValue) Type() string {
	return "ratelimit"
}
