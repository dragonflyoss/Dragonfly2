package config

import (
	"fmt"
	"strings"

	"github.com/docker/go-units"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
)

// SchedulersValue implements the pflag.Value interface.
type SchedulersValue struct {
	ph *PeerHostOption
}

func NewSchedulersValue(ph *PeerHostOption) *SchedulersValue {
	return &SchedulersValue{ph: ph}
}

func (sv *SchedulersValue) String() string {
	var result []string
	for _, v := range sv.ph.Schedulers {
		result = append(result, v.Addr)
	}
	return strings.Join(result, ",")
}

func (sv *SchedulersValue) Set(value string) error {
	addresses := strings.Split(value, ",")
	for _, address := range addresses {
		vv := strings.Split(address, ":")
		if len(vv) > 2 || len(vv) == 0 {
			return errors.New("invalid schedulers")
		}
		if len(vv) == 1 {
			address = fmt.Sprintf("%s:%d", address, DefaultSupernodePort)
		}
		sv.ph.Schedulers = append(sv.ph.Schedulers,
			dfnet.NetAddr{
				Type: dfnet.TCP,
				Addr: address,
			})
	}
	return nil
}

func (sv *SchedulersValue) Type() string {
	return "schedulers"
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
