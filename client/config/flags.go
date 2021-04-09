package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/docker/go-units"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
)

// SchedulersValue implements the pflag.Value interface.

type NetAddrsValue struct {
	n     *[]dfnet.NetAddr
	isSet bool
}

func NewNetAddrsValue(n *[]dfnet.NetAddr) *NetAddrsValue {
	return &NetAddrsValue{
		n:     n,
		isSet: false,
	}
}

func (nv *NetAddrsValue) String() string {
	var result []string
	for _, v := range *nv.n {
		result = append(result, v.Addr)
	}

	return strings.Join(result, ",")
}

func (nv *NetAddrsValue) Set(value string) error {
	vv := strings.Split(value, ":")
	if len(vv) > 2 || len(vv) == 0 {
		return errors.New("invalid net address")
	}
	if len(vv) == 1 {
		value = fmt.Sprintf("%s:%d", value, DefaultSupernodePort)
	}

	if !nv.isSet && len(*nv.n) > 0 {
		*nv.n = []dfnet.NetAddr{}
		nv.isSet = true
	}

	*nv.n = append(*nv.n,
		dfnet.NetAddr{
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

// DurationValue supports time.Duration format like 30s, 1m30s, 1h
// and also treat integer as seconds
type DurationValue time.Duration

func NewDurationValue(p *time.Duration) *DurationValue {
	return (*DurationValue)(p)
}

func (d *DurationValue) Set(s string) error {
	v, err := time.ParseDuration(s)
	if err == nil {
		*d = DurationValue(v)
		return nil
	}
	// try to convert to integer for seconds by default
	seconds, convErr := strconv.Atoi(s)
	if convErr != nil {
		// just return first err
		return err
	}
	*d = DurationValue(time.Duration(seconds) * time.Second)
	return nil
}

func (d *DurationValue) Type() string {
	return "duration"
}

func (d *DurationValue) String() string { return (*time.Duration)(d).String() }
