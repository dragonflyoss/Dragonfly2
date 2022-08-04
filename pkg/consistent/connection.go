package consistent

import (
	"math/rand"
	"sync"
	"time"

	"go.uber.org/atomic"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

const (
	TemporarilyClosed = connectivity.State(6)
)

// ConnPool manage SubConns of Balancer
type ConnPool interface {
	Get(string) (SubConnWrapper, bool)
	GetRandom() (SubConnWrapper, bool)
	GetBySubConn(conn balancer.SubConn) (SubConnWrapper, bool)
	Set(string, SubConnWrapper)
	Remove(string)
	Len() int
	// Keys return all addresses
	Keys() []string
	// ValidKeys return the addresses list without Bad node
	ValidKeys() []string
	Values() []SubConnWrapper
	Close()
}

// SubConnWrapper wrap SubConn
type SubConnWrapper interface {
	// GetConn return the base SubConn
	GetConn() balancer.SubConn
	Address() string
	State() connectivity.State
	UpdateState(state connectivity.State)
	Connect()
	// Disconnect remove SubConn from ClientConn
	Disconnect()
	Keep()
	// AddUsingCount to prevent GC
	AddUsingCount(int64)
	// GetFailedTime return total connecting failed time, it will be clear to zero when connect success
	GetFailedTime() int64
	NeedGC(interval time.Duration) bool
}

type subConnWrapper struct {
	cc           balancer.ClientConn
	healthCheck  bool
	sc           balancer.SubConn
	addr         string
	state        connectivity.State
	lastKeepTime time.Time
	useCount     *atomic.Int64
	failCount    int64 // no need to be thread-safe
}

func NewSubConnWrapper(conn balancer.ClientConn, addr string, healthCheck bool) (SubConnWrapper, error) {
	scw := &subConnWrapper{
		cc:           conn,
		healthCheck:  healthCheck,
		addr:         addr,
		lastKeepTime: time.Now(),
		useCount:     atomic.NewInt64(0),
	}
	sc, err := scw.cc.NewSubConn([]resolver.Address{{Addr: addr}}, balancer.NewSubConnOptions{HealthCheckEnabled: healthCheck})
	if err != nil {
		return nil, err
	}
	scw.sc = sc
	scw.state = connectivity.Idle
	return scw, nil
}

func (scw *subConnWrapper) GetConn() balancer.SubConn {
	return scw.sc
}

func (scw *subConnWrapper) Address() string {
	return scw.addr
}

func (scw *subConnWrapper) Keep() {
	scw.lastKeepTime = time.Now()
}

func (scw *subConnWrapper) State() connectivity.State {
	return scw.state
}

func (scw *subConnWrapper) AddUsingCount(n int64) {
	scw.useCount.Add(n)
}

func (scw *subConnWrapper) GetFailedTime() int64 {
	return scw.failCount
}

func (scw *subConnWrapper) NeedGC(interval time.Duration) bool {
	if scw.State() != connectivity.Ready {
		return false
	}
	if scw.useCount.Load() > 0 {
		return false
	}
	if scw.lastKeepTime.Add(interval).Before(time.Now()) {
		return true
	}
	return false
}

func (scw *subConnWrapper) UpdateState(state connectivity.State) {
	scw.state = state
	if state == connectivity.TransientFailure {
		scw.failCount++
	}
	if state == connectivity.Ready {
		scw.failCount = 0
	}
}

func (scw *subConnWrapper) Connect() {
	scw.sc.Connect()
	scw.Keep()
}

func (scw *subConnWrapper) Disconnect() {
	scw.cc.RemoveSubConn(scw.sc)
}

func (scw *subConnWrapper) Close() {
	scw.cc.RemoveSubConn(scw.sc)
}

type connPool struct {
	*sync.RWMutex
	cons       map[string]SubConnWrapper
	subCons    map[balancer.SubConn]SubConnWrapper
	rand       *rand.Rand
	gcInterval time.Duration
	exit       chan struct{}
}

func NewConnPool(gcInterval time.Duration) ConnPool {
	cp := &connPool{
		RWMutex:    &sync.RWMutex{},
		cons:       map[string]SubConnWrapper{},
		subCons:    map[balancer.SubConn]SubConnWrapper{},
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
		gcInterval: gcInterval,
		exit:       make(chan struct{}, 1),
	}
	if gcInterval != 0 {
		go cp.Watch()
	}
	return cp
}

func (cp *connPool) Watch() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			for _, scw := range cp.Values() {
				if scw.NeedGC(cp.gcInterval) {
					scw.UpdateState(TemporarilyClosed)
					scw.Disconnect()
				}
			}
		case <-cp.exit:
			return
		}
	}

}

func (cp *connPool) Close() {
	close(cp.exit)
}

func (cp *connPool) Get(s string) (SubConnWrapper, bool) {
	cp.RLock()
	defer cp.RUnlock()
	scw, ok := cp.cons[s]
	return scw, ok
}

func (cp *connPool) GetRandom() (SubConnWrapper, bool) {
	// rand is not thread-safe
	cp.Lock()
	defer cp.Unlock()
	keys := maps.Keys(cp.cons)
	if len(cp.cons) == 0 {
		return nil, false
	}
	i := cp.rand.Intn(len(keys))
	scw, _ := cp.cons[keys[i]]
	return scw, true
}

func (cp *connPool) GetBySubConn(sc balancer.SubConn) (SubConnWrapper, bool) {
	cp.RLock()
	defer cp.RUnlock()
	scw, ok := cp.subCons[sc]
	return scw, ok
}

func (cp *connPool) Set(s string, wrapper SubConnWrapper) {
	cp.Lock()
	defer cp.Unlock()
	cp.cons[s] = wrapper
	cp.subCons[wrapper.GetConn()] = wrapper
	return
}

func (cp *connPool) Remove(s string) {
	cp.Lock()
	defer cp.Unlock()
	scw, ok := cp.cons[s]
	if !ok {
		return
	}
	delete(cp.cons, s)
	delete(cp.subCons, scw.GetConn())
	return
}

func (cp *connPool) Len() int {
	cp.RLock()
	defer cp.RUnlock()
	return len(cp.cons)
}

func (cp *connPool) Keys() []string {
	cp.RLock()
	defer cp.RUnlock()
	return maps.Keys(cp.cons)
}

func (cp *connPool) ValidKeys() []string {
	cp.RLock()
	defer cp.RUnlock()
	addrs := make([]string, 0, len(cp.cons))
	for addr, con := range cp.cons {
		if con.GetFailedTime() < MaxSubConnFailTimes {
			addrs = append(addrs, addr)
		}
	}
	return addrs
}

func (cp *connPool) Values() []SubConnWrapper {
	cp.RLock()
	defer cp.RUnlock()
	return maps.Values(cp.cons)
}
