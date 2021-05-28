package lease

import (
	"context"
	"sync"
	"time"

	"d7y.io/dragonfly/v2/manager/store"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
)

type LeaseID string //nolint

func (id LeaseID) String() string {
	return string(id)
}

const (
	// defaultTTL is the assumed lease TTL used for the first keepalive
	// deadline before the actual TTL is known to the client.
	defaultTTL = 5
	// NoLease is a lease ID for the absence of a lease.
	NoLease LeaseID = ""

	// retryConnWait is how long to wait before retrying request due to an error
	retryConnWait = 500 * time.Millisecond
)

type Lessor interface {
	// Grant creates a new lease.
	Grant(ctx context.Context, key, value string, ttl int64) (LeaseID, error)

	// Revoke revokes the given lease.
	Revoke(ctx context.Context, id LeaseID)

	// KeepAlive attempts to keep the given lease alive forever.
	KeepAlive(ctx context.Context, id LeaseID) (chan struct{}, error)

	// Close releases all resources Lease keeps.
	Close() error
}

type keepAlive struct {
	ch chan struct{}
	// deadline is the time the keep alive channels close if no response
	deadline time.Time
	// nextKeepAlive is when to send the next keep alive message
	nextKeepAlive time.Time
}

type lessor struct {
	mu           sync.Mutex
	stopC        chan struct{}
	keepAliveMap map[LeaseID]*keepAlive
	store        store.Store
	wg           sync.WaitGroup
}

type Lease struct {
	LeaseID LeaseID
	Key     string
	Value   string
	TTL     int64
}

func NewLessor(store store.Store) (Lessor, error) {
	lessor := &lessor{
		mu:           sync.Mutex{},
		stopC:        make(chan struct{}),
		keepAliveMap: make(map[LeaseID]*keepAlive),
		store:        store,
	}

	lessor.wg.Add(2)
	go lessor.deadlineLoop()
	go lessor.keepAliveLoop()
	return lessor, nil
}

func (l *lessor) Grant(ctx context.Context, key, value string, ttl int64) (LeaseID, error) {
	if ttl == 0 {
		ttl = defaultTTL
	}

	id := NewID()
	lease := &Lease{
		LeaseID: id,
		Key:     key,
		Value:   value,
		TTL:     ttl,
	}

	inter, err := l.store.Add(ctx, string(lease.LeaseID), lease, store.WithResourceType(store.Lease))
	if err != nil {
		return NoLease, err
	}

	id = inter.(*Lease).LeaseID
	return id, nil
}

func (l *lessor) Revoke(ctx context.Context, id LeaseID) {
	_, _ = l.store.Delete(ctx, string(id), store.WithResourceType(store.Lease))
}

func (l *lessor) keepAlive(ctx context.Context, id LeaseID) *keepAlive {
	l.mu.Lock()
	defer l.mu.Unlock()

	ka, ok := l.keepAliveMap[id]
	if !ok {
		return nil
	}

	return ka
}

func (l *lessor) KeepAlive(ctx context.Context, id LeaseID) (chan struct{}, error) {
	ka := l.keepAlive(ctx, id)
	if ka != nil {
		return ka.ch, nil
	}

	inter, err := l.store.Get(ctx, string(id), store.WithResourceType(store.Lease))
	if err != nil {
		return nil, err
	}

	lease := inter.(*Lease)
	ka = &keepAlive{
		ch:            make(chan struct{}),
		deadline:      time.Now().Add(time.Duration(lease.TTL) * time.Second),
		nextKeepAlive: time.Now().Add(time.Duration(lease.TTL) * time.Second / 3.0),
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.keepAliveMap[id]; !ok {
		l.keepAliveMap[id] = ka
	}

	return ka.ch, nil
}

func (l *lessor) deadlineLoop() {
	defer l.wg.Done()

	for {
		select {
		case <-time.After(time.Second):
		case <-l.stopC:
			return
		}
		now := time.Now()
		l.mu.Lock()
		for id, ka := range l.keepAliveMap {
			if ka.deadline.Before(now) {
				// waited too long for response; lease may be expired
				ka.close()
				delete(l.keepAliveMap, id)
			}
		}
		l.mu.Unlock()
	}
}

func (l *lessor) keepAliveLoop() {
	defer l.wg.Done()

	for {
		var toKeep []LeaseID

		now := time.Now()
		l.mu.Lock()
		for id, ka := range l.keepAliveMap {
			if ka.nextKeepAlive.Before(now) {
				toKeep = append(toKeep, id)
			}
		}
		l.mu.Unlock()

		for _, id := range toKeep {
			lease := &Lease{
				LeaseID: id,
				Key:     "",
				Value:   "",
				TTL:     0,
			}

			inter, err := l.store.Update(context.TODO(), string(lease.LeaseID), lease, store.WithResourceType(store.Lease))
			if err == nil {
				lease = inter.(*Lease)
				l.mu.Lock()
				if ka, ok := l.keepAliveMap[id]; ok {
					ka.deadline = time.Now().Add(time.Duration(lease.TTL) * time.Second)
					ka.nextKeepAlive = time.Now().Add(time.Duration(lease.TTL) * time.Second / 3.0)
				}
				l.mu.Unlock()
			} else if dferrors.CheckError(err, dfcodes.ManagerStoreNotFound) {
				l.mu.Lock()
				if ka, ok := l.keepAliveMap[id]; ok {
					ka.close()
					delete(l.keepAliveMap, id)
				}
				l.mu.Unlock()
			}
		}

		select {
		case <-time.After(retryConnWait):
		case <-l.stopC:
			return
		}
	}
}

func (l *lessor) Close() error {
	close(l.stopC)
	l.wg.Wait()
	l.stopC = nil
	return nil
}

func (ka *keepAlive) close() {
	ka.ch <- struct{}{}
}
