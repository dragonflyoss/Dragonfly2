package util

import (
	"sync"
)

var defaultLocker = NewLockerPool()

// GetLock locks key with defaultLocker.
func GetLock(key string, ro bool) {
	defaultLocker.GetLock(key, ro)
}

// ReleaseLock unlocks key with defaultLocker.
func ReleaseLock(key string, ro bool) {
	defaultLocker.ReleaseLock(key, ro)
}

// LockerPool is a set of reader/writer mutual exclusion locks.
type LockerPool struct {
	// use syncPool to cache allocated but unused *countRWMutex items for later reuse
	syncPool *sync.Pool

	lockerMap map[string]*countRWMutex
	sync.Mutex
}

// NewLockerPool returns a *LockerPool with self-defined prefix.
func NewLockerPool() *LockerPool {
	return &LockerPool{
		syncPool: &sync.Pool{
			New: func() interface{} {
				return newCountRWMutex()
			},
		},
		lockerMap: make(map[string]*countRWMutex),
	}
}

// GetLock locks key.
// If ro(readonly) is true, then it locks key for reading.
// Otherwise, locks key for writing.
func (l *LockerPool) GetLock(key string, ro bool) {
	l.Lock()

	locker, ok := l.lockerMap[key]
	if !ok {
		locker = l.syncPool.Get().(*countRWMutex)
		l.lockerMap[key] = locker
	}

	locker.increaseCount()
	l.Unlock()

	locker.lock(ro)
}

// ReleaseLock unlocks key.
// If ro(readonly) is true, then it unlocks key for reading.
// Otherwise, unlocks key for writing.
func (l *LockerPool) ReleaseLock(key string, ro bool) {
	l.Lock()
	defer l.Unlock()

	locker, ok := l.lockerMap[key]
	if !ok {
		return
	}

	locker.unlock(ro)
	if locker.decreaseCount() < 1 {
		locker.reset()
		l.syncPool.Put(locker)
		delete(l.lockerMap, key)
	}
}
