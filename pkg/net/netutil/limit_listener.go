/*
 *     Copyright 2022 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package netutil

import (
	"net"
	"sync"

	"go.uber.org/atomic"
)

// NewLimitListener returns a Listener that accepts at most n simultaneous
// connections from the provided Listener.
func NewLimitListener(l net.Listener, n int) *LimitListener {
	return &LimitListener{
		Listener: l,
		sem:      make(chan struct{}, n),
		done:     make(chan struct{}),
		stats: ListenerStats{
			Sum:    atomic.NewInt64(0),
			Active: atomic.NewInt64(0),
		},
	}
}

type ListenerStats struct {
	Sum    *atomic.Int64
	Active *atomic.Int64
}

// LimitListener is similar to the limitListener in "golang.org/x/net/netutil", but have Stats
type LimitListener struct {
	net.Listener
	sem       chan struct{}
	closeOnce sync.Once     // ensures the done chan is only closed once
	done      chan struct{} // no values sent; closed when Close is called
	stats     ListenerStats
}

// acquire acquires the limiting semaphore. Returns true if successfully
// accquired, false if the listener is closed and the semaphore is not
// acquired.
func (l *LimitListener) acquire() bool {
	select {
	case <-l.done:
		return false
	case l.sem <- struct{}{}:
		return true
	}
}
func (l *LimitListener) release() {
	<-l.sem
	l.stats.Active.Sub(1)
}

func (l *LimitListener) Accept() (net.Conn, error) {
	if !l.acquire() {
		// If the semaphore isn't acquired because the listener was closed, expect
		// that this call to accept won't block, but immediately return an error.
		// If it instead returns a spurious connection (due to a bug in the
		// Listener, such as https://golang.org/issue/50216), we immediately close
		// it and try again. Some buggy Listener implementations (like the one in
		// the aforementioned issue) seem to assume that Accept will be called to
		// completion, and may otherwise fail to clean up the client end of pending
		// connections.
		for {
			c, err := l.Listener.Accept()
			if err != nil {
				return nil, err
			}
			c.Close()
		}
	}

	c, err := l.Listener.Accept()
	l.stats.Sum.Add(1)
	l.stats.Active.Add(1)
	if err != nil {
		l.release()
		return nil, err
	}
	return &limitListenerConn{Conn: c, release: l.release}, nil
}
func (l *LimitListener) Stats() ListenerStats {
	return l.stats
}

func (l *LimitListener) Close() error {
	err := l.Listener.Close()
	l.closeOnce.Do(func() { close(l.done) })
	return err
}

type limitListenerConn struct {
	net.Conn
	releaseOnce sync.Once
	release     func()
}

func (l *limitListenerConn) Close() error {
	err := l.Conn.Close()
	l.releaseOnce.Do(l.release)
	return err
}
