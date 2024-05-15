/*
 *     Copyright 2023 The Dragonfly Authors
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

package ping

import (
	"errors"
	"time"

	"github.com/gaius-qi/ping"
)

const (
	// defaultPingCount calls pinger to stop after sending (and receiving)
	// default count echo packets.
	defaultPingCount = 1

	// defaultPingTimeout specifies a default timeout before ping exits,
	// regardless of how many packets have been received.
	defaultPingTimeout = 1 * time.Second
)

// Ping returns the ping metrics with the icmp protocol.
func Ping(addr string) (*ping.Statistics, error) {
	pinger, err := ping.NewPinger(addr)
	if err != nil {
		return nil, err
	}

	pinger.Count = defaultPingCount
	pinger.Timeout = defaultPingTimeout

	// SetPrivileged sets the type of ping pinger will send.
	// true means pinger will send a "privileged" raw ICMP ping.
	pinger.SetPrivileged(true)
	if err := pinger.Run(); err != nil {
		return nil, err
	}

	stats := pinger.Statistics()
	if stats.PacketsRecv <= 0 {
		return nil, errors.New("receive packet failed")
	}

	return stats, nil
}
