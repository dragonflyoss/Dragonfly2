/*
 *     Copyright 2020 The Dragonfly Authors
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

//go:generate mockgen -destination mocks/reachable_mock.go -source reachable.go -package mocks

package reachable

import (
	"fmt"
	"net"
	"strings"
	"time"
)

const (
	// DefaultPort is the default tcp port.
	DefaultPort = "80"
	// DefaultNetwork is the default network type.
	DefaultNetwork = "tcp"
	// DefaultTimeout is the default dial timeout.
	DefaultTimeout = 1 * time.Second
)

type Reachable interface {
	// Check that the address can be accessed.
	Check() error
}

type reachable struct {
	address string
	network string
	timeout time.Duration
}

type Config struct {
	Address string
	Network string
	Timeout time.Duration
}

// New returns a new ReachableInterface interface.
func New(r *Config) Reachable {
	network := DefaultNetwork
	if r.Network != "" {
		network = r.Network
	}

	timeout := DefaultTimeout
	if r.Timeout != 0 {
		timeout = r.Timeout
	}

	return &reachable{
		address: r.Address,
		network: network,
		timeout: timeout,
	}
}

// Check that the address can be accessed.
func (r *reachable) Check() error {
	if !strings.Contains(r.address, ":") {
		r.address = fmt.Sprintf("%s:%s", r.address, DefaultPort)
	}

	conn, err := net.DialTimeout(r.network, r.address, r.timeout)
	if err != nil {
		return err
	}
	conn.Close()

	return nil
}
