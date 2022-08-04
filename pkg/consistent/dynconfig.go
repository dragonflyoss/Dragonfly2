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

package consistent

// DynConfig is used by resolver and balancer.
// Resolver register an observer to get configs updated.
// Balancer trigger Reload of DynConfig, then resolver resolve again
type DynConfig[T any] interface {
	// Get return dynConfig data, maybe from cache
	Get() (T, error)
	// Reload return dynConfig data ignoring cache and notify all observer. It is by balancer when balancer find no good node in ClientConn
	Reload() error
	// Convert dynConfig data to target addresses, host is one of (scheduler/manager/seedPeer)
	Convert(host string, data T) []string
	// Register is used by resolver, to register an observer to dynamically change target address
	Register(Observer[T])
	// Deregister is used by resolver, to register an observer to dynamically change target address
	Deregister(Observer[T])
}

type Observer[T any] interface {
	OnNotify(T)
}

type Reloader interface {
	Reload() error
}
