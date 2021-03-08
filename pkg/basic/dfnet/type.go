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

package dfnet

type NetworkType string

const (
	TCP  NetworkType = "tcp"
	UNIX NetworkType = "unix"
)

type NetAddr struct {
	Type NetworkType `json:"type" yaml:"type"`
	// see https://github.com/grpc/grpc/blob/master/doc/naming.md
	Addr string `json:"addr" yaml:"addr"`
}

func (n NetAddr) GetEndpoint() string {
	switch n.Type {
	case UNIX:
		return "unix://" + n.Addr
	default:
		return "dns:///" + n.Addr
	}
}
