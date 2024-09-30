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

package e2e

const (
	proxy                 = "localhost:65001"
	hostnameFilePath      = "/etc/hostname"
	dragonflyNamespace    = "dragonfly-system"
	dragonflyE2ENamespace = "dragonfly-e2e"
)

const (
	dfdaemonCompatibilityTestMode = "dfdaemon"
)

const (
	managerServerName   = "manager"
	schedulerServerName = "scheduler"
	seedPeerServerName  = "seed-peer"
	dfdaemonServerName  = "dfdaemon"
	proxyServerName     = "proxy"
)

type server struct {
	name       string
	namespace  string
	logDirName string
	replicas   int
	pprofPort  int
}

var servers = map[string]server{
	managerServerName: {
		name:       managerServerName,
		namespace:  dragonflyNamespace,
		logDirName: managerServerName,
		replicas:   1,
	},
	schedulerServerName: {
		name:       schedulerServerName,
		namespace:  dragonflyNamespace,
		logDirName: schedulerServerName,
		replicas:   3,
	},
	seedPeerServerName: {
		name:       seedPeerServerName,
		namespace:  dragonflyNamespace,
		logDirName: "daemon",
		replicas:   3,
	},
	dfdaemonServerName: {
		name:       dfdaemonServerName,
		namespace:  dragonflyNamespace,
		logDirName: "daemon",
		replicas:   1,
		pprofPort:  9999,
	},
	proxyServerName: {
		name:       proxyServerName,
		namespace:  dragonflyE2ENamespace,
		logDirName: "daemon",
		replicas:   3,
		pprofPort:  9999,
	},
}
