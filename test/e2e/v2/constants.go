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
	hostnameFilePath      = "/etc/hostname"
	dragonflyNamespace    = "dragonfly-system"
	dragonflyE2ENamespace = "dragonfly-e2e"
)

const (
	dfdaemonCompatibilityTestMode  = "dfdaemon"
	schedulerCompatibilityTestMode = "scheduler"
)

const (
	managerServerName    = "manager"
	schedulerServerName  = "scheduler"
	seedClientServerName = "seed-client"
	clientServerName     = "client"
)

type server struct {
	name       string
	namespace  string
	logDirName string
	replicas   int
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
	seedClientServerName: {
		name:       seedClientServerName,
		namespace:  dragonflyNamespace,
		logDirName: "dfdaemon",
		replicas:   3,
	},
	clientServerName: {
		name:       clientServerName,
		namespace:  dragonflyNamespace,
		logDirName: "dfdaemon",
		replicas:   1,
	},
}
