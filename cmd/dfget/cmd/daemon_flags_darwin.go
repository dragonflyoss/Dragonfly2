/*
 * Copyright The Dragonfly Authors.
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

// +build darwin

package cmd

import (
	"net"
	"time"

	"github.com/dragonflyoss/Dragonfly2/client/config"
	"github.com/dragonflyoss/Dragonfly2/client/daemon/storage"
)

var flagDaemonOpt = daemonOption{
	dataDir:  "",
	workHome: "",

	schedulers:      nil,
	pidFile:         "/tmp/dfdaemon.pid",
	lockFile:        "/tmp/dfdaemon.lock",
	advertiseIP:     net.IPv4zero,
	listenIP:        net.IPv4zero,
	downloadSocket:  "/tmp/dfdamon.sock",
	peerPort:        65000,
	uploadPort:      65002,
	proxyPort:       65001,
	downloadRate:    "100Mi",
	uploadRate:      "100Mi",
	storageDriver:   string(storage.SimpleLocalTaskStoreDriver),
	dataExpireTime:  config.DataExpireTime,
	daemonAliveTime: config.DaemonAliveTime,
	gcInterval:      time.Minute,
	verbose:         false,
}
