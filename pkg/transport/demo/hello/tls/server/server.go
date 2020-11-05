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

package main

import (
	"flag"
	tls "github.com/dragonflyoss/Dragonfly2/pkg/transport/demo/hello/tls"
	"path/filepath"
)

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/transport"
	gxsync "github.com/dubbogo/gost/sync"
)

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/transport/demo/util"
)

var (
	taskPoolMode        = flag.Bool("taskPool", false, "task pool mode")
	taskPoolQueueLength = flag.Int("task_queue_length", 100, "task queue length")
	taskPoolQueueNumber = flag.Int("task_queue_number", 4, "task queue number")
	taskPoolSize        = flag.Int("task_pool_size", 2000, "task poll size")
	pprofPort           = flag.Int("pprof_port", 65432, "pprof http port")
	Sessions            []transport.Session
)

var (
	taskPool *gxsync.TaskPool
)

func main() {
	flag.Parse()

	util.SetLimit()

	util.Profiling(*pprofPort)
	serverPemPath, _ := filepath.Abs("./demo/hello/tls/certs/server0.pem")
	serverKeyPath, _ := filepath.Abs("./demo/hello/tls/certs/server0.key")
	caPemPath, _ := filepath.Abs("./demo/hello/tls/certs/ca.pem")

	c := &transport.ServerTlsConfigBuilder{
		ServerKeyCertChainPath:        serverPemPath,
		ServerPrivateKeyPath:          serverKeyPath,
		ServerTrustCertCollectionPath: caPemPath,
	}

	options := []transport.ServerOption{transport.WithLocalAddress(":8090"),
		transport.WithServerSslEnabled(true),
		transport.WithServerTlsConfigBuilder(c),
	}

	if *taskPoolMode {
		taskPool = gxsync.NewTaskPool(
			gxsync.WithTaskPoolTaskQueueLength(*taskPoolQueueLength),
			gxsync.WithTaskPoolTaskQueueNumber(*taskPoolQueueNumber),
			gxsync.WithTaskPoolTaskPoolSize(*taskPoolSize),
		)
	}

	server := transport.NewTCPServer(options...)

	go server.RunEventLoop(NewHelloServerSession)
	util.WaitCloseSignals(server)
}

func NewHelloServerSession(session transport.Session) (err error) {
	err = tls.InitialSession(session)
	Sessions = append(Sessions, session)
	if err != nil {
		return
	}
	session.SetTaskPool(taskPool)

	return
}
