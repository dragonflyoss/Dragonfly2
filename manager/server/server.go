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

package server

import _ "d7y.io/dragonfly/v2/pkg/rpc/manager/server"

import (
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"github.com/pkg/errors"
)

type Server struct {
	cfg *config.Config
	ms  *ManagerServer
}

func NewServer(cfg *config.Config) (*Server, error) {
	if ms := NewManagerServer(cfg); ms != nil {
		return &Server{
			cfg: cfg,
			ms:  ms,
		}, nil
	} else {
		return nil, errors.New("failed to create manager server")
	}
}

func (s *Server) Start() (err error) {
	port := s.cfg.Server.Port
	if err = rpc.StartTcpServer(port, port, s.ms); err != nil {
		return errors.Wrap(err, "failed to start manager tcp server")
	} else {
		return nil
	}
}
