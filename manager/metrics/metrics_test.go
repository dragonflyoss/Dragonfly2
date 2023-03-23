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

package metrics

import (
	"net/http"
	"testing"

	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/manager/config"
)

func TestNew(t *testing.T) {
	cfg := &config.MetricsConfig{
		Addr: "localhost:8080",
	}
	svr := grpc.NewServer()
	server := New(cfg, svr)

	if server.Addr != cfg.Addr {
		t.Errorf("expected server.Addr to be %s, but got %s", cfg.Addr, server.Addr)
	}

	if _, ok := server.Handler.(*http.ServeMux); !ok {
		t.Errorf("expected server.Handler to be a *http.ServeMux, but got %T", server.Handler)
	}
}
