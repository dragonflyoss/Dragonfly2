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

package nginx

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"text/template"

	"d7y.io/dragonfly/v2/cdn/nginx/conf"
	logger "d7y.io/dragonfly/v2/internal/dflog"
)

type Server struct {
	uploadRepo string
	serverPort int
	config     Config
}

func New(config Config, uploadRepo string, serverPort int) (*Server, error) {
	return &Server{
		config:     config,
		serverPort: serverPort,
		uploadRepo: uploadRepo,
	}, nil
}

func (s *Server) Serve() error {
	params := map[string]interface{}{
		"port": s.serverPort,
		"repo": s.uploadRepo,
	}

	if !s.config.TLS.Enabled {
		logger.Info("Server TLS is disabled")
	}

	src, err := s.generateConfig(s.config.populateParams(params))
	if err != nil {
		return fmt.Errorf("build nginx config: %s", err)
	}

	// write nginx config
	if err := os.MkdirAll(filepath.Dir(s.config.TargetConfPath), 0775); err != nil {
		return err
	}
	if err := os.WriteFile(s.config.TargetConfPath, src, 0644); err != nil {
		return fmt.Errorf("write src: %s", err)
	}

	if err := os.MkdirAll(s.config.LogDir, 0775); err != nil {
		return err
	}
	stdout, err := os.OpenFile(filepath.Join(s.config.LogDir, "nginx-error.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open stdout log: %s", err)
	}

	args := []string{s.config.CmdPath, "-g", "daemon off;", "-c", s.config.TargetConfPath}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdout = stdout
	cmd.Stderr = stdout
	return cmd.Run()
}

func (s *Server) Shutdown() error {
	defer logger.Infof("====stopped nginx server====")
	stdout, err := os.OpenFile(filepath.Join(s.config.LogDir, "nginx-error.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open stdout log: %s", err)
	}
	args := []string{s.config.CmdPath, "-s", "stop"}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdout = stdout
	cmd.Stderr = stdout
	return cmd.Run()
}

// generateConfig generate nginx config.
func (s *Server) generateConfig(params map[string]interface{}) ([]byte, error) {
	var tmpl = conf.Template
	if s.config.TemplatePath != "" {
		b, err := os.ReadFile(s.config.TemplatePath)
		if err != nil {
			return nil, fmt.Errorf("read template: %s", err)
		}
		tmpl = string(b)
	}
	src, err := populateTemplate(tmpl, params)
	if err != nil {
		return nil, fmt.Errorf("populate nginx: %s", err)
	}
	return src, nil
}

func populateTemplate(tmpl string, args map[string]interface{}) ([]byte, error) {
	t, err := template.New("nginx").Parse(tmpl)
	if err != nil {
		return nil, fmt.Errorf("parse: %s", err)
	}
	out := &bytes.Buffer{}
	if err := t.Execute(out, args); err != nil {
		return nil, fmt.Errorf("exec: %s", err)
	}
	return out.Bytes(), nil
}
