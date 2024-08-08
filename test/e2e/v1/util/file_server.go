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

package util

import "fmt"

func GetFileList() []string {
	return []string{
		"/etc/containerd/config.toml",
		"/etc/fstab",
		"/etc/hostname",
		"/usr/bin/kubectl",
		"/usr/bin/systemctl",
		"/usr/local/bin/containerd-shim",
		"/usr/local/bin/clean-install",
		"/usr/local/bin/entrypoint",
		"/usr/local/bin/containerd-shim-runc-v2",
		"/usr/local/bin/ctr",
		"/usr/local/bin/containerd",
		"/usr/local/bin/create-kubelet-cgroup-v2",
		"/usr/local/bin/crictl",
	}
}

func GetFileURL(filePath string) string {
	baseURL := "http://file-server.dragonfly-e2e.svc/kind"
	return fmt.Sprintf("%s%s", baseURL, filePath)
}

func GetNoContentLengthFileURL(filePath string) string {
	baseURL := "http://file-server-no-content-length.dragonfly-e2e.svc/kind"
	return fmt.Sprintf("%s%s", baseURL, filePath)
}
