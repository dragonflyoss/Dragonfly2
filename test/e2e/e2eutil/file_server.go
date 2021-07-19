package e2eutil

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
