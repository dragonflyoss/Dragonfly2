package util

import "os/exec"

func DockerCommand(arg ...string) *exec.Cmd {
	cmd := []string{"exec", "-i", "kind-control-plane"}
	cmd = append(cmd, arg...)
	return exec.Command("docker", cmd...)
}

func CrictlCommand(arg ...string) *exec.Cmd {
	cmd := []string{"/usr/local/bin/crictl"}
	cmd = append(cmd, arg...)
	return DockerCommand(cmd...)
}
