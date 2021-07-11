package util

import "os/exec"

func DockerCommand(arg ...string) *exec.Cmd {
	extArgs := []string{"exec", "-i", "kind-control-plane"}
	extArgs = append(extArgs, arg...)
	return exec.Command("docker", extArgs...)
}

func CrictlCommand(arg ...string) *exec.Cmd {
	extArgs := []string{"/usr/local/bin/crictl"}
	extArgs = append(extArgs, arg...)
	return DockerCommand(extArgs...)
}
