package e2eutil

import "os/exec"

func DockerCommand(arg ...string) *exec.Cmd {
	extArgs := []string{"exec", "-i", "kind-control-plane"}
	extArgs = append(extArgs, arg...)
	return exec.Command("docker", extArgs...)
}

func CriCtlCommand(arg ...string) *exec.Cmd {
	extArgs := []string{"/usr/local/bin/crictl"}
	extArgs = append(extArgs, arg...)
	return DockerCommand(extArgs...)
}

func KubeCtlCommand(arg ...string) *exec.Cmd {
	return exec.Command("kubectl", arg...)
}
