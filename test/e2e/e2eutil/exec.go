package e2eutil

import (
	"os/exec"
)

const (
	kindDockerContainer = "kind-control-plane"
)

func DockerCommand(arg ...string) *exec.Cmd {
	extArgs := []string{"exec", "-i", kindDockerContainer}
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

func ABCommand(arg ...string) *exec.Cmd {
	return exec.Command("ab", arg...)
}

type PodExec struct {
	namespace string
	name      string
}

func NewPodExec(namespace string, name string) *PodExec {
	return &PodExec{
		namespace: namespace,
		name:      name,
	}
}

func (p *PodExec) Command(arg ...string) *exec.Cmd {
	extArgs := []string{"-n", p.namespace, "exec", p.name, "--"}
	extArgs = append(extArgs, arg...)
	return KubeCtlCommand(extArgs...)
}
