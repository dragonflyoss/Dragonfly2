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

func GitCommand(arg ...string) *exec.Cmd {
	return exec.Command("git", arg...)
}

type PodExec struct {
	namespace string
	name      string
	container string
}

func NewPodExec(namespace string, name string, container string) *PodExec {
	return &PodExec{
		namespace: namespace,
		name:      name,
		container: container,
	}
}

func (p *PodExec) Command(arg ...string) *exec.Cmd {
	extArgs := []string{"-n", p.namespace, "exec", p.name, "--"}
	if p.container != "" {
		extArgs = []string{"-n", p.namespace, "exec", "-c", p.container, p.name, "--"}
	}
	extArgs = append(extArgs, arg...)
	return KubeCtlCommand(extArgs...)
}

func (p *PodExec) CurlCommand(arg ...string) *exec.Cmd {
	return p.Command(append([]string{"/usr/bin/curl"}, arg...)...)
}

func KubeCtlCopyCommand(ns, pod, source, target string) *exec.Cmd {
	return KubeCtlCommand("-n", ns, "cp", pod+":"+source, target)
}
