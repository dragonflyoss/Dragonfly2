package e2eutil

import (
	"encoding/json"
	"fmt"
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

func (p *PodExec) CurlCommand(method string, header map[string]string, data map[string]interface{}, target string) *exec.Cmd {
	extArgs := []string{"/usr/bin/curl", "-s"}
	if method != "" {
		extArgs = append(extArgs, "-X", method)
	}
	if header != nil {
		for k, v := range header {
			extArgs = append(extArgs, "-H", fmt.Sprintf("%s:%s", k, v))
		}
	}
	if data != nil {
		jsonData, _ := json.Marshal(data)
		extArgs = append(extArgs, "-d", string(jsonData))
	}
	return p.Command(append(extArgs, target)...)
}

func KubeCtlCopyCommand(ns, pod, source, target string) *exec.Cmd {
	return KubeCtlCommand("-n", ns, "cp", pod+":"+source, target)
}
