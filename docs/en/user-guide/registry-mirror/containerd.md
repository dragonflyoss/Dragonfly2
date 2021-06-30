# Use dfget daemon as registry mirror for Containerd with CRI support

From v1.1.0, Containerd supports registry mirrors, we can configure Containerd via this feature for HA.

## Quick Start

### Step 1: Configure dfget daemon

To use dfget daemon as registry mirror, first you need to ensure configuration in `/etc/dragonfly/dfget.yaml`:

```yaml
proxy:
  security:
    insecure: true
  tcp_listen:
    listen: 0.0.0.0
    port: 65001
  registry_mirror:
    url: https://index.docker.io
  proxies:
    - regx: blobs/sha256.*
```

Run dfget daemon

```shell
dfget daemon
```

## Step 2: Configure Containerd

Then, enable mirrors in Containerd registries configuration in
`/etc/containerd/config.toml`:

```toml
[plugins."io.containerd.grpc.v1.cri".registry.mirrors."docker.io"]
  endpoint = ["http://127.0.0.1:65001","https://registry-1.docker.io"]
```

In this config, there is two mirror endpoints for "docker.io", Containerd will pull images with `http://127.0.0.1:65001` first.
If `http://127.0.0.1:65001` is not available, the default `https://registry-1.docker.io` will be used for HA.

> More details about Containerd configuration: https://github.com/containerd/containerd/blob/v1.5.2/docs/cri/registry.md#configure-registry-endpoint

> Containerd has deprecated the above config from v1.4.0, new format for reference: https://github.com/containerd/containerd/blob/v1.5.2/docs/cri/config.md#registry-configuration

## Step 3: Restart Containerd Daemon

```
systemctl restart containerd
```

## Step 4: Pull Image

You can pull image like this:

```
crictl pull docker.io/library/busybox
```

## Step 5: Validate Dragonfly

You can execute the following command to check if the busybox image is distributed via Dragonfly.

```bash
grep 'register peer task result' /var/log/dragonfly/daemon/*.log
```

If the output of command above has content like

```
{"level":"info","ts":"2021-02-23 20:03:20.306","caller":"client/client.go:83","msg":"register peer task result:true[200] for taskId:adf62a86f001e17037eedeaaba3393f3519b80ce,peerIp:10.15.233.91,securityDomain:,idc:,scheduler:127.0.0.1:8002","peerId":"10.15.233.91-65000-43096-1614081800301788000","errMsg":null}
```
