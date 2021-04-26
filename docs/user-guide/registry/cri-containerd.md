# Use Dfdaemon as Registry mirror for cri-containerd

## Quick Start

### Step 1: Configure Dfdaemon

To use dfdaemon as Registry Mirror, first you need to ensure configuration in `$HOME/.dragonfly/dfget-daemon.yaml`:

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

## Step 2: Configure Containerd

Then, enable mirrors in containerd registries configuration in
`/etc/containers/registries.conf`:

```toml
[plugins.cri.registry.mirrors."docker.io"]
  endpoint = ["http://127.0.0.1:65001"]
```

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
grep 'register peer task result' ~/logs/dragonfly/*.log
```

If the output of command above has content like

```
{"level":"info","ts":"2021-02-23 20:03:20.306","caller":"client/client.go:83","msg":"register peer task result:true[200] for taskId:adf62a86f001e17037eedeaaba3393f3519b80ce,peerIp:10.15.233.91,securityDomain:,idc:,scheduler:127.0.0.1:8002","peerId":"10.15.233.91-65000-43096-1614081800301788000","errMsg":null}
```
