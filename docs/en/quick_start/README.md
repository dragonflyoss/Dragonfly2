# Dragonfly Quick Start

Dragonfly Quick Start document aims to help you to quick start Dragonfly journey. This experiment is quite easy and simplified.

If you are using Dragonfly in your **production environment** to handle production image distribution, please refer to supernode and dfget's detailed production parameter configuration.

## Prerequisites

All steps in this document is doing on the same machine using the docker container, so make sure the docker container engine installed and started on your machine. You can also refer to the documentation: [multi-machine deployment](../user_guide/multi_machines_deployment.md) to experience Dragonfly.

## Step 1: Deploy Dragonfly Manager Server

```bash
docker run -d --name supernode \
    --restart=always \
    -p 8001:8001 \
    -p 8002:8002 \
    -v /home/admin/supernode:/home/admin/supernode \
    dragonflyoss/supernode:1.0.2
```

## Step 2: Deploy Dragonfly CDN Server

```bash
docker run -d --name supernode \
    --restart=always \
    -p 8001:8001 \
    -p 8002:8002 \
    -v /home/admin/supernode:/home/admin/supernode \
    dragonflyoss/supernode:1.0.2
```

## Step 3: Deploy Dragonfly Scheduler Server

```bash
docker run -d --name supernode \
    --restart=always \
    -p 8001:8001 \
    -p 8002:8002 \
    -v /home/admin/supernode:/home/admin/supernode \
    dragonflyoss/supernode:1.0.2
```

## Step 2: Deploy Dragonfly Client

```bash
SUPERNODE_IP=`docker inspect supernode -f '{{.NetworkSettings.Networks.bridge.IPAddress}}'`
docker run -d --name dfclient \
    --restart=always \
    -p 65001:65001 \
    -v $HOME/.small-dragonfly:/root/.small-dragonfly \
    dragonflyoss/dfclient:1.0.2 --registry https://index.docker.io --node $SUPERNODE_IP
```

**NOTE**:

- The `--registry` parameter specifies the mirrored image registry address, and `https://index.docker.io` is the address of official image registry, you can also set it to the other **non-https image registries**.
- The `--node` parameter specifies the supernode's address in the format of **HOST:IP**. And the default value `8002` will be used if the port is not specified. Here we use `docker inspect` to get the ip of supernode container as the host value. Since the supernode container exposes its ports, you can specify this parameter to node ip address as well.

## Step 3. Configure Docker Daemon

We need to modify the Docker Daemon configuration to use the Dragonfly as a pull through registry.

1. Add or update the configuration item `registry-mirrors` in the configuration file`/etc/docker/daemon.json`.

```json
{
  "registry-mirrors": ["http://127.0.0.1:65001"]
}
```

**Tip:** For more information on `/etc/docker/daemon.json`, see [Docker documentation](https://docs.docker.com/registry/recipes/mirror/#configure-the-cache).

2. Restart Docker Daemon.

```bash
systemctl restart docker
```

## Step 4: Pull images with Dragonfly

Through the above steps, we can start to validate if Dragonfly works as expected.

And you can pull the image as usual, for example:

```bash
docker pull nginx:latest
```

## Step 5: Validate Dragonfly

You can execute the following command to check if the nginx image is distributed via Dragonfly.

```bash
docker exec dfclient grep 'downloading piece' /root/.small-dragonfly/logs/dfclient.log
```

If the output of command above has content like

```
2019-03-29 15:49:53.913 INFO sign:96027-1553845785.119 : downloading piece:{"taskID":"00a0503ea12457638ebbef5d0bfae51f9e8e0a0a349312c211f26f53beb93cdc","superNode":"127.0.0.1","dstCid":"127.0.0.1-95953-1553845720.488","range":"67108864-71303167","result":503,"status":701,"pieceSize":4194304,"pieceNum":16}
```

then Dragonfly works successfully.

## SEE ALSO

- [multi machines deployment](../user-guide/multi_machines_deployment.md) - experience Dragonfly on multiple machines
- [install manager](../user-guide/install_manager.md) - how to install the Dragonfly manager
- [install cdn](../user-guide/install_cdn.md) - how to install the Dragonfly cdn
- [install scheduler](../user-guide/install_scheduler.md) - how to install the Dragonfly scheduler
- [install client](../user-guide/install_client.md) - how to install the Dragonfly client
- [proxy](../user_guide/proxy/docker.md) - make Dragonfly as an HTTP proxy for docker daemon
- [download files](../user-guide/download_files.md) - download files with Dragonfly
- Container Runtimes
    - [cri-o mirror](../user_guide/registry/cri-o.md) - make Dragonfly as Registry Mirror for CRIO daemon
    - [cri-containerd_mirror](../user_guide/registry/cri-containerd.md) - make Dragonfly as Registry Mirror for containerd daemon
