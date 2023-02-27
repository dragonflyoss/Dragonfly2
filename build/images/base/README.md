# Base image

## Base Image for Debugging

### Build Base Image

```shell
docker build -t dragonflyoss/base:bpftrace-v0.13.0-go-v1.20.1 -f Dockerfile .
```

### Run Debug Container

```shell
BASE_IMAGE=dragonflyoss/base:bpftrace-v0.13.0-go-v1.20.1

# run debug container
docker run -ti -v /usr/src:/usr/src:ro \
    -v /lib/modules/:/lib/modules:ro \
    -v /sys/kernel/debug/:/sys/kernel/debug:rw \
    --net=host --pid=host --privileged \
    ${BASE_IMAGE}
```

### Build Dragonfly Images

```shell
# tags for condition compiling
export GOTAGS="debug"
# gcflags for dlv
export GOGCFLAGS="all=-N -l"
# base image
export BASE_IMAGE=dragonflyoss/base:bpftrace-v0.13.0-go-v1.20.1

make docker-build
```

## Debug With Delve

### Prepare Code for Debug

```shell
COMMIT_ID=c1c3d652
mkdir -p /go/src/d7y.io/dragonfly
git clone https://github.com/dragonflyoss/Dragonfly2.git /go/src/d7y.io/dragonfly/v2
git reset --hard ${COMMIT_ID}
```

### Debug Operations

1. Attach Process

   ```shell
   pid=$(pidof scheduler) # or dfget, manager
   dlv attach $pid
   ```

2. Debug

   Follow <https://github.com/go-delve/delve/tree/v1.7.0/Documentation/cli>
