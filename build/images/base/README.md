# Base Image for Debugging


## Build Base Image

```shell
BASE_IMAGE=dragonflyoss/base:bpftrace-v0.13.0-go-v1.16.6

docker run -ti -v /usr/src:/usr/src:ro \
    -v /lib/modules/:/lib/modules:ro \
    -v /sys/kernel/debug/:/sys/kernel/debug:rw \
    --net=host --pid=host --privileged \
    ${BASE_IMAGE}
```

## Build Dragonfly Images

```shell
# tags for condition compiling
export GOTAGS="debug"
# gcflags for dlv
export GOGCFLAGS="all=-N -l"
# base image
export BASE_IMAGE=dragonflyoss/base:bpftrace-v0.13.0-go-v1.16.6

make docker-build
```