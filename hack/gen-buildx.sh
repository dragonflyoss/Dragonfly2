#!/bin/bash

components="dfdaemon scheduler manager trainer"

set -x

for c in ${components}; do
  file=build/images/"${c}"/Dockerfile
  sed -i '1i# syntax=docker/dockerfile:1.3' "${file}"
  sed -i "s#RUN make build#RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/root/go/ export GOPATH=/root/go \&\& make build#" "${file}"
done

# buildx need "--load" to export images to docker
sed -i 's/docker build/docker build --load/' hack/docker-build.sh
sed -i 's/docker build/docker build --load/' test/tools/no-content-length/build.sh

# TODO build console in https://github.com/dragonflyoss/console, and build image with github action cache
# remove npm build, use "make build-manager-console" separated
# sed -i '12,18d' build/images/manager/Dockerfile
# sed -i 's#COPY --from=console-builder /build/dist /opt/dragonfly/manager/console/dist#COPY ./manager/console/dist /opt/dragonfly/manager/console/dist#' build/images/manager/Dockerfile
