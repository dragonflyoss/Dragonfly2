#!/bin/bash

components="cdn scheduler manager"
components="cdn"

set -x

for c in ${components}; do
  file=build/images/"${c}"/Dockerfile
  sed -i '1i# syntax=docker/dockerfile:1.3' "${file}"
  sed -i "s#RUN make build-$c && make install-$c#RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/root/go/ export GOPATH=/root/go \&\& make build-$c \&\& make install-$c#" "${file}"
done