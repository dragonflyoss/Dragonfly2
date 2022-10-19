#!/bin/bash

set -e
set -x

export GOPATH=/go
export CGO_ENABLED="1"
export GO111MODULE="on"

cd /go/src/d7y.io/dragonfly/v2 || exit 1
BUILD_TIME=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
BUILD_COMMIT=$(git rev-parse --short HEAD)

PLUGIN_TYPE=${1}
PLUGIN_NAME=${2}
PLUGIN_PATH=${3}

build_plugin(){
  mkdir -p /artifacts/plugins/
  go build -buildmode=plugin \
    -ldflags="-X main.buildTime=${BUILD_TIME} -X main.buildCommit=${BUILD_COMMIT}" \
    -o="/artifacts/plugins/d7y-${PLUGIN_TYPE}-plugin-${PLUGIN_NAME}.so" \
    "${PLUGIN_PATH%/}"/*
}

build_binary(){
  target=${1}
  cd /go/src/d7y.io/dragonfly/v2 && make build-"$target"
  mkdir -p /artifacts/binaries/
  mv -f /go/src/d7y.io/dragonfly/v2/bin/linux_amd64/"$target" /artifacts/binaries/
}

print_verbose_info(){
  target=${1}
  /artifacts/binaries/"$target" version

  # validate plugin is ok
  mkdir -p /usr/local/dragonfly/plugins/
  cp -f "/artifacts/plugins/d7y-${PLUGIN_TYPE}-plugin-${PLUGIN_NAME}.so" /usr/local/dragonfly/plugins/
  /artifacts/binaries/"$target" plugin
}

build(){
  case "$PLUGIN_TYPE" in
    resource)
      build_plugin
      build_binary dfget
      print_verbose_info dfget
      ;;
    scheduler)
      build_plugin
      build_binary scheduler
      print_verbose_info scheduler
      ;;
    manager)
      build_plugin
      build_binary manager
      print_verbose_info manager
      ;;
    *)
      echo "invalid plugin type: $PLUGIN_TYPE, current support: resource, scheduler, searcher"
      ;;
  esac
}

build
