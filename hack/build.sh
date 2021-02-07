#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

CDN_BINARY_NAME=cdn
DFGET_BINARY_NAME=dfget
SCHEDULER_BINARY_NAME=scheduler

PKG=d7y.io/dragonfly/v2
BUILD_IMAGE=golang:1.15.8
TAG=$(git rev-list --tags --max-count=1)
if [[ -z "$TAG" ]]; then
    VERSION=unknown
else
    VERSION=$(git describe --tags "$TAG")
fi

REVISION=$(git rev-parse --short HEAD)
DATE=$(date "+%Y%m%d-%H:%M:%S")
LDFLAGS="-X ${PKG}/version.version=${VERSION} -X ${PKG}/version.revision=${REVISION} -X ${PKG}/version.buildDate=${DATE}"

curDir=$(cd "$(dirname "$0")" && pwd)
cd "${curDir}" || return
BUILD_SOURCE_HOME=$(cd ".." && pwd)

. ./env.sh

BUILD_PATH=bin/${GOOS}_${GOARCH}
USE_DOCKER=${USE_DOCKER:-"0"}

create-dirs() {
    cd "${BUILD_SOURCE_HOME}" || return
    mkdir -p .go/src/${PKG} .go/bin .cache
    mkdir -p "${BUILD_PATH}"
}

build-local() {
    test -f "${BUILD_SOURCE_HOME}/${BUILD_PATH}/$1" && rm -f "${BUILD_SOURCE_HOME}/${BUILD_PATH}/$1"
    cd "${BUILD_SOURCE_HOME}/cmds/$2" || return
    go build -o "${BUILD_SOURCE_HOME}/${BUILD_PATH}/$1" -ldflags "${LDFLAGS}"
    chmod a+x "${BUILD_SOURCE_HOME}/${BUILD_PATH}/$1"
    echo "BUILD: $2 in ${BUILD_SOURCE_HOME}/${BUILD_PATH}/$1"
}

build-cdn-local() {
    build-local ${CDN_BINARY_NAME} cdnsystem
}

build-dfget-local() {
    build-local ${DFGET_BINARY_NAME} dfget
}

build-scheduler-local() {
    build-local ${SCHEDULER_BINARY_NAME} scheduler
}

build-docker() {
    cd "${BUILD_SOURCE_HOME}" || return
    docker run \
        --rm \
        -ti \
        -u "$(id -u)":"$(id -g)" \
        -v "$(pwd)"/.go:/go \
        -v "$(pwd)":/go/src/${PKG} \
        -v "$(pwd)"/"${BUILD_PATH}":/go/bin \
        -v "$(pwd)"/.cache:/.cache \
        -e GOOS="${GOOS}" \
        -e GOARCH="${GOARCH}" \
        -e CGO_ENABLED=0 \
        -e GO111MODULE=on \
        -e GOPROXY="${GOPROXY}" \
        -w /go/src/${PKG} \
        ${BUILD_IMAGE} \
        go build -o "/go/bin/$1" -ldflags "${LDFLAGS}" ./cmds/"$2"
    echo "BUILD: $1 in ${BUILD_SOURCE_HOME}/${BUILD_PATH}/$1"
}

build-cdn-docker() {
    build-docker ${DFDAEMON_BINARY_NAME} dfdaemon
}

build-dfget-docker() {
    build-docker ${DFGET_BINARY_NAME} dfget
}

build-scheduler-docker() {
    build-docker ${scheduler_BINARY_NAME} scheduler
}

main() {
    create-dirs
    if [[ "1" == "${USE_DOCKER}" ]]; then
        echo "Begin to build with docker."
        case "${1-}" in
        dfdaemon)
            build-cdn-docker
            ;;
        dfget)
            build-dfget-docker
            ;;
        scheduler)
            build-scheduler-docker
            ;;
        *)
            build-dfget-docker
            build-cdn-docker
            build-scheduler-docker
            ;;
        esac
    else
        echo "Begin to build in the local environment."
        case "${1-}" in
        cdn)
            build-cdn-local
            ;;
        dfget)
            build-dfget-local
            ;;
        scheduler)
            build-scheduler-local
            ;;
        *)
            build-dfget-local
            build-cdn-local
            build-scheduler-local
            ;;
        esac
    fi
}

main "$@"
