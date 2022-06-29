#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

DFGET_BINARY_NAME=dfget
DFCACHE_BINARY_NAME=dfcache
SCHEDULER_BINARY_NAME=scheduler
MANAGER_BINARY_NAME=manager

PKG=d7y.io/dragonfly/v2
BUILD_IMAGE=golang:1.18.3-alpine3.16

VERSION=$(git rev-parse --short HEAD)
BUILD_TIME=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

CGO_ENABLED=${CGO_ENABLED:-0}
GOPROXY=${GOPROXY:-}
GOTAGS=${GOTAGS:-}
GOGCFLAGS=${GOGCFLAGS:-}
GOLDFLAGS="-X d7y.io/dragonfly/v2/version.GitCommit=${VERSION}"
GOLDFLAGS="${GOLDFLAGS} -X d7y.io/dragonfly/v2/version.BuildTime=${BUILD_TIME}"
GOLDFLAGS="${GOLDFLAGS} -X \"d7y.io/dragonfly/v2/version.Gotags=${GOTAGS:-none}\""
GOLDFLAGS="${GOLDFLAGS} -X \"d7y.io/dragonfly/v2/version.GoVersion=$(go version | grep -o 'go[^ ].*')\""
GOLDFLAGS="${GOLDFLAGS} -X \"d7y.io/dragonfly/v2/version.Gogcflags=${GOGCFLAGS:-none}\""

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
    cd "${BUILD_SOURCE_HOME}/cmd/$2" || return
    go build -tags="${GOTAGS}" -ldflags="${GOLDFLAGS}" -gcflags="${GOGCFLAGS}" -o="${BUILD_SOURCE_HOME}/${BUILD_PATH}/$1"
    chmod a+x "${BUILD_SOURCE_HOME}/${BUILD_PATH}/$1"
    echo "BUILD: $2 in ${BUILD_SOURCE_HOME}/${BUILD_PATH}/$1"
}

build-dfget-local() {
    build-local ${DFGET_BINARY_NAME} dfget
}

build-dfcache-local() {
    build-local ${DFCACHE_BINARY_NAME} dfcache
}

build-scheduler-local() {
    build-local ${SCHEDULER_BINARY_NAME} scheduler
}

build-manager-local() {
    build-local ${MANAGER_BINARY_NAME} manager
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
        -e CGO_ENABLED="${CGO_ENABLED}" \
        -e GO111MODULE=on \
        -e GOPROXY="${GOPROXY}" \
        -e GOTAGS="${GOTAGS}" \
        -e GOGCFLAGS="${GOGCFLAGS}" \
        -w /go/src/${PKG} \
        ${BUILD_IMAGE} \
        go build -o "/go/bin/$1" ./cmd/"$2"
    echo "BUILD: $1 in ${BUILD_SOURCE_HOME}/${BUILD_PATH}/$1"
}

build-dfget-docker() {
    build-docker ${DFGET_BINARY_NAME} dfget
}

build-dfcache-docker() {
    build-docker ${DFCACHE_BINARY_NAME} dfcache
}

build-scheduler-docker() {
    build-docker ${SCHEDULER_BINARY_NAME} scheduler
}

build-manager-docker() {
    build-docker ${MANAGER_BINARY_NAME} manager
}

build-manager-console() {
    set -x
    consoleDir=$(echo $curDir | sed 's#hack#manager/console#')
    docker run --workdir=/build \
        --rm -v ${consoleDir}:/build node:12-alpine \
        sh -c "npm install --loglevel warn --progress false && npm run build"
}

main() {
    create-dirs
    if [[ "1" == "${USE_DOCKER}" ]]; then
        echo "Begin to build with docker."
        case "${1-}" in
        dfget)
            build-dfget-docker
            ;;
        dfcache)
            build-dfcache-docker
            ;;
        scheduler)
            build-scheduler-docker
            ;;
        manager)
            build-manager-docker
            ;;
        manager-console)
            build-manager-console
            ;;
        *)
            build-dfget-docker
            build-dfcache-docker
            build-scheduler-docker
            build-manager-docker
            ;;
        esac
    else
        echo "Begin to build in the local environment."
        case "${1-}" in
        dfget)
            build-dfget-local
            ;;
        dfcache)
            build-dfcache-local
            ;;
        scheduler)
            build-scheduler-local
            ;;
        manager)
            build-manager-local
            ;;
        manager-console)
            build-manager-console
            ;;
        *)
            build-dfget-local
            build-dfcache-local
            build-scheduler-local
            build-manager-local
            ;;
        esac
    fi
}

main "$@"
