#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

DFGET_BINARY_NAME=dfget
DFCACHE_BINARY_NAME=dfcache
DFSTORE_BINARY_NAME=dfstore
SCHEDULER_BINARY_NAME=scheduler
MANAGER_BINARY_NAME=manager
TRAINER_BINARY_NAME=trainer

PKG=d7y.io/dragonfly/v2
BUILD_IMAGE=golang:1.21.1-alpine3.17

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

CUR_DIR=$(cd "$(dirname "$0")" && pwd)
cd "${CUR_DIR}" || return
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

build-dfstore-local() {
    build-local ${DFSTORE_BINARY_NAME} dfstore
}

build-scheduler-local() {
    build-local ${SCHEDULER_BINARY_NAME} scheduler
}

build-manager-local() {
    build-local ${MANAGER_BINARY_NAME} manager
}

build-trainer-local() {
    build-local ${TRAINER_BINARY_NAME} trainer
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

build-dfstore-docker() {
    build-docker ${DFSTORE_BINARY_NAME} dfstore
}

build-scheduler-docker() {
    build-docker ${SCHEDULER_BINARY_NAME} scheduler
}

build-manager-docker() {
    build-docker ${MANAGER_BINARY_NAME} manager
}

build-manager-console() {
    set -x
    CONSOLE_DIR=$(echo $CUR_DIR | sed 's#hack#manager/console#')
    MANAGER_DIR=$(echo $CUR_DIR | sed 's#hack#manager#')
    CONSOLE_ASSETS=$CONSOLE_DIR/dist/*
    MANAGER_ASSETS_DIR=$MANAGER_DIR/dist
    docker run --workdir=/build \
        --rm -v ${CONSOLE_DIR}:/build node:18-alpine \
        sh -c "yarn install --network-timeout 1000000 && yarn build"
    cp -r $CONSOLE_ASSETS $MANAGER_ASSETS_DIR
}

build-trainer-docker() {
    build-docker ${TRAINER_BINARY_NAME} trainer
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
        dfstore)
            build-dfstore-docker
            ;;
        scheduler)
            build-scheduler-docker
            ;;
        trainer)
            build-trainer-docker
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
            build-dfstore-docker
            build-scheduler-docker
            build-manager-docker
            build-trainer-docker
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
        dfstore)
            build-dfstore-local
            ;;
        scheduler)
            build-scheduler-local
            ;;
        trainer)
            build-trainer-local
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
            build-dfstore-local
            build-scheduler-local
            build-manager-local
            build-trainer-local
            ;;
        esac
    fi
}

main "$@"
