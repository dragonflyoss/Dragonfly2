#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

D7Y_VERSION=${D7Y_VERSION:-"latest"}
D7Y_REGISTRY=d7yio
curDir=$(cd "$(dirname "$0")" && pwd)
cd "${curDir}/../" || return

docker-build::build-cdn() {
    docker build --build-arg GOPROXY="${GOPROXY}" -t ${D7Y_REGISTRY}/cdn:"${D7Y_VERSION}" -f Dockerfile.cdn .
}

docker-build::build-dfdaemon() {
    docker build --build-arg GOPROXY="${GOPROXY}" -t ${D7Y_REGISTRY}/dfdaemon:"${D7Y_VERSION}" -f Dockerfile.dfdaemon .
}

main() {
    case "${1-}" in
    cdn)
        docker-build::build-cdn
        ;;
    dfdaemon)
        docker-build::build-dfdaemon
        ;;
    esac
}

main "$@"
