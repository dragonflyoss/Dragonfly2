#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

D7Y_VERSION=${D7Y_VERSION:-"latest"}
D7Y_REGISTRY=${D7Y_REGISTRY:-d7yio}
curDir=$(cd "$(dirname "$0")" && pwd)
IMAGES_DIR="build/images"
cd "${curDir}/../" || return

docker-build::build-cdn() {
    docker build --build-arg GOPROXY="${GOPROXY}" -t "${D7Y_REGISTRY}"/cdn:"${D7Y_VERSION}" -f "${IMAGES_DIR}/cdn/Dockerfile" .
}

docker-build::build-dfdaemon() {
    docker build --build-arg GOPROXY="${GOPROXY}" -t "${D7Y_REGISTRY}"/dfdaemon:"${D7Y_VERSION}" -f "${IMAGES_DIR}/dfdaemon/Dockerfile" .
}

docker-build::build-scheduler() {
    docker build --build-arg GOPROXY="${GOPROXY}" -t "${D7Y_REGISTRY}"/scheduler:"${D7Y_VERSION}" -f "${IMAGES_DIR}/scheduler/Dockerfile" .
}

docker-build::build-manager() {
    docker build --build-arg GOPROXY="${GOPROXY}" -t "${D7Y_REGISTRY}"/manager:"${D7Y_VERSION}" -f "${IMAGES_DIR}/manager/Dockerfile" .
}

main() {
    case "${1-}" in
    cdn)
        docker-build::build-cdn
        ;;
    dfdaemon)
        docker-build::build-dfdaemon
        ;;
    scheduler)
        docker-build::build-scheduler
        ;;
    manager)
        docker-build::build-manager
    esac
}

main "$@"
