#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

curDir=$(cd "$(dirname "$0")" && pwd)
cd "${curDir}/../" || return

D7Y_VERSION=${D7Y_VERSION:-"latest"}
D7Y_REGISTRY=${D7Y_REGISTRY:-dragonflyoss}
IMAGES_DIR="build/images"
BASE_IMAGE=${BASE_IMAGE:-alpine:3.17}

CGO_ENABLED=${CGO_ENABLED:-0}
GOPROXY=${GOPROXY:-`go env GOPROXY`}
GOTAGS=${GOTAGS:-}
GOGCFLAGS=${GOGCFLAGS:-}

GOOS=${GOOS:-linux}
GOARCH=${GOARCH:-amd64}

# enable bash debug output
DEBUG=${DEBUG:-}

if [[ -n "$DEBUG" ]]; then
    set -x
fi

docker-build() {
    name=$1
    docker build \
      --platform ${GOOS}/${GOARCH} \
      --build-arg CGO_ENABLED="${CGO_ENABLED}" \
      --build-arg GOPROXY="${GOPROXY}" \
      --build-arg GOTAGS="${GOTAGS}" \
      --build-arg GOGCFLAGS="${GOGCFLAGS}" \
      --build-arg BASE_IMAGE="${BASE_IMAGE}" \
      -t "${D7Y_REGISTRY}/${name}:${D7Y_VERSION}" \
      -f "${IMAGES_DIR}/${name}/Dockerfile" .
}

git-submodule() {
  git submodule update --init --recursive
}

main() {
    case "${1-}" in
    dfdaemon)
        docker-build dfdaemon
        ;;
    scheduler)
        docker-build scheduler
        ;;
    manager)
        git-submodule
        docker-build manager
        ;;
    trainer)
        docker-build trainer
    esac
}

main "$@"
