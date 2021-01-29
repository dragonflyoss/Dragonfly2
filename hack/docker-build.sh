#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

DF_VERSION=${DF_VERSION:-"latest"}
curDir=$(cd "$(dirname "$0")" && pwd)
cd "${curDir}/../" || return

docker-build::build-cdn(){
    docker build --build-arg GOPROXY="${GOPROXY}" -t cdn:"${DF_VERSION}" -f Dockerfile.cdn .
}

main() {
    case "${1-}" in
        cdn)
            docker-build::build-cdn
        ;;
    esac
}

main "$@"
