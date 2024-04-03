#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

D7Y_VERSION=${D7Y_VERSION:-"latest"}
D7Y_REGISTRY=${D7Y_REGISTRY:-dragonflyoss}
curDir=$(cd "$(dirname "$0")" && pwd)
cd "${curDir}/../" || return

docker-push() {
    docker push "${D7Y_REGISTRY}"/"${1}":"${D7Y_VERSION}"
}

main() {
    case "${1-}" in
    dfdaemon)
        docker-push dfdaemon
        ;;
    scheduler)
        docker-push scheduler
        ;;
    manager)
        docker-push manager
        ;;
    trainer)
        docker-push trainer
    esac
}

main "$@"
