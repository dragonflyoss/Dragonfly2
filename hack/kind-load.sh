#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

D7Y_VERSION=${D7Y_VERSION:-"latest"}
D7Y_REGISTRY=${D7Y_REGISTRY:-dragonflyoss}

kind-load() {
    kind load docker-image "${D7Y_REGISTRY}"/"${1}":"${D7Y_VERSION}" || { echo >&2 "load docker image error"; exit 1; }
}

main() {
    case "${1-}" in
    dfdaemon)
        kind-load dfdaemon
        ;;
    scheduler)
        kind-load scheduler
        ;;
    manager)
        kind-load manager
        ;;
    trainer)
        kind-load trainer
        ;;
    no-content-length)
        kind-load no-content-length
    esac
}

main "$@"
