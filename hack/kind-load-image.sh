#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

D7Y_VERSION=${D7Y_VERSION:-"latest"}
D7Y_REGISTRY=${D7Y_REGISTRY:-d7yio}

kind-load-image() {
    kind load docker-image "${D7Y_REGISTRY}"/"${1}":"${D7Y_VERSION}" || { echo >&2 "load docker image error"; exit 1; }
}

main() {
    case "${1-}" in
    cdn)
        kind-load-image cdn
        ;;
    dfdaemon)
        kind-load-image dfdaemon
        ;;
    scheduler)
        kind-load-image scheduler
        ;;
    manager)
        kind-load-image manager
    esac
}

main "$@"
