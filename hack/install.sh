#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

BIN_DIR="../bin"
CDN_BINARY_NAME=cdn
DFGET_BINARY_NAME=dfget
SCHEDULER_BINARY_NAME=scheduler

curDir=$(cd "$(dirname "$0")" && pwd)
cd "${curDir}" || return

. ./env.sh

install() {
    case "${1-}" in
    cdn)
        install-cdn
        ;;
    dfget)
        install-dfget
        ;;
    scheduler)
        install-scheduler
        ;;
    esac
}

install-cdn() {
    local installCdnDir="${INSTALL_HOME}/${INSTALL_CDN_PATH}"
    echo "install: ${installCdnDir}"
    createDir "${installCdnDir}"

    cp "${BIN_DIR}/${GOOS}_${GOARCH}/${CDN_BINARY_NAME}" "${installCdnDir}"

    createLink "${installCdnDir}/${CDN_BINARY_NAME}" /usr/local/bin/cdn
}

uninstall-cdn() {
    echo "unlink /usr/local/bin/cdn"
    test -e /usr/local/bin/cdn && unlink /usr/local/bin/cdn
}

install-dfget() {
    local bin="${INSTALL_HOME}/${INSTALL_BIN_PATH}"
    echo "install: ${bin}"
    mkdir -p "${bin}"

    cp "${BIN_DIR}/${GOOS}_${GOARCH}/${DFGET_BINARY_NAME}" "${bin}"

    createLink "${bin}/${DFGET_BINARY_NAME}" /usr/local/bin/dfget
}

uninstall-dfget() {
    echo "unlink /usr/local/bin/dfget"
    test -e /usr/local/bin/dfget && unlink /usr/local/bin/dfget
}

install-scheduler() {
    local bin="${INSTALL_HOME}/${INSTALL_BIN_PATH}"
    echo "install: ${bin}"
    mkdir -p "${bin}"

    cp "${BIN_DIR}/${GOOS}_${GOARCH}/${SCHEDULER_BINARY_NAME}" "${bin}"

    createLink "${bin}/${SCHEDULER_BINARY_NAME}" /usr/local/bin/scheduler
}

uninstall-scheduler() {
    echo "unlink /usr/local/bin/scheduler"
    test -e /usr/local/bin/scheduler && unlink /usr/local/bin/scheduler
}

createLink() {
    srcPath="$1"
    linkPath="$2"

    echo "create link ${linkPath} to ${srcPath}"
    test -e "${linkPath}" && unlink "${linkPath}"
    ln -s "${srcPath}" "${linkPath}"
}

createDir() {
    test -e "$1" && rm -rf "$1"
    mkdir -p "$1"
}

main() {
    case "${1-}" in
    install)
        install "${2-}"
        ;;
    uninstall)
        uninstall "${2-}"
        ;;
    *)
        echo "You must specify the subcommand 'install' or 'uninstall'."
        ;;
    esac
}

main "$@"
