#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

BIN_DIR="../bin"
CDN_BINARY_NAME=cdn

curDir=$(cd "$(dirname "$0")" && pwd)
cd "${curDir}" || return

. ./env.sh

install() {
    case "${1-}" in
        cdn)
            install-cdn
        ;;
    esac
}

install-cdn(){
    local installCdnDir="${INSTALL_HOME}/${INSTALL_CDN_PATH}"
    echo "install: ${installCdnDir}"
    createDir "${installCdnDir}"

    cp "${BIN_DIR}/${GOOS}_${GOARCH}/${CDN_BINARY_NAME}"  "${installCdnDir}"

    createLink "${installCdnDir}/${CDN_BINARY_NAME}" /usr/local/bin/cdn
}

uninstall-cdn() {
    echo "unlink /usr/local/bin/cdn"
    test -e /usr/local/bin/cdn && unlink /usr/local/bin/cdn
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
