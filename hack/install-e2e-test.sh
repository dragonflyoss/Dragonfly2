#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

KIND_CONFIG_PATH="test/testdata/kind/config.yaml"
NAMESPACE="dragonfly-system"
CHARTS_PATH="deploy/charts/dragonfly"
curDir=$(cd "$(dirname "$0")" && pwd)
cd "${curDir}/../" || return

install-kind() {
  if which kind >/dev/null ; then
      echo "wait for kind create cluster"
      kind create cluster --config ${KIND_CONFIG_PATH}
  else 
      go install sigs.k8s.io/kind@v0.11.1
      echo "install kind successed"
      kind create cluster --config ${KIND_CONFIG_PATH}
      echo "wait for kind create cluster"
  fi
}

install-helm() {
  if which helm >/dev/null ; then
      echo "wait for helm install dragonfly"
      helm install --wait --timeout 3m --create-namespace --namespace ${NAMESPACE} dragonfly ${CHARTS_PATH}
  else
      go install sigs.k8s.io/kind@v0.11.1
      echo "install helm success"
      curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3
      chmod 700 get_helm.sh
      ./get_helm.sh
      echo "wait for helm install dragonfly"
  fi
}

install-local() {
  echo "start kind create cluster"
  install-kind
  echo "kind create cluster successed"

  echo "start building docker images"
  make docker-build
  echo "build docker images successed"

  echo "start loading image for kind"
  make kind-load
  echo "load docker images successed"

  echo "start helm install dragonfly"
  install-helm
  echo "helm install dragonfly successed"
}

install-actions() {
  echo "start building docker images"
  make docker-build
  echo "build docker images successed"

  echo "start loading image for kind"
  make kind-load
  echo "load docker images successed"

  echo "start helm install dragonfly"
  install-helm
  echo "helm install dragonfly successed"
}

main() {
    case "${1-}" in
    local)
        install-local
        ;;
    actions)
        install-actions
    esac
}

main "$@"
