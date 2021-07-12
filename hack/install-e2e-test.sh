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
      print_step_info "wait for kind create cluster"
      kind create cluster --config ${KIND_CONFIG_PATH}
  else 
      go install sigs.k8s.io/kind@v0.11.1
      print_step_info "install kind successed"
      print_step_info "wait for kind create cluster"
      kind create cluster --config ${KIND_CONFIG_PATH}
  fi
}

install-helm() {
  if which helm >/dev/null ; then
      print_step_info "wait for helm install dragonfly"
      helm install --wait --timeout 10m --create-namespace --namespace ${NAMESPACE} dragonfly ${CHARTS_PATH}
  else
      curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3
      chmod 700 get_helm.sh
      ./get_helm.sh
      print_step_info "install helm success"
      print_step_info "wait for helm install dragonfly"
      helm install --wait --timeout 10m --create-namespace --namespace ${NAMESPACE} dragonfly ${CHARTS_PATH}
  fi
}

install-ginkgo() {
  if which ginkgo >/dev/null ; then
      print_step_info "ginkgo has been installed"
  else
      go get -u github.com/onsi/ginkgo/ginkgo
      print_step_info "install ginkgo success"
  fi
}

install-local() {
  print_step_info "start kind create cluster"
  install-kind
  print_step_info "kind create cluster successed"

  print_step_info "start building docker images"
  make docker-build
  print_step_info "build docker images successed"

  print_step_info "start loading image for kind"
  make kind-load
  print_step_info "load docker images successed"

  print_step_info "start helm install dragonfly"
  install-helm
  print_step_info "helm install dragonfly successed"

  print_step_info "start ginkgo install dragonfly"
  install-ginkgo
  print_step_info "ginkgo install dragonfly successed"
}

install-actions() {
  print_step_info "start building docker images"
  make docker-build
  print_step_info "build docker images successed"

  print_step_info "start loading image for kind"
  make kind-load
  print_step_info "load docker images successed"

  print_step_info "start helm install dragonfly"
  install-helm
  print_step_info "helm install dragonfly successed"

  print_step_info "start ginkgo install dragonfly"
  install-ginkgo
  print_step_info "ginkgo install dragonfly successed"
}

print_step_info() {
  echo "-----------------------------"
  echo $1
  echo "-----------------------------"
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
