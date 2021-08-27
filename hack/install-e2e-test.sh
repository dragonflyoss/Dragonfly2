#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

KIND_CONFIG_PATH="test/testdata/kind/config.yaml"
CHARTS_CONFIG_PATH="test/testdata/charts/config.yaml"
FILE_SERVER_CONFIG_PATH="test/testdata/k8s/file-server.yaml"
CURL_CONFIG_PATH="test/testdata/k8s/curl.yaml"
CHARTS_PATH="deploy/helm-charts/charts/dragonfly"
NAMESPACE="dragonfly-system"
E2E_NAMESPACE="dragonfly-e2e"
FILE_SERVER_NAME="file-server-0"
CURL_COMPONENT="curl"
curDir=$(cd "$(dirname "$0")" && pwd)
cd "${curDir}/../" || return

install-kind() {
  if which kind >/dev/null ; then
      print_step_info "kind has been installed"
  else 
      print_step_info "start install kind"
      go install sigs.k8s.io/kind@v0.11.1
  fi

  kind create cluster --config ${KIND_CONFIG_PATH}
}

install-helm() {
  if which helm >/dev/null ; then
      print_step_info "helm has been installed"
  else
      print_step_info "start install helm"
      curl -fsSL https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 | sh
  fi

  helm install --wait --timeout 10m --dependency-update --create-namespace --namespace ${NAMESPACE} -f ${CHARTS_CONFIG_PATH} dragonfly ${CHARTS_PATH}
}

install-file-server() {
  kubectl apply -f ${FILE_SERVER_CONFIG_PATH}
  kubectl wait --namespace ${E2E_NAMESPACE} \
  --for=condition=ready pod ${FILE_SERVER_NAME} \
  --timeout=10m
}

install-curl() {
  kubectl apply -f ${CURL_CONFIG_PATH}
  kubectl wait --namespace ${E2E_NAMESPACE} \
  --for=condition=ready pod -l component=${CURL_COMPONENT} \
  --timeout=10m
}

install-ginkgo() {
  if which ginkgo >/dev/null ; then
      print_step_info "ginkgo has been installed"
  else
      go get github.com/onsi/ginkgo/ginkgo
  fi
}

install-apache-bench() {
  if which ab >/dev/null ; then
      print_step_info "apache bench has been installed"
  else
      apt-get update
      apt-get install apache2-utils
  fi
}

install-local() {
  print_step_info "start kind create cluster"
  install-kind

  print_step_info "start building docker images"
  make docker-build

  print_step_info "start loading image for kind"
  make kind-load

  print_step_info "start helm install dragonfly"
  install-helm

  print_step_info "start install file server"
  install-file-server

  print_step_info "start install curl"
  install-curl

  print_step_info "start install ginkgo"
  install-ginkgo

  print_step_info "start install apache bench"
  install-apache-bench
}

install-actions() {
  print_step_info "start building docker images"
  make docker-build

  print_step_info "start loading image for kind"
  make kind-load

  print_step_info "start helm install dragonfly"
  install-helm

  print_step_info "start install file server"
  install-file-server

  print_step_info "start install curl"
  install-curl

  print_step_info "start install ginkgo"
  install-ginkgo

  print_step_info "start install apache bench"
  install-apache-bench
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
