#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

KIND_CONFIG_PATH="test/testdata/kind/config.yaml"
CHARTS_CONFIG_PATH="test/testdata/charts/config.yaml"
FILE_SERVER_CONFIG_PATH="test/testdata/k8s/file-server.yaml"
FILE_SERVER_NO_CONTENT_CONFIG_PATH="test/testdata/k8s/file-server.yaml"
PROXY_SERVER_CONFIG_PATH="test/testdata/k8s/proxy.yaml"
MINIO_SERVER_CONFIG_PATH="test/testdata/k8s/minio.yaml"
CHARTS_PATH="deploy/helm-charts/charts/dragonfly"
NAMESPACE="dragonfly-system"
E2E_NAMESPACE="dragonfly-e2e"
FILE_SERVER_NAME="file-server-0"
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

  helm upgrade --install --wait --timeout 10m --dependency-update --create-namespace --namespace ${NAMESPACE} -f ${CHARTS_CONFIG_PATH} dragonfly ${CHARTS_PATH}
}

install-file-server() {
  kubectl apply -f ${FILE_SERVER_CONFIG_PATH}
  kubectl apply -f ${FILE_SERVER_NO_CONTENT_CONFIG_PATH}
  kubectl wait --namespace ${E2E_NAMESPACE} \
    --for=condition=ready pod ${FILE_SERVER_NAME} \
    --timeout=10m
  kubectl wait --namespace ${E2E_NAMESPACE} \
    --for=condition=ready pod file-server-no-content-length-0 \
    --timeout=10m
}

install-proxy-server() {
  kubectl apply -f ${PROXY_SERVER_CONFIG_PATH}
  kubectl wait --namespace ${E2E_NAMESPACE} \
    --for=condition=ready pod proxy-0 \
    --timeout=10m
  kubectl wait --namespace ${E2E_NAMESPACE} \
    --for=condition=ready pod proxy-1 \
    --timeout=10m
  kubectl wait --namespace ${E2E_NAMESPACE} \
    --for=condition=ready pod proxy-2 \
    --timeout=10m
}

install-minio-server() {
  kubectl apply -f ${MINIO_SERVER_CONFIG_PATH}
  kubectl wait --namespace ${E2E_NAMESPACE} \
    --for=condition=ready pod minio-0 \
    --timeout=10m
}

install-ginkgo() {
  if which ginkgo >/dev/null ; then
      print_step_info "ginkgo has been installed"
  else
      go install github.com/onsi/ginkgo/v2/ginkgo@v2.12.0
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

print_step_info() {
  echo "-----------------------------"
  echo $1
  echo "-----------------------------"
}

main() {
  print_step_info "start kind create cluster"
  install-kind

  print_step_info "start building docker images"
  make docker-build docker-build-testing-tools

  print_step_info "start loading image for kind"
  make kind-load

  print_step_info "start helm install dragonfly"
  install-helm

  print_step_info "start install file server"
  install-file-server

  print_step_info "start install proxy server"
  install-proxy-server

  print_step_info "start install minio server"
  install-minio-server

  print_step_info "start install ginkgo"
  install-ginkgo

  print_step_info "start install apache bench"
  install-apache-bench
}

main "$@"
