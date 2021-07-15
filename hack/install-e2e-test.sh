#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

KIND_CONFIG_PATH="test/testdata/kind/config.yaml"
CHARTS_CONFIG_PATH="test/testdata/charts/config.yaml"
NAMESPACE="dragonfly-system"
CHARTS_PATH="deploy/charts/dragonfly"
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

install-ingress-nginx() {
  VERSION=$(curl https://raw.githubusercontent.com/kubernetes/ingress-nginx/master/stable.txt)
  kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/${VERSION}/deploy/static/provider/kind/deploy.yaml
  kubectl wait --namespace ingress-nginx \
  --for=condition=ready pod \
  --selector=app.kubernetes.io/component=controller \
  --timeout=10m
}

install-helm() {
  if which helm >/dev/null ; then
      print_step_info "helm has been installed"
  else
      print_step_info "start install helm"
      curl -fsSL https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 | sh
  fi

  helm install --wait --timeout 10m --create-namespace --namespace ${NAMESPACE} -f ${CHARTS_CONFIG_PATH} dragonfly ${CHARTS_PATH}
}

install-ginkgo() {
  if which ginkgo >/dev/null ; then
      print_step_info "ginkgo has been installed"
  else
      go get github.com/onsi/ginkgo/ginkgo
  fi
}

install-bombardier() {
  if which bombardier >/dev/null ; then
      print_step_info "bombardier has been installed"
  else
      go get github.com/codesenberg/bombardier
  fi
}

install-local() {
  print_step_info "start kind create cluster"
  install-kind

  print_step_info "start install ingress nginx"
  install-ingress-nginx

  print_step_info "start building docker images"
  make docker-build

  print_step_info "start loading image for kind"
  make kind-load

  print_step_info "start helm install dragonfly"
  install-helm

  print_step_info "start install ginkgo"
  install-ginkgo

  print_step_info "start install bombardier"
  install-bombardier
}

install-actions() {
  print_step_info "start install ingress nginx"
  install-ingress-nginx

  print_step_info "start building docker images"
  make docker-build

  print_step_info "start loading image for kind"
  make kind-load

  print_step_info "start helm install dragonfly"
  install-helm

  print_step_info "start install ginkgo"
  install-ginkgo

  print_step_info "start install bombardier"
  install-bombardier
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
