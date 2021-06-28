#!/bin/bash

dir=$(dirname $(readlink -f "$0"))
root=$(cd "$dir" && cd .. && pwd)

if [[ -z "$root" ]]; then
    exit 1
fi

src=$root/deploy/kustomize/single-cluster-openkruise
dst=$root/deploy/kustomize/single-cluster-native

rm -rf "$dst"

mkdir -p $dst/overlays

cp -r "$src/bases" "$dst/bases"
#cp -r "$src/overlays/minikube" "$dst/overlays/minikube"
cp -r "$src/overlays/sample" "$dst/overlays/sample"

sed -i 's#apps.kruise.io/v1.*#apps/v1#' \
  "$dst/bases/cdn/statefulset.yaml" \
  "$dst/bases/dfdaemon/daemonset.yaml" \
  "$dst/bases/scheduler/statefulset.yaml"

sed -i '1,2d' \
  "$dst/bases/cdn/statefulset.yaml" \
  "$dst/bases/dfdaemon/daemonset.yaml" \
  "$dst/bases/scheduler/statefulset.yaml"
