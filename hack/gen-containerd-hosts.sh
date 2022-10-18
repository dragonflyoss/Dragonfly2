#!/bin/bash

# the register to pull image, like "docker.io", when pull image with "docker.io/library/alpine:latest"
registry=${1:-${REGISTRY}}

# the real server which serves image pulling, like "index.docker.io"
# in normal case, registry_server is same with registry
registry_server=${REGISTRY_SERVER:-${registry}}

# dragonfly proxy url
d7y_proxy=${2:-http://127.0.0.1:65001}

if [[ -z "${registry}" ]]; then
    echo empty registry domain
    exit 1
fi

conf_dir=${CONTAINED_CONFIG_DIR:-/etc/containerd/certs.d}

mkdir -p "$conf_dir/${registry}"

cat << EOF > "$conf_dir/${registry}"/hosts.toml
server = "https://${registry_server}"

[host."${d7y_proxy}"]
  capabilities = ["pull", "resolve"]
  [host."${d7y_proxy}".header]
    X-Dragonfly-Registry = ["https://${registry_server}"]
  [host."https://${registry_server}"]
    capabilities = ["pull", "resolve"]
EOF
