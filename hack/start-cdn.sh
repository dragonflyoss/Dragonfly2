#!/usr/bin/env sh

set -o nounset
set -o errexit
set -o pipefail

#nginx

/opt/dragonfly/bin/cdn "$@"
