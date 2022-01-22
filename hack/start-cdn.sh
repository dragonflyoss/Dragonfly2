#!/usr/bin/env sh

set -o nounset
set -o errexit
set -o pipefail

/opt/dragonfly/bin/cdn "$@"
