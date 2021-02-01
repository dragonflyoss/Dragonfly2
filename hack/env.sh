set -o nounset
set -o errexit
set -o pipefail

export INSTALL_HOME=/opt/dragonfly
export INSTALL_CDN_PATH=df-cdn
export GO_SOURCE_EXCLUDES=( \
    "test" \
)

GOOS=$(go env GOOS)
GOARCH=$(go env GOARCH)
export GOOS
export GOARCH
