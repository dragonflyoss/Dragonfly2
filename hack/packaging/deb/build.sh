#!/bin/sh

PKG_ARCH=$(dpkg --print-architecture)
PKG_DATE=$(date -R)
PKG_VERSION=$(cd /go/src/d7y.io/dragonfly/v2 && git describe --tags --abbrev=0)
PKG_VERSION=${PKG_VERSION:-2-unknown}

echo "PKG_VERSION=$PKG_VERSION"
echo "PKG_ARCH=$PKG_ARCH"
echo "PKG_DATE=$PKG_DATE"

cd /go/src/d7y.io/dragonfly/v2 &&
  make build-dfget &&
  mkdir -p /build/debian &&
  cd /build &&
  cp /go/src/d7y.io/dragonfly/v2/bin/linux_amd64/dfget /build/ &&
  cp /go/src/d7y.io/dragonfly/v2/docs/en/cli-reference/dfget.1 /build/ &&
  cp /go/src/d7y.io/dragonfly/v2/LICENSE /build/ &&
  cp /go/src/d7y.io/dragonfly/v2/docs/en/config/dfget.yaml /build/ &&
  cp /go/src/d7y.io/dragonfly/v2/hack/packaging/systemd/dfget-daemon.service /build/debian/ &&
  cp /go/src/d7y.io/dragonfly/v2/hack/packaging/deb/compat /build/debian/compat &&
  cp /go/src/d7y.io/dragonfly/v2/hack/packaging/deb/copyright /build/debian/copyright &&
  cp /go/src/d7y.io/dragonfly/v2/hack/packaging/deb/dfget.manpages /build/debian/dfget.manpages &&
  cp /go/src/d7y.io/dragonfly/v2/hack/packaging/deb/dfget.postinst /build/debian/dfget.postinst &&
  cp /go/src/d7y.io/dragonfly/v2/hack/packaging/deb/rules /build/debian/rules &&
  echo "dfget ($PKG_VERSION) experimental; urgency=low" >/build/debian/changelog &&
  echo "  * dfget version $PKG_VERSION" >>/build/debian/changelog &&
  echo " -- Dragonfly Group <group@d7y.io>  $PKG_DATE" >>/build/debian/changelog &&
  sed "s/__PKG_ARCH__/${PKG_ARCH}/g" /go/src/d7y.io/dragonfly/v2/hack/packaging/deb/control >/build/debian/control &&
  dpkg-buildpackage -us -uc -b &&
  cp ../*.deb /pkg/
# FIXME error and warning
lintian --check --color always /pkg/*.deb | true
