FROM golang:1.15.10-alpine as build

WORKDIR /go/src/d7y.io/dragonfly/v2

RUN apk --no-cache add bash make gcc libc-dev git

COPY . /go/src/d7y.io/dragonfly/v2

ARG GOPROXY
RUN make build-dfget && make install-dfget

FROM centos:8
RUN yum install -y rpm-build
RUN mkdir -p /root/rpmbuild/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
RUN echo "%_topdir /root/rpmbuild" >> .rpmmacros
COPY --from=build /go/src/d7y.io/dragonfly/v2/LICENSE /root/rpmbuild/SOURCES/
COPY --from=build /go/src/d7y.io/dragonfly/v2/bin/linux_amd64/dfget /root/rpmbuild/SOURCES/
COPY --from=build /go/src/d7y.io/dragonfly/v2/docs/ChangeLog /root/rpmbuild/SOURCES/
COPY --from=build /go/src/d7y.io/dragonfly/v2/docs/cli-reference/dfget.1 /root/rpmbuild/SOURCES/
COPY --from=build /go/src/d7y.io/dragonfly/v2/hack/packaging/systemd/dfget-daemon.service /root/rpmbuild/SOURCES/
COPY --from=build /go/src/d7y.io/dragonfly/v2/hack/packaging/systemd/dfget-daemon.service.d/CPUQuota.conf /root/rpmbuild/SOURCES/
COPY --from=build /go/src/d7y.io/dragonfly/v2/hack/packaging/systemd/dfget-daemon.service.d/CPUShares.conf /root/rpmbuild/SOURCES/
COPY --from=build /go/src/d7y.io/dragonfly/v2/hack/packaging/systemd/dfget-daemon.service.d/MemoryLimit.conf /root/rpmbuild/SOURCES/
COPY --from=build /go/src/d7y.io/dragonfly/v2/hack/packaging/systemd/fix.dfget-daemon.cpuset.sh /root/rpmbuild/SOURCES/
COPY --from=build /go/src/d7y.io/dragonfly/v2/hack/packaging/config/dfget-daemon.yaml /root/rpmbuild/SOURCES/
COPY --from=build /go/src/d7y.io/dragonfly/v2/hack/packaging/rpm/dfget.spec /root/rpmbuild/SPECS/
