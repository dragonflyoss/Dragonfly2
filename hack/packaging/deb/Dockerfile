FROM golang:1.16-buster AS build

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update -q && \
    apt-get install -y -qq build-essential devscripts dh-make dh-systemd && \
    mkdir -p /build/debian

COPY . /go/src/d7y.io/dragonfly/v2

CMD ["/go/src/d7y.io/dragonfly/v2/hack/packaging/deb/build.sh"]
