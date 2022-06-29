FROM golang:1.18.3-alpine3.16

ARG GOPROXY
ARG GOTAGS
ARG GOGCFLAGS

WORKDIR /go/src/d7y.io/dragonfly/v2

RUN apk --no-cache add bash make gcc libc-dev git

COPY . /go/src/d7y.io/dragonfly/v2

# prepare golang dependency
RUN ls -alh && \
    go mod vendor && \
    mv /go/src/d7y.io/dragonfly/v2/vendor/* /go/src/ && \
    rmdir /go/src/d7y.io/dragonfly/v2/vendor

ENV CGO_ENABLED="1"
ENV GO111MODULE="off"

COPY build/plugin-builder/build.sh /
