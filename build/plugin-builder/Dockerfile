FROM golang:1.21.1-alpine3.17

ARG GOPROXY
ARG GOTAGS
ARG GOGCFLAGS

WORKDIR /go/src/d7y.io/dragonfly/v2

RUN apk --no-cache add bash make gcc libc-dev git

COPY . /go/src/d7y.io/dragonfly/v2

ENV CGO_ENABLED="1"
ENV GO111MODULE="on"

COPY build/plugin-builder/build.sh /
