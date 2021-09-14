ARG BASE_IMAGE=alpine:3.12

FROM golang:1.16.6-alpine as builder

WORKDIR /go/src/d7y.io/dragonfly/v2

RUN apk --no-cache add bash make gcc libc-dev git

COPY . /go/src/d7y.io/dragonfly/v2

ARG GOPROXY
ARG GOTAGS
ARG GOGCFLAGS

RUN make build-dfget && make install-dfget

FROM ${BASE_IMAGE}

ENV PATH=/opt/dragonfly/bin:$PATH

COPY --from=builder /opt/dragonfly/bin/dfget /opt/dragonfly/bin/dfget

EXPOSE 65001

ENTRYPOINT ["/opt/dragonfly/bin/dfget", "daemon"]

