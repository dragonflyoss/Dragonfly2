ARG BASE_IMAGE=alpine:3.14

FROM golang:1.17.3-alpine3.14 as builder

ARG GOPROXY
ARG GOTAGS
ARG GOGCFLAGS

WORKDIR /go/src/d7y.io/dragonfly/v2

RUN apk --no-cache add bash make gcc libc-dev git

COPY . /go/src/d7y.io/dragonfly/v2

RUN make build-dfget && make install-dfget

FROM ${BASE_IMAGE}

ENV PATH=/opt/dragonfly/bin:$PATH
RUN echo "hosts: files dns" > /etc/nsswitch.conf

COPY --from=builder /opt/dragonfly/bin/dfget /opt/dragonfly/bin/dfget

EXPOSE 65001

ENTRYPOINT ["/opt/dragonfly/bin/dfget", "daemon"]

