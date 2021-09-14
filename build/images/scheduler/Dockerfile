ARG BASE_IMAGE=alpine:3.12

FROM golang:1.16.6-alpine as builder

WORKDIR /go/src/d7y.io/dragonfly/v2

RUN apk --no-cache add bash make gcc libc-dev git

COPY . /go/src/d7y.io/dragonfly/v2

ARG GOPROXY
ARG GOTAGS
ARG GOGCFLAGS

RUN make build-scheduler && make install-scheduler

FROM ${BASE_IMAGE}

ENV PATH=/opt/dragonfly/bin:$PATH

COPY --from=builder /opt/dragonfly/bin/scheduler /opt/dragonfly/bin/scheduler

EXPOSE 8002

ENTRYPOINT ["/opt/dragonfly/bin/scheduler"]
