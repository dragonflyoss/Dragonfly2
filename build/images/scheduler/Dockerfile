FROM golang:1.15.10-alpine as builder

WORKDIR /go/src/d7y.io/dragonfly.v2

RUN apk --no-cache add bash make gcc libc-dev git

COPY . /go/src/d7y.io/dragonfly.v2

ARG GOPROXY

RUN make build-scheduler && make install-scheduler

FROM alpine:3.12

ENV PATH=/opt/dragonfly/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

COPY --from=builder /opt/dragonfly/bin/scheduler /opt/dragonfly/bin/scheduler

EXPOSE 8002

ENTRYPOINT ["/opt/dragonfly/bin/scheduler"]
