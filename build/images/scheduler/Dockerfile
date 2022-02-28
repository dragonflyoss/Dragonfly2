ARG BASE_IMAGE=alpine:3.14

FROM golang:1.17.3-alpine3.14 as builder

ARG GOPROXY
ARG GOTAGS
ARG GOGCFLAGS

WORKDIR /go/src/d7y.io/dragonfly/v2

RUN apk --no-cache add bash make gcc libc-dev git

COPY . /go/src/d7y.io/dragonfly/v2

RUN make build-scheduler && make install-scheduler

FROM ${BASE_IMAGE}

ENV PATH=/opt/dragonfly/bin:$PATH
RUN echo "hosts: files dns" > /etc/nsswitch.conf

COPY --from=builder /opt/dragonfly/bin/scheduler /opt/dragonfly/bin/scheduler

EXPOSE 8002

ENTRYPOINT ["/opt/dragonfly/bin/scheduler"]
