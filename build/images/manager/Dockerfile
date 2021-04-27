FROM golang:1.15.10-alpine as builder

WORKDIR /go/src/d7y.io/dragonfly/v2

RUN apk --no-cache add bash make gcc libc-dev git

COPY . /go/src/d7y.io/dragonfly/v2

ARG GOPROXY

RUN make build-manager && make install-manager

FROM alpine:3.12

RUN apk add --no-cache netcat-openbsd

ENV PATH=/opt/dragonfly/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

COPY --from=builder /go/src/d7y.io/dragonfly/v2/hack/wait-for.sh /root/wait-for.sh
COPY --from=builder /opt/dragonfly/bin/manager /opt/dragonfly/bin/manager

EXPOSE 8004

ENTRYPOINT ["/opt/dragonfly/bin/manager"]
