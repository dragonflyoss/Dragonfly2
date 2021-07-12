FROM golang:1.15.10-alpine as builder

WORKDIR /go/src/d7y.io/dragonfly.v2

RUN apk --no-cache add bash make gcc libc-dev git

COPY . /go/src/d7y.io/dragonfly.v2

ARG GOPROXY

RUN make build-cdn && make install-cdn

FROM nginx:1.19-alpine

COPY --from=builder /go/src/d7y.io/dragonfly.v2/hack/start-cdn.sh /root/start.sh
COPY --from=builder /go/src/d7y.io/dragonfly.v2/hack/cdn-nginx.conf /etc/nginx/nginx.conf
COPY --from=builder /opt/dragonfly/df-cdn/cdn /opt/dragonfly/df-cdn/cdn

EXPOSE 8001 8003

ENTRYPOINT ["/root/start.sh"]
