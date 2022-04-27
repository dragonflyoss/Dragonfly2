FROM golang:1.17.3-alpine3.14 as builder

ARG GOPROXY
ARG GOTAGS
ARG GOGCFLAGS

WORKDIR /go/src/d7y.io/dragonfly/v2

RUN apk --no-cache add bash make gcc libc-dev git

COPY . /go/src/d7y.io/dragonfly/v2

RUN make build-cdn && make install-cdn

FROM alpine:3.14 as health

RUN GRPC_HEALTH_PROBE_VERSION=v0.4.8 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

# TODO support BASE_IMAGE
FROM nginx:1.21-alpine

ENV PATH=/opt/dragonfly/bin:$PATH
RUN echo "hosts: files dns" > /etc/nsswitch.conf

COPY --from=builder /go/src/d7y.io/dragonfly/v2/hack/start-cdn.sh /root/start.sh
COPY --from=builder /go/src/d7y.io/dragonfly/v2/hack/cdn-nginx.conf /etc/nginx/nginx.conf
COPY --from=builder /opt/dragonfly/bin/cdn /opt/dragonfly/bin/cdn
COPY --from=health /bin/grpc_health_probe /bin/grpc_health_probe

EXPOSE 8001 8003

ENTRYPOINT ["/root/start.sh"]
