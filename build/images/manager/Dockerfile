ARG BASE_IMAGE=alpine:3.16

FROM node:12-alpine as console-builder

WORKDIR /build

COPY ./manager/console/package.json /build

RUN npm install --loglevel warn --progress false

COPY ./manager/console /build

RUN npm run build

FROM golang:1.20.1-alpine3.16 as server-builder

ARG GOPROXY
ARG GOTAGS
ARG GOGCFLAGS

WORKDIR /go/src/d7y.io/dragonfly/v2

RUN apk --no-cache add bash make gcc libc-dev git

COPY . /go/src/d7y.io/dragonfly/v2

COPY --from=console-builder /build/dist /go/src/d7y.io/dragonfly/v2/manager/dist

RUN make build-manager-server && make install-manager

FROM ${BASE_IMAGE} as health

ENV GRPC_HEALTH_PROBE_VERSION v0.4.16

RUN wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

FROM ${BASE_IMAGE}

WORKDIR /opt/dragonfly

ENV PATH=/opt/dragonfly/bin:$PATH

RUN mkdir -p /opt/dragonfly/bin/manager/console \
    && echo "hosts: files dns" > /etc/nsswitch.conf

COPY --from=server-builder /opt/dragonfly/bin/manager /opt/dragonfly/bin/server
COPY --from=health /bin/grpc_health_probe /bin/grpc_health_probe

EXPOSE 8080 65003

ENTRYPOINT ["/opt/dragonfly/bin/server"]
