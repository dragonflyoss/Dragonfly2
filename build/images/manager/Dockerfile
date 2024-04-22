ARG BASE_IMAGE=alpine:3.17

FROM node:20-alpine as console-builder

WORKDIR /build

COPY ./manager/console/package.json /build

RUN yarn install --frozen-lockfile --network-timeout 1000000

COPY ./manager/console /build

RUN yarn build

FROM golang:1.21.1-alpine3.17 as server-builder

ARG GOPROXY
ARG GOTAGS
ARG GOGCFLAGS

WORKDIR /go/src/d7y.io/dragonfly/v2

RUN apk --no-cache add bash make gcc libc-dev git

COPY . /go/src/d7y.io/dragonfly/v2

COPY --from=console-builder /build/dist /go/src/d7y.io/dragonfly/v2/manager/dist

RUN make build-manager-server && make install-manager

FROM ${BASE_IMAGE} as health

ENV GRPC_HEALTH_PROBE_VERSION v0.4.24

RUN if [ "$(uname -m)" = "ppc64le" ]; then \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-ppc64le; \
    elif [ "$(uname -m)" = "aarch64" ]; then \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-arm64; \
    else \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64; \
    fi && \
    chmod +x /bin/grpc_health_probe

FROM ${BASE_IMAGE}

ARG USER_NAME=dragonfly
ARG USER_UID=1000
ARG GROUP_NAME=dragonfly
ARG GROUP_GID=1000

WORKDIR /opt/dragonfly

ENV PATH=/opt/dragonfly/bin:$PATH

RUN mkdir -p /opt/dragonfly/bin/manager/console \
    && echo "hosts: files dns" > /etc/nsswitch.conf

# NOTE: Command works for alpine linux, for other distro please update run statement or create user/group in base image
RUN if [ getent group "$GROUP_NAME" ]; then \
    echo "group exists"; \
    else \
    addgroup -S -g $GROUP_GID $GROUP_NAME; \
    fi
RUN if [ getent passwd "$USER_NAME" ]; then \
    echo "user exists"; \
    else \
    adduser -S -u $USER_UID -G $GROUP_NAME  -D $USER_NAME; \
    fi

COPY --from=server-builder --chown=$GROUP_NAME:$USER_NAME /opt/dragonfly/bin/manager /opt/dragonfly/bin/server
COPY --from=health --chown=$GROUP_NAME:$USER_NAME /bin/grpc_health_probe /bin/grpc_health_probe

EXPOSE 8080 65003

ENTRYPOINT ["/opt/dragonfly/bin/server"]
