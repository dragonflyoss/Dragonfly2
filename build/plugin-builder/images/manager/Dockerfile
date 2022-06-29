ARG BASE_IMAGE=alpine:3.16

FROM node:12-alpine as console-builder
WORKDIR /build
COPY ./manager/console/package.json /build
RUN npm install --loglevel warn --progress false
COPY ./manager/console /build
RUN npm run build

FROM ${BASE_IMAGE}
WORKDIR /opt/dragonfly
ENV PATH=/opt/dragonfly/bin:$PATH
RUN mkdir -p /opt/dragonfly/bin/manager/console \
    && echo "hosts: files dns" > /etc/nsswitch.conf  && \
    mkdir -p /usr/local/dragonfly/plugins/
COPY ./artifacts/binaries/manager /opt/dragonfly/bin/server
COPY ./artifacts/plugins/d7y-manager-plugin-* /usr/local/dragonfly/plugins/
COPY --from=console-builder /build/dist /opt/dragonfly/manager/console/dist
EXPOSE 8080 65003
ENTRYPOINT ["/opt/dragonfly/bin/server"]
