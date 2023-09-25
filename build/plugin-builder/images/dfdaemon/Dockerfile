ARG BASE_IMAGE=alpine:3.17

FROM ${BASE_IMAGE}

ENV PATH=/opt/dragonfly/bin:$PATH
RUN echo "hosts: files dns" > /etc/nsswitch.conf && \
    mkdir -p /usr/local/dragonfly/plugins/

COPY ./artifacts/binaries/dfget /opt/dragonfly/bin/dfget
COPY ./artifacts/plugins/d7y-resource-plugin-* /usr/local/dragonfly/plugins/

EXPOSE 65001

ENTRYPOINT ["/opt/dragonfly/bin/dfget", "daemon"]

