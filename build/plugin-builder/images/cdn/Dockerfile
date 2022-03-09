FROM nginx:1.19-alpine

ENV PATH=/opt/dragonfly/bin:$PATH
RUN echo "hosts: files dns" > /etc/nsswitch.conf && \
    mkdir -p /usr/local/dragonfly/plugins/

COPY ./hack/start-cdn.sh /root/start.sh
COPY ./hack/cdn-nginx.conf /etc/nginx/nginx.conf
COPY ./artifacts/binaries/cdn /opt/dragonfly/bin/cdn
COPY ./artifacts/plugins/d7y-resource-plugin-* /usr/local/dragonfly/plugins/

EXPOSE 8001 8003

ENTRYPOINT ["/root/start.sh"]
