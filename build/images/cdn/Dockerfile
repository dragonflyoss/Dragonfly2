FROM golang:1.17.3-alpine3.14 as builder

ARG GOPROXY
ARG GOTAGS
ARG GOGCFLAGS

WORKDIR /go/src/d7y.io/dragonfly/v2

RUN apk --no-cache add bash make gcc libc-dev git

COPY . /go/src/d7y.io/dragonfly/v2

RUN make build-cdn && make install-cdn

# TODO support BASE_IMAGE
FROM nginx:1.19-alpine

ENV PATH=/opt/dragonfly/bin:$PATH
RUN echo "hosts: files dns" > /etc/nsswitch.conf

COPY --from=builder /go/src/d7y.io/dragonfly/v2/hack/start-cdn.sh /root/start.sh
COPY --from=builder /go/src/d7y.io/dragonfly/v2/hack/cdn-nginx.conf /etc/nginx/nginx.conf
COPY --from=builder /opt/dragonfly/bin/cdn /opt/dragonfly/bin/cdn

EXPOSE 8001 8003

ENTRYPOINT ["/root/start.sh"]
