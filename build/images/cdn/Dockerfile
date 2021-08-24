FROM golang:1.16.6-alpine as builder

WORKDIR /go/src/d7y.io/dragonfly/v2

RUN apk --no-cache add bash make gcc libc-dev git

COPY . /go/src/d7y.io/dragonfly/v2

ARG GOPROXY
ARG GOTAGS
ARG GOGCFLAGS

RUN make build-cdn && make install-cdn

# TODO support BASE_IMAGE
FROM nginx:1.19-alpine

COPY --from=builder /go/src/d7y.io/dragonfly/v2/hack/start-cdn.sh /root/start.sh
COPY --from=builder /go/src/d7y.io/dragonfly/v2/hack/cdn-nginx.conf /etc/nginx/nginx.conf
COPY --from=builder /opt/dragonfly/df-cdn/cdn /opt/dragonfly/df-cdn/cdn
RUN apk --no-cache add curl

RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/v1.18.0/bin/linux/amd64/kubectl
RUN chmod +x ./kubectl
RUN mv ./kubectl /usr/local/bin/kubectl
RUN kubectl version --client

EXPOSE 8001 8003

ENTRYPOINT ["/root/start.sh"]
