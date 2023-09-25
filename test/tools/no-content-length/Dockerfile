ARG BASE_IMAGE=alpine:3.17

FROM golang:1.21.1-alpine3.17 as builder

COPY . /go/src/

ARG GOPROXY
ARG GOTAGS
ARG GOGCFLAGS

RUN cd /go/src/ && GO111MODULE=off go build -o /tmp/no-content-length .

FROM ${BASE_IMAGE}

COPY --from=builder /tmp/no-content-length /usr/local/bin/

RUN echo "hosts: files dns" > /etc/nsswitch.conf

EXPOSE 80

WORKDIR /

ENTRYPOINT ["/usr/local/bin/no-content-length"]

