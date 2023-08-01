ARG BASE_IMAGE=alpine:3.16

FROM golang:1.20.1-alpine3.16 as builder

ARG GOPROXY
ARG GOTAGS
ARG GOGCFLAGS

WORKDIR /go/src/d7y.io/dragonfly/v2

RUN apk --no-cache add bash make gcc libc-dev git

COPY . /go/src/d7y.io/dragonfly/v2

RUN make build-trainer && make install-trainer

FROM ${BASE_IMAGE} as health

ENV GRPC_HEALTH_PROBE_VERSION v0.4.16

RUN wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

FROM ${BASE_IMAGE}

ENV PATH=/opt/dragonfly/bin:$PATH
RUN echo "hosts: files dns" > /etc/nsswitch.conf

ENV TENSORFLOW_LIB_URL https://storage.googleapis.com/tensorflow/libtensorflow/libtensorflow-cpu-linux-x86_64-2.6.0.tar.gz
RUN curl -fsSL "$TENSORFLOW_LIB_URL" -o tensorflow_lib.tar.gz \
    && tar -C /usr/local -xzf tensorflow_lib.tar.gz \
	&& rm tensorflow_lib.tar.gz 

COPY --from=builder /opt/dragonfly/bin/trainer /opt/dragonfly/bin/trainer
COPY --from=health /bin/grpc_health_probe /bin/grpc_health_probe

EXPOSE 9090

ENTRYPOINT ["/opt/dragonfly/bin/trainer"]
