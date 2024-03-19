FROM quay.io/iovisor/bpftrace:v0.13.0

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
    && apt-get install -y binutils bpfcc-tools curl dnsutils git iproute2 iputils-ping jq netcat socat tree vim wget \
    && apt-get install -y openssh-client openssh-server \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /run/sshd

ENV PATH=/go/bin:/usr/local/go/bin:$PATH

ENV GOPATH=/go
ENV GOLANG_VERSION 1.21.1
ENV GOLANG_DOWNLOAD_URL https://go.dev/dl/go1.21.1.linux-amd64.tar.gz
ENV GOLANG_DOWNLOAD_SHA256 000a5b1fca4f75895f78befeb2eecf10bfff3c428597f3f1e69133b63b911b02

ARG GOPROXY

RUN curl -fsSL "$GOLANG_DOWNLOAD_URL" -o golang.tar.gz \
	&& echo "$GOLANG_DOWNLOAD_SHA256  golang.tar.gz" | sha256sum -c - \
	&& tar -C /usr/local -xzf golang.tar.gz \
	&& rm golang.tar.gz \
	&& go install github.com/go-delve/delve/cmd/dlv@v1.7.0

# Create none root user and group
ARG USER_NAME=dragonfly
ARG USER_UID=1000
ARG GROUP_NAME=dragonfly
ARG GROUP_GID=1000
RUN groupadd -g $GROUP_GID $GROUP_NAME && useradd -u $USER_UID -g $GROUP_GID -m -s /bin/bash $USER_NAME
