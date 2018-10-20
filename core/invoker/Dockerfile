# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements; and to You under the Apache License, Version 2.0.

# define previous image as build stage in order to fetch results from previous image
FROM scala AS build

FROM adoptopenjdk/openjdk8:jdk8u172-b11
ENV LANG=en_US.UTF-8 \
    LANGUAGE=en_US:en \
    LC_ALL=en_US.UTF-8
ENV UID=1001 \
    NOT_ROOT_USER=owuser
ENV CONTAINERD_VERSION=1.1.4 \
    CONTAINERD_DOWNLOAD_SHA256=08cdfe5680315b43b26c974f75d5b83de1ab22a188fda9edfe18f6cb92fe93f3 \
    CNI_VERSION=0.6.0 \
    CNI_DOWNLOAD_SHA256=a7f84a742c8f3a95843b3cc636444742554a4853835649ec371a07c841daebab \
    CNI_PLUGINS_VERSION=0.7.1 \
    CNI_PLUGINS_DOWNLOAD_SHA256=6ecc5c7dbb8e4296b0d0d017e5440618e19605b9aa3b146a2c29af492f299dc7

# copy shell scripts from usual build artifact
COPY --from=build /transformEnvironment.sh /transformEnvironment.sh
COPY --from=build /copyJMXFiles.sh /copyJMXFiles.sh

RUN apt-get update && \
    apt-get install -y curl \
        iptables \
        iproute2 \
        tar \
        bridge-utils && \
    apt-get clean

# Install containerd client (remove in production as wskc directly uses GRPC)
RUN curl -sSL -o containerd-${CONTAINERD_VERSION}.linux-amd64.tar.gz https://github.com/containerd/containerd/releases/download/v${CONTAINERD_VERSION}/containerd-${CONTAINERD_VERSION}.linux-amd64.tar.gz && \
    echo "${CONTAINERD_DOWNLOAD_SHA256}  containerd-${CONTAINERD_VERSION}.linux-amd64.tar.gz" | sha256sum -c - && \
    tar -xvzf containerd-${CONTAINERD_VERSION}.linux-amd64.tar.gz && \
    rm -f containerd-${CONTAINERD_VERSION}.linux-amd64.tar.gz

# Install CNI tool (remove in production as CNI lib is directly used in wskc)
#RUN curl -sSL -o cni-amd64-v${CNI_VERSION}.tgz https://github.com/containernetworking/cni/releases/download/v${CNI_VERSION}/cni-amd64-v${CNI_VERSION}.tgz && \
#    echo "${CNI_DOWNLOAD_SHA256}  cni-amd64-v${CNI_VERSION}.tgz" | sha256sum -c - && \
#    tar --directory /usr/local/bin -zxvf cni-amd64-v${CNI_VERSION}.tgz && \
#    rm -f cni-amd64-v${CNI_VERSION}.tgz

# Install CNI plugins (bridge necessary, it is not part of wskc)
RUN curl -sSL -o cni-plugins-amd64-v${CNI_PLUGINS_VERSION}.tgz https://github.com/containernetworking/plugins/releases/download/v${CNI_PLUGINS_VERSION}/cni-plugins-amd64-v${CNI_PLUGINS_VERSION}.tgz && \
    echo "${CNI_PLUGINS_DOWNLOAD_SHA256}  cni-plugins-amd64-v${CNI_PLUGINS_VERSION}.tgz" | sha256sum -c - && \
    tar --directory /usr/local/bin -zxvf cni-plugins-amd64-v${CNI_PLUGINS_VERSION}.tgz && \
    rm -f cni-plugins-amd64-v${CNI_PLUGINS_VERSION}.tgz

# TODO currently wskc.tar must be copied into distributions folder manually
# As soon as wskc is published, this should be compiled prior the docker build
ADD build/distributions/wskc.tar ./
ADD build/distributions/invoker.tar ./

COPY init.sh /
RUN chmod +x init.sh
RUN useradd -u ${UID} -ms /bin/bash ${NOT_ROOT_USER}

EXPOSE 8080
CMD ["./init.sh", "0"]
