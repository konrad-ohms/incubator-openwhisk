# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements; and to You under the Apache License, Version 2.0.

# define previous image as build stage in order to fetch results from previous image
FROM scala AS build

FROM adoptopenjdk/openjdk8:jdk8u172-b11
ENV UID=1001 \
    NOT_ROOT_USER=owuser
ENV CNI_PLUGINS_VERSION=0.7.1 \
    CNI_PLUGINS_DOWNLOAD_SHA256=6ecc5c7dbb8e4296b0d0d017e5440618e19605b9aa3b146a2c29af492f299dc7

# copy shell scripts from usual build artifact
COPY --from=build /transformEnvironment.sh /transformEnvironment.sh
COPY --from=build /copyJMXFiles.sh /copyJMXFiles.sh

RUN apt-get update && \
    apt-get install -y curl \
        iptables \
        iproute2 \
        tar \
        jq \
        bridge-utils && \
    apt-get clean

# Install CNI plugins (bridge necessary, it is not part of wskc)
RUN curl -sSL -o cni-plugins-amd64-v${CNI_PLUGINS_VERSION}.tgz https://github.com/containernetworking/plugins/releases/download/v${CNI_PLUGINS_VERSION}/cni-plugins-amd64-v${CNI_PLUGINS_VERSION}.tgz && \
    echo "${CNI_PLUGINS_DOWNLOAD_SHA256}  cni-plugins-amd64-v${CNI_PLUGINS_VERSION}.tgz" | sha256sum -c - && \
    tar --directory /usr/local/bin -zxvf cni-plugins-amd64-v${CNI_PLUGINS_VERSION}.tgz && \
    rm -f cni-plugins-amd64-v${CNI_PLUGINS_VERSION}.tgz

# wskc.tar must be build and copied into the distributions folder before executing the docker build
ADD build/distributions/wskc.tar ./
ADD build/distributions/invoker.tar ./

COPY init.sh /
RUN chmod +x init.sh
RUN useradd -u ${UID} -ms /bin/bash ${NOT_ROOT_USER}

EXPOSE 8080
CMD ["./init.sh", "0"]
