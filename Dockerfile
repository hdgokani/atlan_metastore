#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#FROM scratch
#ADD alpine-minirootfs-3.20.3-x86_64.tar.gz /
#FROM alpine:latest
FROM alpine:3.13
#FROM ubuntu:22.04
LABEL maintainer="engineering@atlan.com"
ARG VERSION=3.0.0-SNAPSHOT

COPY distro/target/apache-atlas-3.0.0-SNAPSHOT-server.tar.gz  /apache-atlas-3.0.0-SNAPSHOT-server.tar.gz

RUN apk update \
    && apk upgrade --no-cache \
    && apk add --no-cache \
        wget \
        python2 \
        openjdk8 \
        patch \
        netcat-openbsd \
        curl \
    && cd / \
    && export MAVEN_OPTS="-Xms2g -Xmx2g" \
    && export JAVA_HOME="/usr/lib/jvm/java-1.8-openjdk" \
    && tar -xzvf /apache-atlas-3.0.0-SNAPSHOT-server.tar.gz -C /opt \
    && mv /opt/apache-atlas-${VERSION} /opt/apache-atlas \
    && rm -rf /apache-atlas-3.0.0-SNAPSHOT-server.tar.gz

# Copy the repair index jar file
RUN cd / \
    && wget https://atlan-build-artifacts.s3.ap-south-1.amazonaws.com/atlas/atlas-index-repair-tool-${VERSION}.tar.gz \
    && tar -xzvf /atlas-index-repair-tool-${VERSION}.tar.gz \
    && mkdir /opt/apache-atlas/libext \
    && mv /atlas-index-repair-tool-${VERSION}.jar /opt/apache-atlas/libext/ \
    && rm -rf /atlas-index-repair-tool-${VERSION}.tar.gz

#RUN ln -s /usr/bin/python2 /usr/bin/python
RUN /usr/bin/python --version

COPY atlas-hub/repair_index.py /opt/apache-atlas/bin/

RUN chmod +x /opt/apache-atlas/bin/repair_index.py

COPY atlas-hub/atlas_start.py.patch atlas-hub/atlas_config.py.patch /opt/apache-atlas/bin/
COPY atlas-hub/pre-conf/atlas-log4j.xml /opt/apache-atlas/conf/
COPY atlas-hub/pre-conf/atlas-log4j2.xml /opt/apache-atlas/conf/
COPY atlas-hub/pre-conf/atlas-auth/ /opt/apache-atlas/conf/

RUN curl https://repo1.maven.org/maven2/org/jolokia/jolokia-jvm/1.6.2/jolokia-jvm-1.6.2-agent.jar -o /opt/apache-atlas/libext/jolokia-jvm-agent.jar

RUN cd /opt/apache-atlas/bin \
    && ./atlas_start.py -setup || true

VOLUME ["/opt/apache-atlas/conf", "/opt/apache-atlas/logs"]
