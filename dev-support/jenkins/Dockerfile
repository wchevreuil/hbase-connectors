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

# Dockerfile for hbase-connectors pre-commit build.
# https://builds.apache.org/job/PreCommit-HBASE-CONNECTORS-Build

FROM maven:3.8-jdk-8

# hadolint ignore=DL3008
RUN apt-get -q update && apt-get -q install --no-install-recommends -y \
       git \
       rsync \
       shellcheck \
       wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# JDK 17 for the Spark4 profile (maven-enforcer-plugin requires JDK 17+)
RUN wget -q https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.11%2B9/OpenJDK17U-jdk_x64_linux_hotspot_17.0.11_9.tar.gz -O /tmp/jdk17.tar.gz && \
    mkdir -p /usr/local/openjdk-17 && \
    tar -xzf /tmp/jdk17.tar.gz --strip-components=1 -C /usr/local/openjdk-17 && \
    rm /tmp/jdk17.tar.gz

# Yetus xml check uses jrunscript (removed in JDK 14). Provide a wrapper so
# it remains available when JAVA_HOME points to JDK 17.
RUN printf '#!/bin/sh\nexec /usr/local/openjdk-8/bin/jrunscript "$@"\n' \
      > /usr/local/openjdk-17/bin/jrunscript && \
    chmod +x /usr/local/openjdk-17/bin/jrunscript

###
# Avoid out of memory errors in builds
###
ENV MAVEN_OPTS=-Xmx3g

CMD ["/bin/bash"]

###
# Everything past this point is either not needed for testing or breaks Yetus.
# So tell Yetus not to read the rest of the file:
# YETUS CUT HERE
###
