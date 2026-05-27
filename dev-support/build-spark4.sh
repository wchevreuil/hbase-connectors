#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Runs a full Maven install with the Spark 4 reactor (-Pspark4).
# Requires JDK 17+ (JAVA_HOME recommended). Omit this script and profile for the default Spark 3 build.
#
# Maven: defaults to Homebrew Maven 3.9.x at /opt/homebrew/opt/maven/bin/mvn when present
# (matches Spark 4.0 build docs). Override with MVN=/path/to/mvn.
#

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

HOMEBREW_MVN="/opt/homebrew/opt/maven/bin/mvn"
if [[ -n "${MVN:-}" ]]; then
  :
elif [[ -x "${HOMEBREW_MVN}" ]]; then
  MVN="${HOMEBREW_MVN}"
else
  MVN="mvn"
fi

if [[ ! -x "${MVN}" ]] && ! command -v "${MVN}" >/dev/null 2>&1; then
  echo "[ERROR] Maven not found. Install Maven 3.9+ (e.g. brew install maven) or set MVN to your mvn binary." >&2
  exit 1
fi

JAVA_BIN="${JAVA_HOME:+$JAVA_HOME/bin/}java"
line="$("${JAVA_BIN}" -version 2>&1 | head -n 1)"

major=""
if [[ "${line}" =~ version\ \'1\. ]]; then
  major=8
elif [[ "${line}" =~ version\ \"1\. ]]; then
  major=8
elif [[ "${line}" =~ version\ \'([0-9]+)\. ]]; then
  major="${BASH_REMATCH[1]}"
elif [[ "${line}" =~ version\ \"([0-9]+)\. ]]; then
  major="${BASH_REMATCH[1]}"
fi

if [[ -z "${major}" ]] || [[ "${major}" -lt 17 ]]; then
  echo "[ERROR] JDK 17+ required for Spark 4 build (-Pspark4). javac reports: ${line}" >&2
  exit 1
fi

if [[ "$#" -eq 0 ]]; then
  set -- -B
fi

echo "[INFO] Using Maven: ${MVN} ($("${MVN}" --version 2>&1 | head -n 1))" >&2

exec "${MVN}" "$@" -Pspark4 -DskipTests clean install
