#!/usr/bin/env sh

set -x
set -e

mvn -t toolchains-pipeline.xml test

