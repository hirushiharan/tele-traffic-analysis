#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
/opt/hadoop/sbin/start-dfs.sh
tail -f /dev/null