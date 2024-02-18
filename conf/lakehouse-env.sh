#!/usr/bin/env bash

# Set JAVA_HOME here to override the environment setting
# JAVA_HOME=

# default settings for starting auto-recovery

# Log4j configuration file
# LAKEHOUSE_LOG_CONF=

# Logs location
# LAKEHOUSE_LOG_DIR=

# Extra options to be passed to the jvm
LAKEHOUSE_MEM=${LAKEHOUSE_MEM:-"-Xmx512m -XX:MaxDirectMemorySize=512m"}

# Garbage collection options
LAKEHOUSE_GC=${LAKEHOUSE_GC:-"-XX:+UseG1GC -XX:+PerfDisableSharedMem -XX:+AlwaysPreTouch"}

if [ -z "$JAVA_HOME" ]; then
  JAVA_BIN=java
else
  JAVA_BIN="$JAVA_HOME/bin/java"
fi
for token in $("$JAVA_BIN" -version 2>&1 | grep 'version "'); do
    if [[ $token =~ \"([[:digit:]]+)\.([[:digit:]]+)(.*)\" ]]; then
        if [[ ${BASH_REMATCH[1]} == "1" ]]; then
          JAVA_MAJOR_VERSION=${BASH_REMATCH[2]}
        else
          JAVA_MAJOR_VERSION=${BASH_REMATCH[1]}
        fi
        break
    elif [[ $token =~ \"([[:digit:]]+)(.*)\" ]]; then
        # Process the java versions without dots, such as `17-internal`.
        JAVA_MAJOR_VERSION=${BASH_REMATCH[1]}
        break
    fi
done

LAKEHOUSE_GC_LOG_DIR=${LAKEHOUSE_GC_LOG_DIR:-"${LAKEHOUSE_LOG_DIR}"}

if [[ -z "$LAKEHOUSE_GC_LOG" ]]; then
  if [[ $JAVA_MAJOR_VERSION -gt 8 ]]; then
    LAKEHOUSE_GC_LOG="-Xlog:gc*,safepoint:${LAKEHOUSE_GC_LOG_DIR}/ar_gc_%p.log:time,uptime,tags:filecount=10,filesize=20M"
    if [[ $JAVA_MAJOR_VERSION -ge 17 ]]; then
      # Use async logging on Java 17+ https://bugs.openjdk.java.net/browse/JDK-8264323
      LAKEHOUSE_GC_LOG="-Xlog:async ${LAKEHOUSE_GC_LOG}"
    fi
  else
    # Java 8 gc log options
    LAKEHOUSE_GC_LOG="-Xloggc:${LAKEHOUSE_GC_LOG_DIR}/ar_gc_%p.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=20M"
  fi
fi

# Extra options to be passed to the jvm
LAKEHOUSE_EXTRA_OPTS="${LAKEHOUSE_EXTRA_OPTS:-" -Dio.netty.recycler.maxCapacityPerThread=4096"}"
