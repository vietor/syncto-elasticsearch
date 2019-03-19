#!/bin/bash

if [ "x$JVM_MIN_MEM" = "x" ]; then
    JVM_MIN_MEM=256m
fi
if [ "x$JVM_MAX_MEM" = "x" ]; then
    JVM_MAX_MEM=1g
fi
if [ "x$JVM_HEAP_SIZE" != "x" ]; then
    JVM_MIN_MEM=$JVM_HEAP_SIZE
    JVM_MAX_MEM=$JVM_HEAP_SIZE
fi

JAVA_OPTS="$JAVA_OPTS -Xms${JVM_MIN_MEM}"
JAVA_OPTS="$JAVA_OPTS -Xmx${JVM_MAX_MEM}"

if [ "x$JVM_HEAP_NEWSIZE" != "x" ]; then
    JAVA_OPTS="$JAVA_OPTS -Xmn${JVM_HEAP_NEWSIZE}"
fi

if [ "x$JVM_DIRECT_SIZE" != "x" ]; then
    JAVA_OPTS="$JAVA_OPTS -XX:MaxDirectMemorySize=${JVM_DIRECT_SIZE}"
fi

if [ "x$JVM_GC_OPTS" = "x" ]; then
    JVM_GC_OPTS="$JVM_GC_OPTS -XX:+UseParNewGC"
    JVM_GC_OPTS="$JVM_GC_OPTS -XX:+UseConcMarkSweepGC"
    JVM_GC_OPTS="$JVM_GC_OPTS -XX:CMSInitiatingOccupancyFraction=75"
    JVM_GC_OPTS="$JVM_GC_OPTS -XX:+UseCMSInitiatingOccupancyOnly"
fi

JAVA_OPTS="$JAVA_OPTS $JVM_GC_OPTS"

JAVA_OPTS="$JAVA_OPTS -XX:+DisableExplicitGC"
JAVA_OPTS="$JAVA_OPTS -Dfile.encoding=UTF-8"
JAVA_OPTS="$JAVA_OPTS -Djna.nosys=true"

JAVA_COMMAND="java"
if [ "x$JAVA_HOME" != "x" ]; then
    JAVA_COMMAND="$JAVA_HOME/bin/java"
fi

WORK_HOME=$(cd `dirname $0` && pwd -P)
WORK_CLASSPATH="$WORK_HOME/lib/*"

cd $WORK_HOME
if ! echo $* | grep -E '(^-d |-d$| -d |--daemonize$|--daemonize )' > /dev/null; then
    exec $JAVA_COMMAND $JAVA_OPTS -cp "$WORK_CLASSPATH" MongodbSync
else
    exec $JAVA_COMMAND $JAVA_OPTS -cp "$WORK_CLASSPATH" MongodbSync 1>/dev/null 2>&1 &
fi
