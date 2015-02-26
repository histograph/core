#!/bin/bash

set -e
set -u

case `uname` in
  CYGWIN*)
    CP="`dirname $0`"/../config
    CP="$CP":$( echo `dirname $0`/../lib/*.jar . | sed 's/ /;/g')
    ;;
  *)
    CP="`dirname $0`"/../config
    CP="$CP":$( echo `dirname $0`/../lib/*.jar . | sed 's/ /:/g')
esac

export CLASSPATH="${CLASSPATH:-}:$CP"

# Find Java
if [ -z "${JAVA_HOME:-}" ]; then
    JAVA="java"
else
    JAVA="$JAVA_HOME/bin/java"
fi

MAIN_CLASS=org.waag.histograph.es.Main

# Start the JVM, execute the application, and return its exit code
exec $JAVA $MAIN_CLASS "$@"
