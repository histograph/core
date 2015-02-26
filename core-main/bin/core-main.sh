#!/bin/bash

if [ ! -d `dirname $0`/../target ]; then
	echo "Histograph Core Main has not been built yet! Run 'mvn clean install' first."
	exit
fi

`dirname $0`/../target/histograph-core-main-*-standalone/bin/core-main.sh $@
