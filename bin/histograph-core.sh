#!/bin/bash

if [ ! -d `dirname $0`/../core-main/target ] || [ ! -d `dirname $0`/../core-es/target ]; then
	echo "Histograph Core has not been built yet! Run 'mvn clean install' first."
	exit
fi

echo "Starting Histograph Elasticsearch controller..."
`dirname $0`/../core-es/bin/core-es.sh $@

echo "Starting Histograph Main module..."
`dirname $0`/../core-main/bin/core-main.sh $@
