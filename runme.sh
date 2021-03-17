#!/bin/bash
pushd data
./mkdb.sh
./load-kafka.sh
popd

set -e

mvn clean package

export STREAMS_application_id=kafka-mysql
export STREAMS_bootstrap_servers=localhost:9092
export STREAMS_num_stream_threads=1
java -jar target/kafka-mysql-1.0-SNAPSHOT-shaded.jar
