#!/bin/bash
export STREAMS_application_id=kafka-mysql
export STREAMS_bootstrap_servers=localhost:9092
export STREAMS_num_stream_threads=1
export MAPPING_PATH=data/courses-mapping.yml
java -jar target/kafka-mysql-1.0-SNAPSHOT-shaded.jar
