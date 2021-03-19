#!/bin/bash
pushd data
./mkdb.sh
./load-kafka.sh
popd
