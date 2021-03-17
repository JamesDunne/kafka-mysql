# Kafka to MySQL adapter

## Run the demo
Set up a local kafka cluster and MySQL server:
```shell
brew install mysql kafka
mysql.server start
# run each in a separate terminal window/tab:
zookeeper-server-start /usr/local/etc/zookeeper/zoo.cfg
kafka-server-start /usr/local/etc/kafka/server.properties
```

### Automatic Setup

Run the `runme.sh` script:
```shell
./runme.sh
```

This script will initialize the MySQL database and load sample data into Kafka topics.
If the SQL database and Kafka topics already exist, they will be dropped/deleted and
be recreated from scratch.

### Manual Setup

Prefer to use the `runme.sh` script instead, but if you want to manually replicate it,
follow these steps.

Create and import the sample dataset into kafka topics as expected by
`src/main/resources/mapping.yml`
```shell
cd data
./load-kafka.sh
```

Make the sample MySQL database expected by `src/main/resources/datasource.properties`
```shell
./mkdb.sh
```

Build the main application
```shell
cd ..
mvn clean package
```

Run the application with environment variables set:
```shell
export STREAMS_application_id=kafka-mysql
export STREAMS_bootstrap_servers=localhost:9092
export STREAMS_num_stream_threads=1
java -jar target/kafka-mysql-1.0-SNAPSHOT-shaded.jar
```

## Expected output

The tool should not take very long at all to complete, so press CTRL-C once
the CPU goes quiet. The process is intended to be a long-running Kafka consumer
process and so it's not normally necessary to interrupt it like this, but for a
demo it suffices.

The tool should follow the relational mapping strategy described by `src/main/resources/mapping.yml`
to extract data from JSON messages pulled from the Kafka topics `cr_course` and `cr_person`.

Data is denormalized in these JSON messages. This tool will normalize the data out
to the original 4 MySQL tables from which it was built: `advisedBy`, `course`, `person`, `taughtBy`.

To verify the output, query each table and check their row counts:

 * `advisedBy`: `113` rows
 * `course`: `132` rows
 * `person`: `278` rows
 * `taughtBy`: `189` rows

You can inspect the JSON messages in `data/person.json` and `data/course.json` to
see source data that was mapped out of Kafka and into the MySQL relational schema.
