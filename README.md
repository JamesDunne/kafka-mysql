# Kafka to MySQL adapter

## What is it?

This tool is a long-running Kafka consumer process that extracts denormalized data from
JSON messages and translates it into a normalized form stored in a relational MySQL schema.

The data translation is entirely declarative, driven by a `mapping.yml` file that specifies
which Kafka topics to source data from, which tables they output to, and how to extract
any denormalized data from the JSON messages (if desired).

As an example, these input JSON messages in Kafka are translated into 2 output tables in MySQL:

```json
{"courseId": 11, "courseLevel": "Level_300", "taughtBy": [{"personId": 52}, {"personId": 57}, {"personId": 298}, {"personId": 324}, {"personId": 331}]}
{"courseId": 12, "courseLevel": "Level_500", "taughtBy": [{"personId": 79}, {"personId": 211}, {"personId": 407}]}
{"courseId": 13, "courseLevel": "Level_500", "taughtBy": [{"personId": 72}, {"personId": 342}]}
{"courseId": 14, "courseLevel": "Level_500", "taughtBy": [{"personId": 124}]}
{"courseId": 15, "courseLevel": "Level_500", "taughtBy": [{"personId": 292}]}
```

```mysql
course:                     taughtBy:
+----------+-------------+  +----------+----------+
| courseId | courseLevel |  | courseId | personId |
+----------+-------------+  +----------+----------+
|       11 | Level_300   |  |       11 |       52 |
|       12 | Level_500   |  |       11 |       57 |
|       13 | Level_500   |  |       11 |      298 |
|       14 | Level_500   |  |       11 |      324 |
|       15 | Level_500   |  |       11 |      331 |
+----------+-------------+  |       12 |       79 |
                            |       12 |      211 |
                            |       12 |      407 |
                            |       13 |       72 |
                            |       13 |      342 |
                            |       14 |      124 |
                            |       15 |      292 |
                            +----------+----------+
```

By this mapping section in the `mapping.yml` file:

```yaml
topics:
  'cr_course':
    events:
      - filter: "$"
        tables:
          'course':
            entities:
              root:
                from: $
                nondated:
                  courseId:
                    type: int
                    from: $.courseId
                    primaryKey: true
                  courseLevel:
                    type: varchar(12)
                    from: $.courseLevel
          'taughtBy':
            flatMapped:
              from: $.taughtBy
              filter: "$[?(@.personId != null)]"
              nondated:
                courseId:
                  type: int
                  fromRoot: $.courseId
                  primaryKey: true
                personId:
                  type: int
                  from: $.personId
                  primaryKey: true
                  entityId: true
```

## How it works

At the top level of `mapping.yml`, we have a list Kafka topics to source JSON messages from. From
there, it's possible that multiple kinds of messages will be interleaved in the topic, so we
specify `filter` json-path expressions which let us logically split up the messages into separate
tables if we want. If only one kind of message exists in the topic, then we can just use a
catch-all `$` json-path filter.

For each kind of message, we can generate one or more MySQL tables to fill data into. Each table
can specify either an `entities` translation or a `flatMapped` translation.

The `entities` translation allows one to extract data from one or more sub-objects from the JSON
data and flatten them into columns on the MySQL table. As long as the `from` json-path expressions
don't select more than one property value from the JSON message, any valid json-path is allowed
to select the data to map into the output MySQL column.

For arrays embedded in the JSON message, these can be translated to separate join tables using the
`flatMapped` translation. The `from` json-path on the `flatMapped` translation must be used to
select the array of data to extract column data from.

Within the MySQL column lists, the `from` json-path expressions select a single property value from
the current JSON object context. The `fromRoot` json-path expression selects a single property value
from the root of the JSON message, allowing us to combine properties from outside a `flatMapped` array
with properties inside one.

The `type`s are standard MySQL column type declarations and are only used when starting up the app
to automatically create the MySQL tables (if not exists).

`primaryKey` specifies whether the column is part of a composite primary key for the table.

Every table update is executed as a transaction since there often a batch of INSERTs and possibly
a DELETE statement. Every table record is logically upserted using MySQL's
`INSERT ... ON DUPLICATE KEY UPDATE` feature.

For any `flatMapped` data, a `DELETE WHERE primaryKeyColumnss=... AND entityId NOT IN (?,?,...)`
statement is issued to clear out any previous associations from prior JSON messages. Each JSON message
is treated as a logical snapshot and not as an ordered set of changes. This can and probably should
be made into an optional feature at this point. `entityId` is also part of this feature and is used
to identify the unique key for each array item.

## Environment variables

All environment variables prefixed with `STREAMS_` are treated as Kafka Streams configuration
properties. The `STREAMS_` prefix is removed from the variable name and all subsequent `'_'`
characters are replaced with `'.'` characters to fit the Kafka properties naming conventions.

For example, `STREAMS_application_id` is translated into `application.id`.

See the [Kafka Streams Documentation](https://kafka.apache.org/10/documentation/streams/developer-guide/config-streams.html#required-configuration-parameters)
for reference on which properties are required and what they do.

At a minimum you'll need to supply at least `STREAMS_application_id` and `STREAMS_bootstrap_servers`.

`STREAMS_num_stream_threads` can be used to specify the number of parallel threads the Kafka Streams
consumer will use. This will also be used to configure the MySQL connection pool size so that each
thread can have its own independent connection to MySQL.

The potential for deadlocks is very high when operating with more than a single Streams thread so
care must be taken to properly key and co-partition the Kafka topics. An alternative MySQL transaction
strategy can be investigated which moves the transaction boundary from the per-table level to the
per-message level.

# Run the demo
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
