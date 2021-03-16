package org.bittwiddlers.tools.kafkamysql

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.JsonPath
import com.jayway.jsonpath.Option
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Named
import org.bittwiddlers.env.EnvVars
import org.bittwiddlers.kafka.JacksonSerde
import org.bittwiddlers.kafka.extractKafkaProperties
import java.util.Properties
import java.util.concurrent.CountDownLatch

typealias JsonObject = LinkedHashMap<String, Any>

fun main(args: Array<String>) = Main().run(args)

class Main {
  val env = EnvVars(System.getenv())

  private val yaml: ObjectMapper = ObjectMapper(YAMLFactory())
  private val json: ObjectMapper = ObjectMapper()

  val keySerde = Serdes.StringSerde()
  val valueSerde = JacksonSerde.of(object : TypeReference<JsonObject>() {})

  fun run(args: Array<String>) {
    env.extractFilesFromEnv()
    env.extractJavaProperties()

    // configure kafka streams:
    val props = Properties()
    props.putAll(
      env.extractKafkaProperties("STREAMS_", StreamsConfig.configDef())
    )
    props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.StringSerde::class.java.name
    val numThreads = Runtime.getRuntime().availableProcessors()
    props[StreamsConfig.NUM_STREAM_THREADS_CONFIG] = numThreads

    // load mapping.yml:
    val mappingRoot = this::class.java.classLoader.getResourceAsStream("mapping.yml").use {
      yaml.readValue(it, MappingRoot::class.java)
    }

    val tableCount = mappingRoot.topics?.values?.sumOf { topic ->
      topic.events?.sumOf { event ->
        event.tables?.size ?: 0
      } ?: 0
    } ?: 1

    val dsProps = this::class.java.classLoader.getResourceAsStream("datasource.properties").use {
      val props = Properties()
      props.load(it)
      props
    }

    // estimate upper bound of connection pool size:
    if (!dsProps.containsKey("maximumPoolSize")) {
      dsProps["maximumPoolSize"] = (numThreads * 2 * tableCount) * 2
    }

    // create connection pool:
    val ds = HikariDataSource(HikariConfig(dsProps))

    // build topology:
    val builder = StreamsBuilder()

    val config = Configuration.defaultConfiguration().setOptions(Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST)

    for (topic in mappingRoot.topics ?: emptyMap()) {
      val stream = builder.stream(topic.key, Consumed.with(keySerde, valueSerde))
      for (event in topic.value.events ?: emptyList()) {
        val filterPath = JsonPath.compile(event.filter)
        val streamFiltered = stream.filter { _, v ->
          JsonPath.parse(v, config).read<List<Any?>>(filterPath)?.size == 1
        }

        for (table in event.tables ?: emptyMap()) {
          // create the table once up front:
          TablePopulator(ds, json, table).use {
            it.conn = ds.connection
            it.createTable()
          }

          // populate the table:
          streamFiltered.process(
            { TablePopulator(ds, json, table) },
            Named.`as`("${table.key}")
          )
        }
      }
    }

    // start up streams:
    val streams = KafkaStreams(builder.build(props), props)

    val latch = CountDownLatch(1)
    Runtime.getRuntime().addShutdownHook(object : Thread("shutdown-hook") {
      override fun run() {
        streams.close()
        latch.countDown()
      }
    })

    try {
      streams.start()
      latch.await()
    } catch (e: Throwable) {
      e.printStackTrace()
    }
  }
}
