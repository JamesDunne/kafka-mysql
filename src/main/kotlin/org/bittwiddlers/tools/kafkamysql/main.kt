package org.bittwiddlers.tools.kafkamysql

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Named
import org.bittwiddlers.env.EnvVars
import org.bittwiddlers.kafka.JacksonSerde
import org.bittwiddlers.kafka.extractKafkaProperties
import java.nio.file.FileSystems
import java.nio.file.Files
import java.util.Properties
import java.util.concurrent.CountDownLatch
import javax.script.ScriptEngineManager

typealias JsonObject = LinkedHashMap<String, Any?>
typealias JsonObjectGeneral = Map<String, Any?>

fun main(args: Array<String>) = Main().run(args)

class Main {
  val env = EnvVars(System.getenv())

  private val yaml: ObjectMapper = ObjectMapper(YAMLFactory())
  private val json: ObjectMapper = ObjectMapper()

  val keySerde = Serdes.StringSerde()
  val valueSerde = JacksonSerde.of(object : TypeReference<JsonObject>() {})

  @Suppress("UNCHECKED_CAST")
  fun run(args: Array<String>) {
    env.extractFilesFromEnv()
    env.extractJavaProperties()

    val mappingPathStr = env.stringOrFail("MAPPING_PATH")
    val mappingPath = FileSystems.getDefault().getPath(mappingPathStr).toAbsolutePath()

    // configure kafka streams:
    val props = Properties()
    props.putAll(
      env.extractKafkaProperties("STREAMS_", StreamsConfig.configDef())
    )
    props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.StringSerde::class.java.name

    // set number of stream threads if not set:
    val numThreads = Runtime.getRuntime().availableProcessors()
    if (!props.containsKey(StreamsConfig.NUM_STREAM_THREADS_CONFIG)) {
      props[StreamsConfig.NUM_STREAM_THREADS_CONFIG] = numThreads
    }

    // load mapping.yml:
    val mappingRoot = Files.newInputStream(mappingPath).use {
      yaml.readValue(it, MappingRoot::class.java)
    }

    val tableCount = mappingRoot.topics?.values?.sumOf { topic ->
      topic.mappings?.sumOf { event ->
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

    // instantiate Groovy script engine:
    val scriptEngineManager = ScriptEngineManager(this::class.java.classLoader)
    val scriptEngine = scriptEngineManager.getEngineByName("groovy")!!

    // configure the mapping:
    mappingRoot.configure(mappingPath.parent, scriptEngine)

    // build topology:
    val builder = StreamsBuilder()
    for (topic in mappingRoot.topics ?: emptyMap()) {
      val streamExact: KStream<String, LinkedHashMap<String, Any?>> = builder.stream(topic.key, Consumed.with(keySerde, valueSerde))
      // downcast value type from LinkedHashMap<> to generic Map<>:
      val stream = streamExact as KStream<String, JsonObjectGeneral>

      for ((i, tableMapping) in (topic.value.mappings ?: emptyList()).withIndex()) {
        tableMapping.configure(topic.key, i, scriptEngine)

        // run filter and mapValues functions over this stream first at the message level:
        val processedStream = tableMapping.preprocess?.fold(stream) { str, step ->
          step.filterPredicate?.let { str.filter(it) }
            ?: step.mapFunction?.let { str.mapValues(it) }
            ?: str
        } ?: stream

        // TODO: create a MySQL transaction at the message level here?
        // now process each table independently:
        for (table in tableMapping.tables?.values ?: emptyList()) {
          // create the table once up front:
          TablePopulator(ds, json, table).use {
            it.conn = ds.connection
            it.createTable()
          }

          // populate the table:
          processedStream.process(
            // each stream thread instantiates its own TablePopulator instance so they can run in parallel:
            { TablePopulator(ds, json, table) },
            Named.`as`(table.tableName)
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
