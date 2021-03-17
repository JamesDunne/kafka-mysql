package org.bittwiddlers.kafka

import org.bittwiddlers.env.EnvVars
import org.apache.kafka.common.config.ConfigDef
import java.io.IOException

@Throws(IOException::class)
fun EnvVars.extractKafkaProperties(prefix: String, configDef: ConfigDef): Map<Any, Any> {
  val properties: MutableMap<Any, Any> = LinkedHashMap()

  // load any overrides onto consumer properties:
  val envProps: Map<Any, Any> = this.mapOnto(prefix, LinkedHashMap<Any, Any>())
  // translate '_' to '.' in the keys:
  val stringOverrides = envProps.mapKeys { (it.key as String).replace('_', '.') }

  // parse each string value to its target type:
  for (key in configDef.configKeys().values) {
    if (!stringOverrides.containsKey(key.name)) {
      continue
    }
    val value = ConfigDef.parseType(
      key.name,
      stringOverrides[key.name],
      key.type
    )
    properties[key.name] = value
  }

  return properties
}