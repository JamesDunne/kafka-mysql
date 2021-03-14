package org.bittwiddlers.kafka

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import java.io.ByteArrayOutputStream

class JacksonSerde<T> : Serde<T> {
  var objectMapper = ObjectMapper()

  private lateinit var typeReference : TypeReference<T>

  override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
    super.configure(configs, isKey)
    typeReference = configs!!["JsonTypeReference"] as TypeReference<T>
  }

  override fun serializer(): Serializer<T> = Serializer<T> { topic, data ->
    ByteArrayOutputStream().use {
      objectMapper.writeValue(it, data)
      it.toByteArray()
    }
  }

  override fun deserializer(): Deserializer<T> =
    Deserializer<T> { topic, data ->
      objectMapper.readValue<T>(data, typeReference)
    }

  companion object {
    fun <T> of(typeReference: TypeReference<T>): JacksonSerde<T> {
      val serde = JacksonSerde<T>()
      serde.configure(mutableMapOf("JsonTypeReference" to typeReference), false)
      return serde
    }
  }
}