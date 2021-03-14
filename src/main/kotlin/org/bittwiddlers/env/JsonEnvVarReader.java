package org.bittwiddlers.env;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

public class JsonEnvVarReader {
  private final ObjectMapper objectMapper;
  private final EnvVars env;

  public JsonEnvVarReader(EnvVars env, ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
    this.env = env;
  }

  public <T> T read(String envName, TypeReference<T> typeReference) throws IOException {
    final String props = env.stringOrFail(envName);
    return objectMapper.readValue(props, typeReference);
  }

  public <T> T readOrDefault(String envName, String defaultValue, TypeReference<T> typeReference) throws IOException {
    final String props = env.stringOrDefault(envName, defaultValue);
    return objectMapper.readValue(props, typeReference);
  }
}
