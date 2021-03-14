package org.bittwiddlers.env;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EnvVars {
  private static final Logger LOG = LogManager.getLogger(EnvVars.class);

  public static final String FILE_PATH_PREFIX = "_FILE_PATH_";
  public static final String FILE_B64_PREFIX = "_FILE_B64_";
  public static final String FILE_RAW_PREFIX = "_FILE_RAW_";

  private final Map<String, String> env;

  public EnvVars(Map<String, String> env) {
    this.env = env;
  }

  public EnvVars() {
    this(System.getenv());
  }

  public boolean exists(String name) {
    final boolean exists = env.containsKey(name);
    if (exists) {
      LOG.info("env {} exists", name);
    } else {
      LOG.info("env {} does not exist", name);
    }
    return exists;
  }

  public String stringOrFail(String name) {
    final String value = env.get(name);
    if (value == null) {
      throw new RuntimeException(String.format("Expected value for env var '%s'", name));
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("env {} read as {}", name, value);
    } else {
      LOG.info("env {} read", name);
    }
    return value;
  }

  public String stringOrDefault(String name, String defaultValue) {
    final String value = env.get(name);
    if (value == null) {
      LOG.info("env {} defaults to {}", name, defaultValue);
      return defaultValue;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("env {} read as {}", name, value);
    } else {
      LOG.info("env {} read", name);
    }
    return value;
  }

  public int intOrDefault(String name, int defaultValue) {
    final String value = env.get(name);
    if (value == null) {
      LOG.info("env {} defaults as int to {}", name, defaultValue);
      return defaultValue;
    }
    try {
      final int intValue = Integer.parseInt(value);
      if (LOG.isDebugEnabled()) {
        LOG.debug("env {} parsed to int as {}", name, intValue);
      } else {
        LOG.info("env {} parsed to int", name);
      }
      return intValue;
    } catch (NumberFormatException ex) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("env {} could not be parsed from {} to int, defaulting to {}", name, value, defaultValue);
      } else {
        LOG.info("env {} could not be parsed to int, defaulting to {}", name, defaultValue);
      }
      return defaultValue;
    }
  }

  public boolean boolOrDefault(String name, boolean defaultValue) {
    final String value = env.get(name);
    if (value == null) {
      LOG.info("env {} defaults as bool to {}", name, defaultValue);
      return defaultValue;
    }

    final boolean boolValue = Boolean.parseBoolean(value);
    if (LOG.isDebugEnabled()) {
      LOG.debug("env {} parsed to bool as {}", name, boolValue);
    } else {
      LOG.info("env {} parsed to bool", name);
    }
    return boolValue;
  }

  public Map<Object, Object> mapOnto(String prefix, Map<Object, Object> map) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("reading env {}* into map", prefix);
    } else {
      LOG.info("reading env {}* into map", prefix);
    }
    for (Map.Entry<String, String> e : env.entrySet()) {
      final String key = e.getKey();
      final String value = e.getValue();

      if (key.startsWith(prefix)) {
        String propKey = key.substring(prefix.length());
        if (LOG.isDebugEnabled()) {
          LOG.debug("  {}={}", propKey, value);
        } else {
          LOG.info("  {}", propKey);
        }
        map.put(propKey, value);
      }
    }

    return map;
  }

  public void extractJavaProperties() {
    final Map<Object, Object> javaProperties = this.mapOnto("_JAVA_PROPERTY_", new LinkedHashMap<>());
    for (Map.Entry<Object, Object> e : javaProperties.entrySet()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Setting java property {}={}", e.getKey(), e.getValue());
      } else {
        LOG.info("Setting java property {}", e.getKey());
      }
      System.setProperty((String) e.getKey(), (String) e.getValue());
    }
  }


  public boolean extractFilesFromEnv() {
    // A map to keep track of unique file identifiers with String[0] being the path and String[1] being the base64-encoded data:
    HashMap<String, EnvVarFile> filesToWrite = new HashMap<>();

    // Find pairs of _FILE_PATH_XYZ and _FILE_DATA_XYZ to determine files to create:
    for (Map.Entry<String, String> e : env.entrySet()) {
      String key = e.getKey();
      String value = e.getValue();
      if (key.startsWith(FILE_PATH_PREFIX)) {
        // supply the file's target path in the env var:
        String fileKey = key.substring(FILE_PATH_PREFIX.length());
        filesToWrite.putIfAbsent(fileKey, new EnvVarFile(value, null, new String[]{key, null}));
        filesToWrite.get(fileKey).path = value;
        filesToWrite.get(fileKey).envVarPair[0] = key;
      } else if (key.startsWith(FILE_B64_PREFIX)) {
        // allow base64-encoded file contents to be written from the env var:
        String fileKey = key.substring(FILE_B64_PREFIX.length());

        final byte[] data;
        try {
          data = Base64.getDecoder().decode(value);
        } catch (IllegalArgumentException ex) {
          LOG.error("Could not decode base64 contents of env {}", FILE_B64_PREFIX + fileKey, ex);
          continue;
        }

        filesToWrite.putIfAbsent(fileKey, new EnvVarFile(null, data, new String[]{null, key}));
        filesToWrite.get(fileKey).data = data;
        filesToWrite.get(fileKey).envVarPair[1] = key;
      } else if (key.startsWith(FILE_RAW_PREFIX)) {
        // allow raw file contents to be written from the env var:
        String fileKey = key.substring(FILE_RAW_PREFIX.length());
        final byte[] data = value.getBytes(StandardCharsets.UTF_8);
        filesToWrite.putIfAbsent(fileKey, new EnvVarFile(null, data, new String[]{null, key}));
        filesToWrite.get(fileKey).data = data;
        filesToWrite.get(fileKey).envVarPair[1] = key;
      }
    }

    // Validate each env var pair and write the files:
    for (Map.Entry<String, EnvVarFile> entry : filesToWrite.entrySet()) {
      String k = entry.getKey();
      EnvVarFile envVarFile = entry.getValue();
      final String path = envVarFile.path;
      if (path == null) {
        String missingEnvVar = FILE_PATH_PREFIX + k;
        LOG.error("Missing env var '{}'", missingEnvVar);
        return false;
      }

      final byte[] data = envVarFile.data;
      if (data == null) {
        String missingEnvVar = FILE_B64_PREFIX + k;
        LOG.error("Missing env var '{}'", missingEnvVar);
        return false;
      }

      //if (LOG.isDebugEnabled()) {
      //  LOG.debug("Contents of {}:\n{}", path, new String(envVarFile.data));
      //}

      // Make containing directories:
      {
        File file = new File(path);
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
          LOG.error("Could not make directories for {}", file.getParentFile().getAbsolutePath());
          return false;
        }
      }

      // write out the file contents:
      try (final FileOutputStream file = new FileOutputStream(path)) {
        file.write(data);
        LOG.info("Wrote contents of env {} out to {}", envVarFile.envVarPair[1], path);
      } catch (IOException ex) {
        LOG.error("Failed to write contents of env {} out to {}", envVarFile.envVarPair[1], path, ex);
        return false;
      }
    }

    return true;
  }

  private static class EnvVarFile {
    public String path;
    public byte[] data;
    public String[] envVarPair;

    public EnvVarFile(String path, byte[] data, String[] envVarPair) {
      this.path = path;
      this.data = data;
      this.envVarPair = envVarPair;
    }
  }
}
